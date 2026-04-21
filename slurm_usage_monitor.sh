#!/usr/bin/env bash
#
# slurm_usage_monitor.sh
# Collects hourly SLURM resource usage per user across SLURM partitions.
# Run via cron on the login node.
#
# Output files (daily rotation):
#   ~/slurm_monitor/data/live_usage_YYYY-MM-DD.csv      - per-user squeue snapshot
#   ~/slurm_monitor/data/completed_jobs_YYYY-MM-DD.csv   - finished jobs from sacct
#   ~/slurm_monitor/monitor.log                          - execution log
#
# Retention: 90 days (auto-cleanup of old CSV files)

set -euo pipefail

# --- Configuration ---
MONITOR_DIR="$HOME/slurm_monitor"
DATA_DIR="${MONITOR_DIR}/data"
LOG_FILE="${MONITOR_DIR}/monitor.log"
# Set to "all" to auto-discover every partition, or a comma-separated list
# (e.g. "aics_gpu,aics") to monitor specific partitions only.
PARTITIONS="all"
RETENTION_DAYS=90

TIMESTAMP=$(date +"%Y-%m-%dT%H:%M:%S")
DATE_TAG=$(date +"%Y-%m-%d")

LIVE_CSV="${DATA_DIR}/live_usage_${DATE_TAG}.csv"
COMPLETED_CSV="${DATA_DIR}/completed_jobs_${DATE_TAG}.csv"

# --- Setup ---
mkdir -p "$DATA_DIR"

# --- Resolve partitions ---
if [[ "$PARTITIONS" == "all" ]]; then
    PARTITIONS=$(sinfo -h -o "%P" | tr -d '*' | paste -sd, -)
    if [[ -z "$PARTITIONS" ]]; then
        echo "[${TIMESTAMP}] ERROR: sinfo returned no partitions" >> "$LOG_FILE"
        exit 1
    fi
fi

SQUEUE_PARTITION_FLAG="-p $PARTITIONS"
SACCT_PARTITION_FLAG="--partition=$PARTITIONS"

log() {
    echo "[${TIMESTAMP}] $1" >> "$LOG_FILE"
}

# --- Section 1: Live usage snapshot (squeue) ---
# Aggregates per (user, partition): running/pending jobs, CPUs, memory, GPUs

if [[ ! -f "$LIVE_CSV" ]]; then
    echo "timestamp,partition,user,running_jobs,pending_jobs,alloc_cpus,alloc_mem_gb,alloc_gpus" \
        > "$LIVE_CSV"
fi

squeue_output=$(squeue --noheader $SQUEUE_PARTITION_FLAG \
    --format="%u|%T|%P|%C|%m|%b" 2>/dev/null) || true

live_users=0
if [[ -n "$squeue_output" ]]; then
    new_live=$(echo "$squeue_output" | awk -F'|' '
    {
        gsub(/^[ \t]+|[ \t]+$/, "", $1)
        gsub(/^[ \t]+|[ \t]+$/, "", $2)
        gsub(/^[ \t]+|[ \t]+$/, "", $3)
        gsub(/^[ \t]+|[ \t]+$/, "", $4)
        gsub(/^[ \t]+|[ \t]+$/, "", $5)
        gsub(/^[ \t]+|[ \t]+$/, "", $6)

        user = $1; state = $2; partition = $3
        cpus = $4 + 0; mem_raw = $5; gres_raw = $6

        if (user == "") next

        key = user SUBSEP partition

        # Memory to GB (handles M, G, T suffixes)
        mem_gb = 0
        if (mem_raw ~ /[0-9]+M$/)
            mem_gb = substr(mem_raw, 1, length(mem_raw)-1) / 1024.0
        else if (mem_raw ~ /[0-9]+G$/)
            mem_gb = substr(mem_raw, 1, length(mem_raw)-1) + 0
        else if (mem_raw ~ /[0-9]+T$/)
            mem_gb = (substr(mem_raw, 1, length(mem_raw)-1) + 0) * 1024

        # GPUs from GRES (formats: N/A, gres/gpu:1, gres/gpu:v100:1, gres/gpu:a100sxm:4)
        gpus = 0
        if (gres_raw ~ /gpu/) {
            n = split(gres_raw, parts, ":")
            gpus = parts[n] + 0
        }

        if (state == "RUNNING") {
            running[key]++
            alloc_cpus[key] += cpus
            alloc_mem[key] += mem_gb
            alloc_gpus[key] += gpus
        } else if (state == "PENDING") {
            pending[key]++
        }
        seen[key] = 1
    }
    END {
        for (key in seen) {
            split(key, k, SUBSEP)
            printf "%s,%s,%s,%d,%d,%d,%.1f,%d\n",
                ts, k[2], k[1],
                running[key]+0, pending[key]+0,
                alloc_cpus[key]+0, alloc_mem[key]+0,
                alloc_gpus[key]+0
        }
    }
    ' ts="$TIMESTAMP")

    if [[ -n "$new_live" ]]; then
        echo "$new_live" >> "$LIVE_CSV"
        live_users=$(echo "$new_live" | wc -l)
    fi
fi

# --- Section 2: Completed jobs in the last hour (sacct) ---
# One row per finished job, with max RSS pulled from sub-steps.

if [[ ! -f "$COMPLETED_CSV" ]]; then
    echo "timestamp,partition,user,job_id,state,alloc_cpus,alloc_gpus,max_rss_mb,elapsed" \
        > "$COMPLETED_CSV"
fi

sacct_output=$(sacct -a -S now-1hour \
    $SACCT_PARTITION_FLAG \
    --state=CD,F,TO,CA,OOM \
    --parsable2 --noheader \
    --format=User,JobID,Partition,State,AllocCPUS,AllocTRES,MaxRSS,Elapsed 2>/dev/null) || true

completed_jobs=0
if [[ -n "$sacct_output" ]]; then
    new_completed=$(echo "$sacct_output" | awk -F'|' '
    {
        user = $1; jobid = $2; partition = $3; state = $4
        cpus = $5 + 0; tres = $6; maxrss_raw = $7; elapsed = $8

        # Parse MaxRSS to MB (appears on step rows, not parent)
        rss_mb = 0
        if (maxrss_raw ~ /[0-9]+K$/)
            rss_mb = substr(maxrss_raw, 1, length(maxrss_raw)-1) / 1024.0
        else if (maxrss_raw ~ /[0-9]+M$/)
            rss_mb = substr(maxrss_raw, 1, length(maxrss_raw)-1) + 0
        else if (maxrss_raw ~ /[0-9]+G$/)
            rss_mb = (substr(maxrss_raw, 1, length(maxrss_raw)-1) + 0) * 1024

        if (jobid ~ /\./) {
            # Sub-step: track max RSS per parent job
            split(jobid, jparts, ".")
            pid = jparts[1]
            if (rss_mb > step_rss[pid]) step_rss[pid] = rss_mb
        } else {
            # Parent job: store metadata
            p_user[jobid]      = user
            p_part[jobid]      = partition
            p_state[jobid]     = state
            p_cpus[jobid]      = cpus
            p_elapsed[jobid]   = elapsed
            p_order[++p_count] = jobid

            # Parse GPUs from AllocTRES
            # e.g. billing=32,cpu=32,gres/gpu:a100sxm=4,mem=220G
            gpus = 0
            n = split(tres, fields, ",")
            for (i = 1; i <= n; i++) {
                if (fields[i] ~ /gres\/gpu/) {
                    split(fields[i], eq, "=")
                    gpus = eq[2] + 0
                }
            }
            p_gpus[jobid] = gpus
        }
    }
    END {
        for (i = 1; i <= p_count; i++) {
            jid = p_order[i]
            st = p_state[jid]
            sub(/ by.*/, "", st)  # "CANCELLED by 12345" -> "CANCELLED"

            printf "%s,%s,%s,%s,%s,%d,%d,%.0f,%s\n",
                ts, p_part[jid], p_user[jid], jid, st,
                p_cpus[jid], p_gpus[jid],
                step_rss[jid]+0, p_elapsed[jid]
        }
    }
    ' ts="$TIMESTAMP")

    if [[ -n "$new_completed" ]]; then
        echo "$new_completed" >> "$COMPLETED_CSV"
        completed_jobs=$(echo "$new_completed" | wc -l)
    fi
fi

# --- Section 3: Cleanup old data (90-day retention) ---
find "$DATA_DIR" -name "*.csv" -mtime +${RETENTION_DAYS} -delete 2>/dev/null || true

# --- Section 4: Log summary ---
log "partitions=${PARTITIONS} live_users=${live_users} completed_jobs=${completed_jobs}"
