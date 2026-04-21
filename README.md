# slurm-monitor

Lightweight SLURM resource usage monitoring for the `aics` and `aics_gpu` partitions. Collects hourly snapshots of per-user resource allocations and completed job statistics via cron.

## What It Does

- **`slurm_usage_monitor.sh`** — Bash script that runs hourly via cron on a login node. Collects:
  - **Live usage** (`squeue`): per-user running/pending jobs, allocated CPUs, memory (GB), and GPUs, aggregated by partition.
  - **Completed jobs** (`sacct`): jobs that finished in the last hour with state, resource allocations, peak RSS, and elapsed time.
  - Auto-cleans CSV files older than 90 days.

- **`generate_report.py`** — Python script that reads collected data and prints a formatted summary report covering:
  - Weekly per-user resource-hours (CPU, GPU, memory)
  - Average and peak allocations per user
  - Partition breakdown (aics vs aics_gpu)
  - Daily breakdown and active hours per user
  - Cluster-wide daily trends
  - Completed job statistics (counts, failure rates)

## Requirements

- SLURM client tools (`squeue`, `sacct`) accessible from the login node
- Python 3.10+ with `pandas` (for the report generator)
- User must be able to view all users' jobs (`sacct -a` / `squeue` visibility)

## Setup

1. Clone to your home directory on the SLURM login node:

   ```bash
   git clone https://github.com/AllenCell/slurm-monitor.git ~/slurm_monitor
   chmod +x ~/slurm_monitor/slurm_usage_monitor.sh
   ```

2. Install the cron job (runs at 5 minutes past every hour):

   ```bash
   crontab -e
   # Add this line:
   5 * * * * /home/$USER/slurm_monitor/slurm_usage_monitor.sh >> /home/$USER/slurm_monitor/cron.log 2>&1
   ```

3. (Optional) Adjust configuration at the top of `slurm_usage_monitor.sh`:
   - `PARTITIONS` — comma-separated list of SLURM partitions to monitor
   - `RETENTION_DAYS` — how long to keep CSV files (default: 90)

## Usage

Run the monitor manually to verify it works:

```bash
./slurm_usage_monitor.sh
ls data/  # should contain live_usage_*.csv and completed_jobs_*.csv
```

Generate a summary report:

```bash
python3 generate_report.py            # all available data
python3 generate_report.py --days 7   # last 7 days
python3 generate_report.py -o report.txt  # save to file
```

## Stopping or Pausing

**Pause** monitoring (keeps cron entry but disables it):

```bash
crontab -l | sed 's/^\([^#].*slurm_usage_monitor\)/#\1/' | crontab -
```

**Resume** a paused monitor:

```bash
crontab -l | sed 's/^#\(.*slurm_usage_monitor\)/\1/' | crontab -
```

**Stop** monitoring entirely (removes the cron entry):

```bash
crontab -l | grep -v slurm_usage_monitor | crontab -
```

**Verify** current state:

```bash
crontab -l  # look for the slurm_usage_monitor line (# prefix = paused)
```

> Note: Pausing or stopping the cron job does not delete any collected data. CSV files in `data/` are retained until they exceed the configured retention period (default: 90 days).

## Output Files

| File | Location | Content |
|------|----------|---------|
| `data/live_usage_YYYY-MM-DD.csv` | One per day | Hourly snapshots of per-user resource allocations |
| `data/completed_jobs_YYYY-MM-DD.csv` | One per day | Jobs that completed in each hourly window |
| `monitor.log` | Single file | Execution log with row counts per run |

### CSV Schemas

**live_usage**:
`timestamp, partition, user, running_jobs, pending_jobs, alloc_cpus, alloc_mem_gb, alloc_gpus`

**completed_jobs**:
`timestamp, partition, user, job_id, state, alloc_cpus, alloc_gpus, max_rss_mb, elapsed`

## Tested On

- SLURM 24.05.4
- GNU awk (gawk)
- Python 3.x + pandas 2.x
