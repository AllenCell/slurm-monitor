#!/usr/bin/env python3
"""
SLURM Usage Report Generator

Reads hourly live_usage and completed_jobs CSVs from ~/slurm_monitor/data/
and produces a formatted text report covering:
  1. Weekly summary per user (resource averages, peaks, total resource-hours)
  2. Daily breakdown per user
  3. Partition breakdown (aics vs aics_gpu)
  4. Active hours per user
  5. Time series trends (daily cluster totals)
  6. Completed job stats (if data exists)

Usage:
    python3 generate_report.py                  # report for all available data
    python3 generate_report.py --days 7         # last 7 days only
    python3 generate_report.py --output report.txt  # save to file
"""

import argparse
import glob
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

DATA_DIR = Path.home() / "slurm_monitor" / "data"


def load_live_data(days: int | None = None) -> pd.DataFrame:
    files = sorted(glob.glob(str(DATA_DIR / "live_usage_*.csv")))
    if not files:
        print("ERROR: No live_usage CSV files found in", DATA_DIR)
        sys.exit(1)

    dfs = []
    for f in files:
        df = pd.read_csv(f)
        if not df.empty:
            dfs.append(df)

    if not dfs:
        print("ERROR: All live_usage files are empty.")
        sys.exit(1)

    data = pd.concat(dfs, ignore_index=True)
    data["timestamp"] = pd.to_datetime(data["timestamp"])
    data["date"] = data["timestamp"].dt.date

    if days:
        cutoff = datetime.now() - timedelta(days=days)
        data = data[data["timestamp"] >= cutoff]

    return data


def load_completed_data(days: int | None = None) -> pd.DataFrame:
    files = sorted(glob.glob(str(DATA_DIR / "completed_jobs_*.csv")))
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        df = pd.read_csv(f)
        if not df.empty:
            dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    data = pd.concat(dfs, ignore_index=True)
    data["timestamp"] = pd.to_datetime(data["timestamp"])
    if days:
        cutoff = datetime.now() - timedelta(days=days)
        data = data[data["timestamp"] >= cutoff]
    return data


def fmt_float(v, decimals=1):
    return f"{v:,.{decimals}f}"


def print_header(title: str, width: int = 90):
    print()
    print("=" * width)
    print(f" {title}")
    print("=" * width)


def print_subheader(title: str, width: int = 90):
    print()
    print(f"--- {title} ---")


def section_overview(df: pd.DataFrame):
    print_header("OVERVIEW")
    date_range = f"{df['date'].min()} to {df['date'].max()}"
    n_snapshots = df["timestamp"].nunique()
    n_users = df["user"].nunique()
    n_days = df["date"].nunique()

    print(f"  Date range:       {date_range}")
    print(f"  Days covered:     {n_days}")
    print(f"  Hourly snapshots: {n_snapshots}")
    print(f"  Unique users:     {n_users}")
    print(f"  Partitions:       {', '.join(sorted(df['partition'].unique()))}")


def section_weekly_summary(df: pd.DataFrame):
    """Per-user weekly aggregate: avg and peak resources, total resource-hours."""
    print_header("WEEKLY SUMMARY PER USER")

    # Each row = one (user, partition, timestamp) observation.
    # A user present in a snapshot hour is "using" those resources for 1 hour.
    # Resource-hours = sum across all snapshots of allocated resources.

    user_stats = []
    for user, udf in df.groupby("user"):
        snapshots = udf["timestamp"].nunique()
        total_cpu_h = udf["alloc_cpus"].sum()  # each snapshot = 1 hour
        total_gpu_h = udf["alloc_gpus"].sum()
        total_mem_gh = udf["alloc_mem_gb"].sum()  # GB-hours

        # Per-snapshot aggregates (sum across partitions per snapshot)
        per_snap = udf.groupby("timestamp").agg(
            cpus=("alloc_cpus", "sum"),
            gpus=("alloc_gpus", "sum"),
            mem=("alloc_mem_gb", "sum"),
            running=("running_jobs", "sum"),
            pending=("pending_jobs", "sum"),
        )

        user_stats.append(
            {
                "user": user,
                "active_hours": snapshots,
                "avg_cpus": per_snap["cpus"].mean(),
                "peak_cpus": per_snap["cpus"].max(),
                "avg_gpus": per_snap["gpus"].mean(),
                "peak_gpus": per_snap["gpus"].max(),
                "avg_mem_gb": per_snap["mem"].mean(),
                "peak_mem_gb": per_snap["mem"].max(),
                "avg_running": per_snap["running"].mean(),
                "peak_running": per_snap["running"].max(),
                "avg_pending": per_snap["pending"].mean(),
                "cpu_hours": total_cpu_h,
                "gpu_hours": total_gpu_h,
                "mem_gb_hours": total_mem_gh,
            }
        )

    stats = pd.DataFrame(user_stats).sort_values("cpu_hours", ascending=False)

    # Resource hours table
    print_subheader("Total Resource-Hours (sorted by CPU-hours)")
    print(
        f"  {'User':<25} {'CPU-hrs':>10} {'GPU-hrs':>10} {'Mem GB-hrs':>12} {'Active hrs':>12}"
    )
    print(f"  {'-' * 25} {'-' * 10} {'-' * 10} {'-' * 12} {'-' * 12}")
    for _, r in stats.iterrows():
        print(
            f"  {r['user']:<25} {r['cpu_hours']:>10,.0f} {r['gpu_hours']:>10,.0f} "
            f"{r['mem_gb_hours']:>12,.0f} {r['active_hours']:>12}"
        )
    total_cpu = stats["cpu_hours"].sum()
    total_gpu = stats["gpu_hours"].sum()
    total_mem = stats["mem_gb_hours"].sum()
    print(f"  {'-' * 25} {'-' * 10} {'-' * 10} {'-' * 12} {'-' * 12}")
    print(
        f"  {'TOTAL':<25} {total_cpu:>10,.0f} {total_gpu:>10,.0f} {total_mem:>12,.0f}"
    )

    # Averages & peaks table
    print_subheader("Average & Peak Allocations Per Snapshot")
    print(
        f"  {'User':<25} {'Avg CPUs':>9} {'Peak':>6} {'Avg GPUs':>9} {'Peak':>6} "
        f"{'Avg Mem GB':>11} {'Peak':>8} {'Avg Jobs':>9} {'Peak':>6}"
    )
    print(
        f"  {'-' * 25} {'-' * 9} {'-' * 6} {'-' * 9} {'-' * 6} {'-' * 11} {'-' * 8} {'-' * 9} {'-' * 6}"
    )
    for _, r in stats.iterrows():
        print(
            f"  {r['user']:<25} {r['avg_cpus']:>9.1f} {r['peak_cpus']:>6.0f} "
            f"{r['avg_gpus']:>9.1f} {r['peak_gpus']:>6.0f} "
            f"{r['avg_mem_gb']:>11.1f} {r['peak_mem_gb']:>8.0f} "
            f"{r['avg_running']:>9.1f} {r['peak_running']:>6.0f}"
        )


def section_partition_breakdown(df: pd.DataFrame):
    """Resource usage split by partition."""
    print_header("PARTITION BREAKDOWN")

    for partition in sorted(df["partition"].unique()):
        pdf = df[df["partition"] == partition]
        print_subheader(f"Partition: {partition}")

        user_stats = []
        for user, udf in pdf.groupby("user"):
            user_stats.append(
                {
                    "user": user,
                    "cpu_hours": udf["alloc_cpus"].sum(),
                    "gpu_hours": udf["alloc_gpus"].sum(),
                    "mem_gb_hours": udf["alloc_mem_gb"].sum(),
                    "active_hours": udf["timestamp"].nunique(),
                    "avg_running": udf["running_jobs"].mean(),
                    "avg_pending": udf["pending_jobs"].mean(),
                }
            )

        pstats = pd.DataFrame(user_stats).sort_values("cpu_hours", ascending=False)
        print(
            f"  {'User':<25} {'CPU-hrs':>10} {'GPU-hrs':>10} {'Mem GB-hrs':>12} "
            f"{'Active hrs':>11} {'Avg Run':>8} {'Avg Pend':>9}"
        )
        print(
            f"  {'-' * 25} {'-' * 10} {'-' * 10} {'-' * 12} {'-' * 11} {'-' * 8} {'-' * 9}"
        )
        for _, r in pstats.iterrows():
            print(
                f"  {r['user']:<25} {r['cpu_hours']:>10,.0f} {r['gpu_hours']:>10,.0f} "
                f"{r['mem_gb_hours']:>12,.0f} {r['active_hours']:>11} "
                f"{r['avg_running']:>8.1f} {r['avg_pending']:>9.1f}"
            )


def section_daily_breakdown(df: pd.DataFrame):
    """Per-user daily resource consumption."""
    print_header("DAILY BREAKDOWN PER USER")

    dates = sorted(df["date"].unique())
    users = sorted(df["user"].unique())

    # CPU-hours per user per day
    print_subheader("CPU-Hours Per Day")
    date_labels = [str(d) for d in dates]
    header = (
        f"  {'User':<25}"
        + "".join(f" {d[-5:]:>8}" for d in date_labels)
        + f" {'Total':>8}"
    )
    print(header)
    print(f"  {'-' * 25}" + f" {'-' * 8}" * (len(dates) + 1))

    for user in users:
        udf = df[df["user"] == user]
        row = f"  {user:<25}"
        total = 0
        for d in dates:
            val = udf[udf["date"] == d]["alloc_cpus"].sum()
            total += val
            row += f" {val:>8.0f}"
        row += f" {total:>8.0f}"
        if total > 0:
            print(row)

    # GPU-hours per user per day (only if any GPU usage exists)
    if df["alloc_gpus"].sum() > 0:
        print_subheader("GPU-Hours Per Day")
        print(header)
        print(f"  {'-' * 25}" + f" {'-' * 8}" * (len(dates) + 1))

        for user in users:
            udf = df[df["user"] == user]
            row = f"  {user:<25}"
            total = 0
            for d in dates:
                val = udf[udf["date"] == d]["alloc_gpus"].sum()
                total += val
                row += f" {val:>8.0f}"
            row += f" {total:>8.0f}"
            if total > 0:
                print(row)


def section_active_hours(df: pd.DataFrame):
    """Hours per day each user had running jobs."""
    print_header("ACTIVE HOURS PER DAY (hours with at least 1 running job)")

    dates = sorted(df["date"].unique())
    users = sorted(df["user"].unique())

    # Filter to only snapshots where user had running jobs
    running = df[df["running_jobs"] > 0]

    date_labels = [str(d) for d in dates]
    header = (
        f"  {'User':<25}"
        + "".join(f" {d[-5:]:>6}" for d in date_labels)
        + f" {'Avg/day':>8}"
    )
    print(header)
    print(f"  {'-' * 25}" + f" {'-' * 6}" * len(dates) + f" {'-' * 8}")

    for user in users:
        udf = running[running["user"] == user]
        row = f"  {user:<25}"
        vals = []
        for d in dates:
            hours = udf[udf["date"] == d]["timestamp"].nunique()
            vals.append(hours)
            row += f" {hours:>6}"
        active_days = sum(1 for v in vals if v > 0)
        avg = sum(vals) / active_days if active_days > 0 else 0
        row += f" {avg:>8.1f}"
        if sum(vals) > 0:
            print(row)


def section_time_series(df: pd.DataFrame):
    """Daily cluster-wide totals showing trends."""
    print_header("DAILY CLUSTER TOTALS (across all users)")

    daily = []
    for date, ddf in df.groupby("date"):
        # Per-snapshot totals, then average across snapshots in the day
        per_snap = ddf.groupby("timestamp").agg(
            total_cpus=("alloc_cpus", "sum"),
            total_gpus=("alloc_gpus", "sum"),
            total_mem=("alloc_mem_gb", "sum"),
            total_running=("running_jobs", "sum"),
            total_pending=("pending_jobs", "sum"),
            unique_users=("user", "nunique"),
        )
        daily.append(
            {
                "date": date,
                "snapshots": len(per_snap),
                "avg_users": per_snap["unique_users"].mean(),
                "avg_cpus": per_snap["total_cpus"].mean(),
                "peak_cpus": per_snap["total_cpus"].max(),
                "avg_gpus": per_snap["total_gpus"].mean(),
                "peak_gpus": per_snap["total_gpus"].max(),
                "avg_mem_gb": per_snap["total_mem"].mean(),
                "avg_running": per_snap["total_running"].mean(),
                "avg_pending": per_snap["total_pending"].mean(),
            }
        )

    print(
        f"  {'Date':<12} {'Snaps':>6} {'Users':>6} {'Avg CPUs':>9} {'Peak':>6} "
        f"{'Avg GPUs':>9} {'Peak':>6} {'Avg Mem GB':>11} {'Avg Run':>8} {'Avg Pend':>9}"
    )
    print(
        f"  {'-' * 12} {'-' * 6} {'-' * 6} {'-' * 9} {'-' * 6} {'-' * 9} {'-' * 6} {'-' * 11} {'-' * 8} {'-' * 9}"
    )
    for d in daily:
        print(
            f"  {str(d['date']):<12} {d['snapshots']:>6} {d['avg_users']:>6.1f} "
            f"{d['avg_cpus']:>9.0f} {d['peak_cpus']:>6.0f} "
            f"{d['avg_gpus']:>9.1f} {d['peak_gpus']:>6.0f} "
            f"{d['avg_mem_gb']:>11.0f} {d['avg_running']:>8.1f} {d['avg_pending']:>9.1f}"
        )


def section_completed_jobs(comp_df: pd.DataFrame):
    """Completed job stats if data exists."""
    print_header("COMPLETED JOB STATISTICS")

    if comp_df.empty:
        print("  No completed job data recorded.")
        print("  This is expected if all jobs on aics/aics_gpu are long-running")
        print("  (multi-day) and rarely complete within a single hour window.")
        print()
        print("  To capture historical job data with a wider window, run:")
        earliest = (
            comp_df["timestamp"].min().strftime("%Y-%m-%d")
            if not comp_df.empty
            else "YYYY-MM-DD"
        )
        print(f"    sacct -a -S {earliest} --partition=aics_gpu,aics \\")
        print("      --state=CD,F,TO,CA,OOM --parsable2 \\")
        print(
            "      --format=User,JobID,Partition,State,AllocCPUS,AllocTRES,MaxRSS,Elapsed"
        )
        return

    # If we do have data, show per-user job counts and failure rates
    for user, udf in comp_df.groupby("user"):
        total = len(udf)
        completed = (udf["state"] == "COMPLETED").sum()
        failed = (udf["state"] == "FAILED").sum()
        timeout = (udf["state"] == "TIMEOUT").sum()
        cancelled = (udf["state"] == "CANCELLED").sum()
        oom = (udf["state"] == "OUT_OF_MEMORY").sum()
        print(
            f"  {user:<25} total={total}  completed={completed}  "
            f"failed={failed}  timeout={timeout}  cancelled={cancelled}  oom={oom}"
        )


def main():
    parser = argparse.ArgumentParser(description="SLURM Usage Report Generator")
    parser.add_argument("--days", type=int, help="Only include the last N days")
    parser.add_argument("--output", "-o", type=str, help="Save report to file")
    args = parser.parse_args()

    # Redirect stdout to file if --output specified
    original_stdout = sys.stdout
    if args.output:
        sys.stdout = open(args.output, "w")

    df = load_live_data(args.days)
    comp_df = load_completed_data(args.days)

    print()
    print(
        "╔══════════════════════════════════════════════════════════════════════════════════════════╗"
    )
    print(
        "║                    SLURM RESOURCE USAGE REPORT — aics & aics_gpu                       ║"
    )
    print(
        f"║                    Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<40}            ║"
    )
    print(
        "╚══════════════════════════════════════════════════════════════════════════════════════════╝"
    )

    section_overview(df)
    section_weekly_summary(df)
    section_partition_breakdown(df)
    section_daily_breakdown(df)
    section_active_hours(df)
    section_time_series(df)
    section_completed_jobs(comp_df)

    print()
    print("=" * 90)
    print(" END OF REPORT")
    print("=" * 90)

    if args.output:
        sys.stdout.close()
        sys.stdout = original_stdout
        print(f"Report saved to: {args.output}")


if __name__ == "__main__":
    main()
