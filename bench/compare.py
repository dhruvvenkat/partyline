#!/usr/bin/env python3
"""Build and run reproducible sparse, dense, and broadcast comparisons."""

import argparse
import csv
import json
import os
import platform
import resource
import shlex
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HARNESS_VERSION = "comparison-v2-process-validity"
RELEASE_FLAGS = "-std=c++17 -Wall -Wextra -pedantic -O2 -DNDEBUG"
SERVERS = {
    "poll": ROOT / "poll(2)" / "server",
    "epoll": ROOT / "epoll" / "server",
}
MEDIAN_FIELDS = (
    "offered_msg_s", "completed_by_deadline_msg_s", "inbound_messages_s",
    "outbound_deliveries_s", "per_socket_receive_rate_s", "delivery_ratio",
    "deadline_completion_ratio", "p50_ms", "p95_ms", "p99_ms",
    "server_cpu_percent", "generator_cpu_percent", "cpu_per_completed_request_us",
    "scheduling_lag_p95_ms", "scheduling_lag_p99_ms", "max_read_service_gap_ms",
    "wait_calls", "total_events", "events_per_wait_p50", "events_per_wait_p95",
    "events_per_wait_max", "full_event_batches", "epoll_ctl_add", "epoll_ctl_mod",
    "epoll_ctl_del", "immediate_write_successes", "partial_writes", "send_calls",
    "recv_calls", "send_eagain", "recv_eagain", "bytes_read", "bytes_written",
    "queue_high_water_bytes", "queue_high_water_messages",
)


def command_output(*command):
    return subprocess.run(
        command, cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE
    ).stdout.strip()


def comma_values(value, cast=str):
    return [cast(item.strip()) for item in value.split(",") if item.strip()]


def execution_order(implementations, trial):
    return implementations if trial % 2 else tuple(reversed(implementations))


def new_result_directory(root, name, sha=None):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    sha = sha or command_output("git", "rev-parse", "--short", "HEAD")
    base = root / f"{timestamp}-{name}-{sha}"
    candidate = base
    suffix = 2
    while candidate.exists():
        candidate = Path(f"{base}-{suffix}")
        suffix += 1
    candidate.mkdir(parents=True)
    return candidate


def cpu_model():
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def experiment_groups(args):
    selected = set(comma_values(args.experiments))
    unknown = selected - {"sparse", "dense", "broadcast"}
    if unknown:
        raise ValueError("unknown experiments: " + ",".join(sorted(unknown)))
    groups = []
    if "sparse" in selected:
        for total in comma_values(args.sparse_tiers, int):
            for active in comma_values(args.sparse_active, int):
                if active <= total:
                    groups.append({
                        "experiment": "sparse", "workload": "direct",
                        "total": total, "active": active, "rates": [args.sparse_rate],
                    })
    if "dense" in selected:
        rates = sorted(comma_values(args.dense_rates, float))
        for total in comma_values(args.dense_tiers, int):
            groups.append({
                "experiment": "dense", "workload": "direct",
                "total": total, "active": total, "rates": rates,
            })
    if "broadcast" in selected:
        rates = sorted(comma_values(args.broadcast_rates, float))
        for total in comma_values(args.broadcast_tiers, int):
            groups.append({
                "experiment": "broadcast", "workload": "broadcast",
                "total": total, "active": total, "rates": rates,
            })
    return groups


def metadata(args, result_directory, implementations, git_sha, dirty_status, groups):
    soft_fds, hard_fds = resource.getrlimit(resource.RLIMIT_NOFILE)
    return {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "result_directory": str(result_directory),
        "git_sha": git_sha,
        "git_dirty": bool(dirty_status),
        "git_dirty_status": dirty_status.splitlines(),
        "compiler": command_output(os.environ.get("CXX", "g++"), "--version").splitlines()[0],
        "release_flags": RELEASE_FLAGS,
        "kernel": platform.release(),
        "system": platform.platform(),
        "cpu": cpu_model(),
        "logical_cpus": os.cpu_count(),
        "fd_limit_soft": soft_fds,
        "fd_limit_hard": hard_fds,
        "harness_version": HARNESS_VERSION,
        "worker_processes": args.workers,
        "affinity": {
            "server_cpu": args.server_cpu,
            "worker_cpus": comma_values(args.worker_cpus, int) if args.worker_cpus else [],
        },
        "invocation": shlex.join(sys.argv),
        "implementations": list(implementations),
        "servers": {name: str(SERVERS[name]) for name in implementations},
        "workload_groups": groups,
        "parameters": {
            "runs": args.runs, "duration": args.duration, "warmup": args.warmup,
            "warmup_rate": args.warmup_rate, "drain": args.drain,
            "payload_size": args.payload_size,
            "min_delivery_ratio": args.min_delivery_ratio,
            "min_deadline_completion_ratio": args.min_deadline_completion_ratio,
            "max_missed_offer_ratio": args.max_missed_offer_ratio,
            "max_scheduler_lag_ms": args.max_scheduler_lag_ms,
            "max_read_service_gap_ms": args.max_read_service_gap_ms,
            "max_latency_p99_ms": args.max_latency_p99_ms,
        },
        "historical_results_compatible": False,
        "commands": [],
        "skipped": [],
    }


def combine_csvs(paths, destination):
    rows = []
    fields = []
    for path in paths:
        with path.open(newline="") as source:
            for row in csv.DictReader(source):
                rows.append(row)
                for field in row:
                    if field not in fields:
                        fields.append(field)
    if rows:
        with destination.open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
    return rows


def row_valid(row):
    return str(row.get("valid", "")).lower() in {"1", "true", "yes"}


def number(row, field):
    value = row.get(field)
    try:
        return float(value) if value not in (None, "", "None") else None
    except ValueError:
        return None


def write_summaries(rows, result_directory, runs):
    grouped = {}
    invalid = []
    for row in rows:
        if not row_valid(row):
            invalid.append(row)
            continue
        key = (
            row["experiment"], row["label"], int(row["total_connections"]),
            int(row["active_connections"]), float(row["global_offered_rate_s"]),
        )
        grouped.setdefault(key, []).append(row)

    headlines = []
    for key, valid_rows in sorted(grouped.items()):
        experiment, label, total, active, rate = key
        headline = {
            "experiment": experiment, "label": label,
            "total_connections": total, "active_connections": active,
            "global_offered_rate_s": rate, "valid_trials": len(valid_rows),
            "required_trials": runs,
        }
        for field in MEDIAN_FIELDS:
            values = [number(row, field) for row in valid_rows]
            values = [value for value in values if value is not None]
            headline[f"median_{field}"] = statistics.median(values) if values else None
        headlines.append(headline)

    if headlines:
        with (result_directory / "headline-valid-medians.csv").open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=headlines[0].keys())
            writer.writeheader()
            writer.writerows(headlines)
    if invalid:
        with (result_directory / "invalid-trials.csv").open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=invalid[0].keys())
            writer.writeheader()
            writer.writerows(invalid)

    highest = []
    keys = {(row["experiment"], row["label"], int(row["total_connections"])) for row in rows}
    for experiment, label, total in sorted(keys):
        if experiment not in {"dense", "broadcast"}:
            continue
        candidates = []
        rates = {float(row["global_offered_rate_s"]) for row in rows if (
            row["experiment"], row["label"], int(row["total_connections"])
        ) == (experiment, label, total)}
        for rate in rates:
            rate_rows = [row for row in rows if (
                row["experiment"], row["label"], int(row["total_connections"]),
                float(row["global_offered_rate_s"])
            ) == (experiment, label, total, rate)]
            if len(rate_rows) == runs and all(row_valid(row) for row in rate_rows):
                candidates.append(rate)
        highest.append({
            "experiment": experiment, "label": label, "total_connections": total,
            "highest_sustainable_valid_rate": max(candidates) if candidates else None,
        })

    summary = {
        "valid_trials": sum(row_valid(row) for row in rows),
        "invalid_trials": len(invalid),
        "highest_sustainable": highest,
    }
    (result_directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def benchmark_command(args, group, rate, label, output, trial=1):
    command = [
        sys.executable, str(ROOT / "bench" / "benchmark.py"),
        "--server", str(SERVERS[label]), "--label", label,
        "--experiment", group["experiment"], "--tiers", str(group["total"]),
        "--active-connections", str(group["active"]),
        "--workloads", group["workload"], "--rate", str(rate), "--runs", "1",
        "--trial-id", str(trial),
        "--duration", str(args.duration), "--warmup", str(args.warmup),
        "--warmup-rate", str(args.warmup_rate), "--drain", str(args.drain),
        "--payload-size", str(args.payload_size),
        "--workers", str(args.workers),
        "--min-delivery-ratio", str(args.min_delivery_ratio),
        "--min-deadline-completion-ratio", str(args.min_deadline_completion_ratio),
        "--max-missed-offer-ratio", str(args.max_missed_offer_ratio),
        "--max-scheduler-lag-ms", str(args.max_scheduler_lag_ms),
        "--max-read-service-gap-ms", str(args.max_read_service_gap_ms),
        "--max-latency-p99-ms", str(args.max_latency_p99_ms),
        "--output", str(output),
    ]
    if args.server_cpu is not None:
        command.extend(("--server-cpu", str(args.server_cpu)))
    if args.worker_cpus:
        command.extend(("--worker-cpus", args.worker_cpus))
    return command


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--implementations", default="poll,epoll")
    parser.add_argument("--name", default="comparison")
    parser.add_argument("--result-root", type=Path, default=ROOT / "bench/results")
    parser.add_argument("--experiments", default="sparse,dense,broadcast")
    parser.add_argument("--sparse-tiers", default="1000,5000,10000,50000")
    parser.add_argument("--sparse-active", default="1,10,100")
    parser.add_argument("--sparse-rate", type=float, default=1000.0)
    parser.add_argument("--dense-tiers", default="100,500,1000")
    parser.add_argument("--dense-rates", default="1000,5000,10000,25000,50000,100000")
    parser.add_argument("--broadcast-tiers", default="10,100,500")
    parser.add_argument("--broadcast-rates", default="100,500,1000,5000")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--warmup", type=float, default=1.0)
    parser.add_argument("--warmup-rate", type=float, default=1000.0)
    parser.add_argument("--drain", type=float, default=2.0)
    parser.add_argument("--payload-size", type=int, default=128)
    parser.add_argument("--workers", type=int, default=max(1, min(4, os.cpu_count() or 1)))
    parser.add_argument("--server-cpu", type=int)
    parser.add_argument("--worker-cpus", default="")
    parser.add_argument("--min-delivery-ratio", type=float, default=0.999)
    parser.add_argument("--min-deadline-completion-ratio", type=float, default=0.99)
    parser.add_argument("--max-missed-offer-ratio", type=float, default=0.01)
    parser.add_argument("--max-scheduler-lag-ms", type=float, default=20.0)
    parser.add_argument("--max-read-service-gap-ms", type=float, default=20.0)
    parser.add_argument("--max-latency-p99-ms", type=float, default=50.0)
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args()

    implementations = tuple(comma_values(args.implementations))
    if not implementations or any(name not in SERVERS for name in implementations):
        parser.error("implementations must contain poll and/or epoll")
    if args.runs < 1 or min(args.duration, args.drain) <= 0 or args.warmup < 0:
        parser.error("invalid run durations")
    try:
        groups = experiment_groups(args)
    except ValueError as error:
        parser.error(str(error))
    if not groups:
        parser.error("no experiment groups selected")
    worker_cpus = comma_values(args.worker_cpus, int) if args.worker_cpus else []
    if args.server_cpu is not None and args.server_cpu in worker_cpus:
        parser.error("server and worker CPU affinity sets must not overlap")

    if not args.no_build:
        subprocess.run(["make", "release"], cwd=ROOT, check=True)
    missing = [str(SERVERS[name]) for name in implementations if not os.access(SERVERS[name], os.X_OK)]
    if missing:
        parser.error(f"release server not built: {', '.join(missing)}")

    git_sha = command_output("git", "rev-parse", "HEAD")
    dirty_status = command_output("git", "status", "--porcelain=v1")
    result_directory = new_result_directory(args.result_root, args.name, git_sha[:7])
    run_metadata = metadata(
        args, result_directory, implementations, git_sha, dirty_status, groups
    )
    csv_paths = []
    try:
        for trial in range(1, args.runs + 1):
            for group in groups:
                active_implementations = set(implementations)
                for rate in group["rates"]:
                    rate_tag = str(rate).replace(".", "p")
                    for label in execution_order(implementations, trial):
                        if label not in active_implementations:
                            run_metadata["skipped"].append({
                                "trial": trial, "implementation": label,
                                **group, "rate": rate,
                                "reason": "lower offered rate was invalid",
                            })
                            continue
                        output = result_directory / (
                            f"trial-{trial:02d}-{group['experiment']}-t{group['total']}-"
                            f"a{group['active']}-r{rate_tag}-{label}.csv"
                        )
                        command = benchmark_command(args, group, rate, label, output, trial)
                        record = {
                            "trial": trial, "implementation": label,
                            "server": str(SERVERS[label]), "experiment": group["experiment"],
                            "workload": group["workload"], "total_connections": group["total"],
                            "active_connections": group["active"], "global_rate": rate,
                            "command": shlex.join(command),
                        }
                        run_metadata["commands"].append(record)
                        print(
                            f"\ntrial={trial} experiment={group['experiment']} "
                            f"total={group['total']} active={group['active']} "
                            f"rate={rate:g} implementation={label}", flush=True,
                        )
                        completed = subprocess.run(command, cwd=ROOT)
                        record["returncode"] = completed.returncode
                        if completed.returncode or not output.exists():
                            active_implementations.discard(label)
                            continue
                        csv_paths.append(output)
                        with output.open(newline="") as source:
                            row = next(csv.DictReader(source))
                        if not row_valid(row):
                            active_implementations.discard(label)
    finally:
        (result_directory / "metadata.json").write_text(
            json.dumps(run_metadata, indent=2) + "\n"
        )

    rows = combine_csvs(csv_paths, result_directory / "results.csv")
    summary = write_summaries(rows, result_directory, args.runs)
    print(
        f"\nvalid_trials={summary['valid_trials']} invalid_trials={summary['invalid_trials']}"
    )
    for item in summary["highest_sustainable"]:
        print(
            f"highest_sustainable experiment={item['experiment']} label={item['label']} "
            f"total={item['total_connections']} rate={item['highest_sustainable_valid_rate']}"
        )
    print(f"results={result_directory}")


if __name__ == "__main__":
    main()
