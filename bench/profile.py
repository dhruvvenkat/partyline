#!/usr/bin/env python3
"""Run one non-headline perf-stat or strace diagnostic trial."""

import argparse
import json
import os
import platform
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bench.compare import ROOT, SERVERS, command_output, new_result_directory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("perf", "strace"), default="perf")
    parser.add_argument("--implementation", choices=tuple(SERVERS), default="epoll")
    parser.add_argument("--result-root", type=Path, default=ROOT / "bench/results")
    parser.add_argument("--total-connections", type=int, default=100)
    parser.add_argument("--active-connections", type=int, default=100)
    parser.add_argument("--rate", type=float, default=5000)
    parser.add_argument("--duration", type=float, default=2)
    parser.add_argument("--warmup", type=float, default=0.5)
    parser.add_argument("--workers", type=int, default=max(1, min(4, os.cpu_count() or 1)))
    parser.add_argument("--server-cpu", type=int)
    parser.add_argument("--worker-cpus", default="")
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args()

    if not args.no_build:
        subprocess.run(["make", "release"], cwd=ROOT, check=True)
    sha = command_output("git", "rev-parse", "HEAD")
    result_directory = new_result_directory(
        args.result_root, f"diagnostic-{args.mode}-{args.implementation}", sha[:7]
    )
    output = result_directory / "results.csv"
    profile_output = result_directory / f"{args.mode}-server.txt"
    command = [
        sys.executable, str(ROOT / "bench/benchmark.py"),
        "--server", str(SERVERS[args.implementation]), "--label", args.implementation,
        "--experiment", "dense", "--tiers", str(args.total_connections),
        "--active-connections", str(args.active_connections), "--workloads", "direct",
        "--rate", str(args.rate), "--runs", "1", "--duration", str(args.duration),
        "--warmup", str(args.warmup), "--drain", "1", "--workers", str(args.workers),
        "--profile", args.mode, "--profile-output", str(profile_output),
        "--output", str(output),
    ]
    if args.server_cpu is not None:
        command.extend(("--server-cpu", str(args.server_cpu)))
    if args.worker_cpus:
        command.extend(("--worker-cpus", args.worker_cpus))
    metadata = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "git_sha": sha,
        "git_dirty": bool(command_output("git", "status", "--porcelain=v1")),
        "kernel": platform.release(),
        "mode": args.mode,
        "server": str(SERVERS[args.implementation]),
        "command": shlex.join(command),
        "latency_claims_allowed": False,
    }
    completed = subprocess.run(command, cwd=ROOT)
    metadata["returncode"] = completed.returncode
    try:
        metadata["perf_event_paranoid"] = Path(
            "/proc/sys/kernel/perf_event_paranoid"
        ).read_text().strip()
    except OSError:
        metadata["perf_event_paranoid"] = "unavailable"
    (result_directory / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(f"diagnostic_results={result_directory}")
    raise SystemExit(completed.returncode)


if __name__ == "__main__":
    main()
