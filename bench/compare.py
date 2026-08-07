#!/usr/bin/env python3
"""Build reproducible poll/epoll comparisons in a fresh result directory."""

import argparse
import csv
import json
import os
import platform
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HARNESS_VERSION = "comparison-v1"
RELEASE_FLAGS = "-std=c++17 -Wall -Wextra -pedantic -O2 -DNDEBUG"
SERVERS = {
    "poll": ROOT / "poll(2)" / "server",
    "epoll": ROOT / "epoll" / "server",
}


def command_output(*command):
    return subprocess.run(
        command, cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE
    ).stdout.strip()


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


def metadata(args, result_directory, implementations, git_sha, git_dirty):
    return {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "result_directory": str(result_directory),
        "git_sha": git_sha,
        "git_dirty": git_dirty,
        "compiler": command_output(os.environ.get("CXX", "g++"), "--version").splitlines()[0],
        "release_flags": RELEASE_FLAGS,
        "kernel": platform.release(),
        "system": platform.platform(),
        "cpu": cpu_model(),
        "harness_version": HARNESS_VERSION,
        "workers": args.workers,
        "affinity": "none",
        "invocation": shlex.join(sys.argv),
        "implementations": list(implementations),
        "servers": {name: str(SERVERS[name]) for name in implementations},
        "parameters": {
            "tiers": args.tiers,
            "workloads": args.workloads,
            "rate_per_client": args.rate,
            "runs": args.runs,
            "duration": args.duration,
            "warmup": args.warmup,
            "drain": args.drain,
            "payload_size": args.payload_size,
        },
        "commands": [],
    }


def combine_csvs(paths, destination):
    rows = []
    for path in paths:
        with path.open(newline="") as source:
            rows.extend(csv.DictReader(source))
    if not rows:
        return
    with destination.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--implementations", default="poll,epoll")
    parser.add_argument("--name", default="comparison")
    parser.add_argument("--result-root", type=Path, default=ROOT / "bench" / "results")
    parser.add_argument("--tiers", default="10,50,100,500")
    parser.add_argument("--workloads", default="direct")
    parser.add_argument("--rate", type=float, default=100.0)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--warmup", type=float, default=2.0)
    parser.add_argument("--drain", type=float, default=2.0)
    parser.add_argument("--payload-size", type=int, default=128)
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()

    implementations = tuple(
        name.strip() for name in args.implementations.split(",") if name.strip()
    )
    if not implementations or any(name not in SERVERS for name in implementations):
        parser.error("implementations must contain poll and/or epoll")
    if args.runs < 1:
        parser.error("runs must be positive")
    missing = [str(SERVERS[name]) for name in implementations if not SERVERS[name].is_file()]
    if missing:
        parser.error(f"release server not built: {', '.join(missing)}")

    git_sha = command_output("git", "rev-parse", "HEAD")
    git_dirty = bool(command_output("git", "status", "--porcelain"))
    result_directory = new_result_directory(args.result_root, args.name, git_sha[:7])
    run_metadata = metadata(
        args, result_directory, implementations, git_sha, git_dirty
    )
    csv_paths = []
    try:
        for trial in range(1, args.runs + 1):
            for label in execution_order(implementations, trial):
                output = result_directory / f"trial-{trial:02d}-{label}.csv"
                command = [
                    sys.executable,
                    str(ROOT / "bench" / "benchmark.py"),
                    "--server", str(SERVERS[label]),
                    "--label", label,
                    "--tiers", args.tiers,
                    "--workloads", args.workloads,
                    "--rate", str(args.rate),
                    "--runs", "1",
                    "--duration", str(args.duration),
                    "--warmup", str(args.warmup),
                    "--drain", str(args.drain),
                    "--payload-size", str(args.payload_size),
                    "--workers", str(args.workers),
                    "--output", str(output),
                ]
                command_record = {
                    "trial": trial,
                    "implementation": label,
                    "server": str(SERVERS[label]),
                    "command": shlex.join(command),
                }
                run_metadata["commands"].append(command_record)
                print(f"\ntrial={trial} implementation={label}", flush=True)
                completed = subprocess.run(command, cwd=ROOT)
                command_record["returncode"] = completed.returncode
                if completed.returncode:
                    raise subprocess.CalledProcessError(completed.returncode, command)
                csv_paths.append(output)
    finally:
        (result_directory / "metadata.json").write_text(
            json.dumps(run_metadata, indent=2) + "\n"
        )

    combine_csvs(csv_paths, result_directory / "results.csv")
    print(f"\nresults={result_directory}")


if __name__ == "__main__":
    main()
