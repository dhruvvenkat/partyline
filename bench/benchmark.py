#!/usr/bin/env python3
"""Loopback scale benchmark for the poll-based chat server."""

import argparse
import csv
import heapq
import os
import platform
import selectors
import socket
import statistics
import subprocess
import tempfile
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HOST = "127.0.0.1"
PROMPT = b"enter your username: "
WHERE_RESPONSE = b"you are in room main-room"


def csv_values(value, cast=str):
    values = [cast(item.strip()) for item in value.split(",") if item.strip()]
    if not values:
        raise argparse.ArgumentTypeError("expected a comma-separated value")
    return values


def percentile(samples, fraction):
    if not samples:
        return None
    ordered = sorted(samples)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((HOST, 0))
        return sock.getsockname()[1]


def receive_until(sock, marker, timeout=5.0):
    deadline = time.monotonic() + timeout
    received = b""
    while marker not in received:
        sock.settimeout(max(0.0, deadline - time.monotonic()))
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError("server closed a connection during setup")
        received += chunk


def connect_clients(port, count):
    clients = []
    for index in range(count):
        deadline = time.monotonic() + 5.0
        while True:
            try:
                sock = socket.create_connection((HOST, port), timeout=1.0)
                break
            except ConnectionRefusedError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("server did not start listening")
                time.sleep(0.01)
        receive_until(sock, PROMPT)
        sock.sendall(f"bench-{index}\n".encode())
        sock.setblocking(False)
        clients.append(
            {"sock": sock, "buffer": b"", "pending": deque(), "closed": False}
        )
    return clients


def process_reads(selector, clients, state, events):
    for key, _ in events:
        index = key.data
        client = clients[index]
        while True:
            try:
                data = client["sock"].recv(65536)
            except BlockingIOError:
                break
            except OSError:
                data = b""

            if not data:
                if not client["closed"]:
                    client["closed"] = True
                    state["disconnects"].add(index)
                    selector.unregister(client["sock"])
                break

            received_ns = time.monotonic_ns()
            by_deadline = received_ns <= state["deadline_ns"]

            if state["workload"] == "broadcast":
                delivered = data.count(b"\n")
                state["deliveries"] += delivered
                if by_deadline:
                    state["deliveries_by_deadline"] += delivered
                if index != state["observer"]:
                    continue

            client["buffer"] += data
            while b"\n" in client["buffer"]:
                line, client["buffer"] = client["buffer"].split(b"\n", 1)
                if state["workload"] == "direct":
                    if WHERE_RESPONSE not in line or not client["pending"]:
                        continue
                    sent_ns = client["pending"].popleft()
                    state["completed"] += 1
                    state["deliveries"] += 1
                    if by_deadline:
                        state["completed_by_deadline"] += 1
                        state["deliveries_by_deadline"] += 1
                    state["latencies_ms"].append(
                        (received_ns - sent_ns) / 1_000_000
                    )
                    continue

                marker = line.find(b"BENCH|")
                if marker == -1:
                    continue
                fields = line[marker:].split(b"|", 5)
                if len(fields) != 6 or fields[1] != state["token"]:
                    continue
                try:
                    sent_ns = int(fields[4])
                except ValueError:
                    continue
                state["completed"] += 1
                if by_deadline:
                    state["completed_by_deadline"] += 1
                state["latencies_ms"].append(
                    (received_ns - sent_ns) / 1_000_000
                )


def wait_for_events(selector, clients, state, timeout):
    events = selector.select(timeout)
    if events:
        process_reads(selector, clients, state, events)


def settle(selector, clients, max_wait=5.0, quiet=0.1):
    deadline = time.monotonic() + max_wait
    last_data = time.monotonic()
    while time.monotonic() < deadline and time.monotonic() - last_data < quiet:
        events = selector.select(quiet)
        if not events:
            continue
        last_data = time.monotonic()
        for key, _ in events:
            client = clients[key.data]
            while True:
                try:
                    data = client["sock"].recv(65536)
                except BlockingIOError:
                    break
                if not data:
                    raise RuntimeError("server disconnected a client during setup")
    for client in clients:
        client["buffer"] = b""
        client["pending"].clear()


def server_cpu_seconds(pid):
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        fields = stat[stat.rfind(")") + 2 :].split()
        ticks = int(fields[11]) + int(fields[12])
        return ticks / os.sysconf("SC_CLK_TCK")
    except (FileNotFoundError, IndexError, OSError, ValueError):
        return None


def run_phase(selector, clients, workload, rate, duration, payload_size, token):
    participant_count = len(clients) if workload == "direct" else len(clients) - 1
    if participant_count < 1:
        raise ValueError("broadcast workload needs at least two clients")

    start = time.monotonic()
    deadline = start + duration
    period = 1.0 / rate
    schedule = [
        (start + index * period / participant_count, index, 0)
        for index in range(participant_count)
    ]
    heapq.heapify(schedule)
    state = {
        "workload": workload,
        "token": token,
        "observer": len(clients) - 1,
        "deadline_ns": int(deadline * 1_000_000_000),
        "offered": 0,
        "completed": 0,
        "completed_by_deadline": 0,
        "deliveries": 0,
        "deliveries_by_deadline": 0,
        "latencies_ms": [],
        "scheduler_lag_ms": [],
        "disconnects": set(),
        "send_errors": 0,
    }
    filler = b"x" * payload_size

    while True:
        now = time.monotonic()
        if now >= deadline:
            break

        while schedule and schedule[0][0] <= now:
            scheduled, index, sequence = heapq.heappop(schedule)
            if scheduled >= deadline:
                continue
            client = clients[index]
            sent_ns = time.monotonic_ns()
            if workload == "direct":
                frame = b"WHERE\n"
            else:
                frame = (
                    b"BENCH|"
                    + token
                    + f"|{index}|{sequence}|{sent_ns}|".encode()
                    + filler
                    + b"\n"
                )

            try:
                sent = client["sock"].send(frame)
                if sent != len(frame):
                    raise RuntimeError(f"partial benchmark send ({sent}/{len(frame)})")
            except (BlockingIOError, OSError, RuntimeError):
                state["send_errors"] += 1
            else:
                state["offered"] += 1
                state["scheduler_lag_ms"].append(
                    max(0.0, (sent_ns / 1_000_000_000 - scheduled) * 1000)
                )
                if workload == "direct":
                    client["pending"].append(sent_ns)

            heapq.heappush(schedule, (scheduled + period, index, sequence + 1))
            now = time.monotonic()

        next_send = schedule[0][0] if schedule else deadline
        wait_for_events(
            selector,
            clients,
            state,
            max(0.0, min(0.05, next_send - now, deadline - now)),
        )

    return state


def drain_phase(selector, clients, state, max_wait):
    deadline = time.monotonic() + max_wait
    quiet_since = time.monotonic()
    previous_deliveries = state["deliveries"]
    while time.monotonic() < deadline and time.monotonic() - quiet_since < 0.1:
        wait_for_events(selector, clients, state, 0.05)
        if state["deliveries"] != previous_deliveries:
            previous_deliveries = state["deliveries"]
            quiet_since = time.monotonic()


def run_trial(server_path, label, workload, client_count, run, args):
    port = free_port()
    environment = os.environ.copy()
    environment["CHAT_SERVER_PORT"] = str(port)
    log = tempfile.TemporaryFile(mode="w+t")
    server = subprocess.Popen(
        [str(server_path)],
        cwd=ROOT,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=log,
    )
    selector = selectors.DefaultSelector()
    clients = []
    state = None

    try:
        clients = connect_clients(port, client_count)
        for index, client in enumerate(clients):
            selector.register(client["sock"], selectors.EVENT_READ, index)
        settle(selector, clients)

        if args.warmup:
            warmup = run_phase(
                selector,
                clients,
                workload,
                args.rate,
                args.warmup,
                args.payload_size,
                b"warmup",
            )
            drain_phase(selector, clients, warmup, args.drain)
            if warmup["disconnects"] or warmup["send_errors"]:
                raise RuntimeError(
                    "client failure during warmup: "
                    f"disconnects={sorted(warmup['disconnects'])} "
                    f"send_errors={warmup['send_errors']}"
                )
            if workload == "direct" and any(c["pending"] for c in clients):
                raise RuntimeError("direct responses remained pending after warmup")
            for client in clients:
                client["buffer"] = b""

        cpu_start = server_cpu_seconds(server.pid)
        state = run_phase(
            selector,
            clients,
            workload,
            args.rate,
            args.duration,
            args.payload_size,
            f"{client_count}-{run}".encode(),
        )
        cpu_end = server_cpu_seconds(server.pid)
        drain_phase(selector, clients, state, args.drain)

        expected_deliveries = state["offered"] * (
            client_count - 1 if workload == "broadcast" else 1
        )
        latency = state["latencies_ms"]
        cpu_percent = (
            (cpu_end - cpu_start) / args.duration * 100
            if cpu_start is not None and cpu_end is not None
            else None
        )
        result = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "label": label,
            "server": str(server_path),
            "workload": workload,
            "clients": client_count,
            "run": run,
            "duration_s": args.duration,
            "rate_per_client_s": args.rate,
            "payload_bytes": 0 if workload == "direct" else args.payload_size,
            "offered": state["offered"],
            "offered_msg_s": state["offered"] / args.duration,
            "completed": state["completed"],
            "completed_by_deadline_msg_s": (
                state["completed_by_deadline"] / args.duration
            ),
            "delivered": state["deliveries"],
            "delivered_by_deadline_msg_s": (
                state["deliveries_by_deadline"] / args.duration
            ),
            "delivery_ratio": (
                state["deliveries"] / expected_deliveries
                if expected_deliveries
                else 0.0
            ),
            "latency_samples": len(latency),
            "p50_ms": percentile(latency, 0.50),
            "p95_ms": percentile(latency, 0.95),
            "p99_ms": percentile(latency, 0.99),
            "server_cpu_percent": cpu_percent,
            "scheduler_lag_p95_ms": percentile(
                state["scheduler_lag_ms"], 0.95
            ),
            "client_disconnects": len(state["disconnects"]),
            "send_errors": state["send_errors"],
            "disconnect_reasons": "",
        }
    finally:
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=2)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
        for client in clients:
            client["sock"].close()
        selector.close()

    log.seek(0)
    disconnect_reasons = [
        line.strip() for line in log if line.startswith("disconnect ")
    ]
    log.close()
    result["disconnect_reasons"] = " | ".join(disconnect_reasons)
    return result


def display(value, unit=""):
    return "n/a" if value is None else f"{value:.2f}{unit}"


def print_result(row):
    print(
        f"{row['workload']:9} clients={row['clients']:4} run={row['run']} "
        f"offered={row['offered_msg_s']:.1f}/s "
        f"completed={row['completed_by_deadline_msg_s']:.1f}/s "
        f"delivered={row['delivered_by_deadline_msg_s']:.1f}/s "
        f"ratio={row['delivery_ratio']:.4f} "
        f"p50={display(row['p50_ms'], 'ms')} "
        f"p95={display(row['p95_ms'], 'ms')} "
        f"cpu={display(row['server_cpu_percent'], '%')} "
        f"lag_p95={display(row['scheduler_lag_p95_ms'], 'ms')} "
        f"disconnects={row['client_disconnects']} "
        f"send_errors={row['send_errors']}",
        flush=True,
    )


def print_summary(rows):
    print("\nmedian across runs")
    for workload in sorted({row["workload"] for row in rows}):
        for clients in sorted({row["clients"] for row in rows}):
            group = [
                row
                for row in rows
                if row["workload"] == workload and row["clients"] == clients
            ]
            if not group:
                continue
            failures = sum(
                row["client_disconnects"]
                or row["send_errors"]
                or row["delivery_ratio"] < 0.999
                for row in group
            )
            print(
                f"{workload:9} clients={clients:4} "
                f"offered={statistics.median(r['offered_msg_s'] for r in group):.1f}/s "
                f"completed={statistics.median(r['completed_by_deadline_msg_s'] for r in group):.1f}/s "
                f"delivered={statistics.median(r['delivered_by_deadline_msg_s'] for r in group):.1f}/s "
                f"ratio={statistics.median(r['delivery_ratio'] for r in group):.4f} "
                f"p50={display(statistics.median(r['p50_ms'] for r in group if r['p50_ms'] is not None), 'ms')} "
                f"p95={display(statistics.median(r['p95_ms'] for r in group if r['p95_ms'] is not None), 'ms')} "
                f"cpu={display(statistics.median(r['server_cpu_percent'] for r in group if r['server_cpu_percent'] is not None), '%')} "
                f"failures={failures}/{len(group)}"
            )


def main():
    parser = argparse.ArgumentParser(
        description="Measure direct-response and room-broadcast scaling"
    )
    parser.add_argument("--server", default=str(ROOT / "server"))
    parser.add_argument("--label", default="poll")
    parser.add_argument(
        "--tiers", type=lambda value: csv_values(value, int), default=[10, 50, 100, 500]
    )
    parser.add_argument(
        "--workloads",
        type=csv_values,
        default=["direct", "broadcast"],
        help="comma-separated: direct,broadcast",
    )
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--warmup", type=float, default=2.0)
    parser.add_argument("--drain", type=float, default=2.0)
    parser.add_argument("--rate", type=float, default=3.0)
    parser.add_argument("--payload-size", type=int, default=128)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    server_path = Path(args.server).resolve()
    if not server_path.is_file() or not os.access(server_path, os.X_OK):
        parser.error(f"server is not executable: {server_path}")
    if any(tier < 2 for tier in args.tiers):
        parser.error("every tier must contain at least two clients")
    if any(workload not in {"direct", "broadcast"} for workload in args.workloads):
        parser.error("workloads must be direct and/or broadcast")
    if args.runs < 1 or min(args.duration, args.rate, args.drain) <= 0:
        parser.error("runs, duration, rate, and drain must be positive")
    if args.warmup < 0 or not 0 <= args.payload_size <= 4000:
        parser.error("warmup and payload size are out of range")
    print(
        f"server={server_path} label={args.label} "
        f"system={platform.system()} {platform.release()} "
        f"python={platform.python_version()} selector={selectors.DefaultSelector.__name__}",
        flush=True,
    )
    rows = []
    for workload in args.workloads:
        for tier in args.tiers:
            for run in range(1, args.runs + 1):
                row = run_trial(
                    server_path, args.label, workload, tier, run, args
                )
                rows.append(row)
                print_result(row)

    print_summary(rows)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nwrote {args.output}")


if __name__ == "__main__":
    main()
