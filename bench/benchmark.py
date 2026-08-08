#!/usr/bin/env python3
"""Process-based load generator for the poll and epoll chat servers."""

import argparse
import csv
import heapq
import json
import multiprocessing
import os
import platform
import selectors
import signal
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
MAX_OVERDUE_SENDS = 32
MAX_RECV_CALLS = 4
MAX_RECV_BYTES = 65536
MAX_CLIENT_SEND_QUEUE = 65536
START_DELAY_S = 0.2


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


def receive_until(sock, marker, timeout=10.0):
    deadline = time.monotonic() + timeout
    received = b""
    while marker not in received:
        sock.settimeout(max(0.01, deadline - time.monotonic()))
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError("server closed a connection during setup")
        received += chunk


def partition_clients(clients, worker_count):
    return [clients[offset::worker_count] for offset in range(worker_count)]


def rotate_ready(events, cursor):
    if not events:
        return [], cursor
    offset = cursor % len(events)
    return events[offset:] + events[:offset], (offset + 1) % len(events)


def due_offers(schedule, now_ns, max_count, max_lag_ns):
    ready = []
    while schedule and schedule[0][0] <= now_ns and len(ready) < max_count:
        scheduled_ns, *rest = heapq.heappop(schedule)
        ready.append((scheduled_ns, now_ns - scheduled_ns > max_lag_ns, *rest))
    return ready


def empty_state(worker_id=0):
    return {
        "worker_id": worker_id,
        "offered_operations": 0,
        "sent_operations": 0,
        "missed_offers": 0,
        "completed_by_deadline": 0,
        "eventual_completions": 0,
        "deliveries": 0,
        "deliveries_by_deadline": 0,
        "send_errors": 0,
        "disconnects": set(),
        "latencies_ms": [],
        "scheduling_lag_ms": [],
        "max_read_service_gap_ms": 0.0,
        "generator_cpu_seconds": 0.0,
        "start_skew_ms": 0.0,
        "client_partial_sends": 0,
        "client_send_eagain": 0,
    }


def normalize_state(state):
    normalized = empty_state(state.get("worker_id", 0))
    normalized.update(state)
    aliases = {
        "offered": "offered_operations",
        "completed": "eventual_completions",
        "completed_by_deadline": "completed_by_deadline",
    }
    for old, new in aliases.items():
        if old in state and new not in state:
            normalized[new] = state[old]
    return normalized


def merge_states(states):
    ordered = [normalize_state(state) for state in sorted(states, key=lambda item: item.get("worker_id", 0))]
    merged = empty_state()
    for key in (
        "offered_operations", "sent_operations", "missed_offers",
        "completed_by_deadline", "eventual_completions", "deliveries",
        "deliveries_by_deadline", "send_errors", "generator_cpu_seconds",
        "client_partial_sends", "client_send_eagain",
    ):
        merged[key] = sum(state[key] for state in ordered)
    merged["latencies_ms"] = [value for state in ordered for value in state["latencies_ms"]]
    merged["scheduling_lag_ms"] = [
        value for state in ordered for value in state["scheduling_lag_ms"]
    ]
    merged["disconnects"] = set().union(*(state["disconnects"] for state in ordered))
    merged["max_read_service_gap_ms"] = max(
        (state["max_read_service_gap_ms"] for state in ordered), default=0.0
    )
    merged["start_skew_ms"] = max((abs(state["start_skew_ms"]) for state in ordered), default=0.0)
    merged["offered"] = merged["offered_operations"]
    merged["completed"] = merged["eventual_completions"]
    return merged


def is_disconnect_log(line):
    return line.startswith("disconnect ") or " event=client.disconnected " in line


def server_cpu_seconds(pid):
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        fields = stat[stat.rfind(")") + 2:].split()
        ticks = int(fields[11]) + int(fields[12])
        return ticks / os.sysconf("SC_CLK_TCK")
    except (FileNotFoundError, IndexError, OSError, ValueError):
        return None


def new_client(index, port, active):
    deadline = time.monotonic() + 15
    while True:
        sock = socket.socket()
        sock.settimeout(1)
        try:
            sock.connect((HOST, port))
            break
        except OSError:
            sock.close()
            if time.monotonic() >= deadline:
                raise RuntimeError(f"client {index} could not connect")
            time.sleep(0.005)
    receive_until(sock, PROMPT)
    if active:
        sock.sendall(f"bench-{index}\n".encode())
    sock.setblocking(False)
    return {
        "index": index,
        "sock": sock,
        "active": active,
        "buffer": b"",
        "pending": deque(),
        "send_buffer": bytearray(),
        "closed": False,
        "last_read_service_ns": 0,
    }


def update_interest(selector, client):
    events = selectors.EVENT_READ
    if client["send_buffer"]:
        events |= selectors.EVENT_WRITE
    selector.modify(client["sock"], events, client)


def flush_client_send(selector, client, state):
    while client["send_buffer"]:
        before = len(client["send_buffer"])
        try:
            sent = client["sock"].send(client["send_buffer"])
        except BlockingIOError:
            state["client_send_eagain"] += 1
            break
        except OSError:
            state["send_errors"] += 1
            client["send_buffer"].clear()
            break
        if sent <= 0:
            state["send_errors"] += 1
            client["send_buffer"].clear()
            break
        if sent < before:
            state["client_partial_sends"] += 1
        del client["send_buffer"][:sent]
    update_interest(selector, client)


def queue_client_frame(selector, client, frame, sent_ns, workload, state):
    if len(client["send_buffer"]) + len(frame) > MAX_CLIENT_SEND_QUEUE:
        return False
    client["send_buffer"].extend(frame)
    if workload == "direct":
        client["pending"].append(sent_ns)
    state["sent_operations"] += 1
    flush_client_send(selector, client, state)
    return True


def close_client(selector, client, state):
    if client["closed"]:
        return
    client["closed"] = True
    state["disconnects"].add(client["index"])
    try:
        selector.unregister(client["sock"])
    except (KeyError, ValueError):
        pass


def process_client_read(selector, client, state, phase, now_ns):
    if client["last_read_service_ns"]:
        state["max_read_service_gap_ms"] = max(
            state["max_read_service_gap_ms"],
            (now_ns - client["last_read_service_ns"]) / 1_000_000,
        )
    client["last_read_service_ns"] = now_ns
    calls = 0
    bytes_read = 0
    while calls < phase["max_recv_calls"] and bytes_read < phase["max_recv_bytes"]:
        try:
            data = client["sock"].recv(min(16384, phase["max_recv_bytes"] - bytes_read))
        except BlockingIOError:
            break
        except OSError:
            data = b""
        calls += 1
        if not data:
            close_client(selector, client, state)
            return
        bytes_read += len(data)
        client["buffer"] += data
        while b"\n" in client["buffer"]:
            line, client["buffer"] = client["buffer"].split(b"\n", 1)
            received_ns = time.monotonic_ns()
            by_deadline = received_ns <= phase["deadline_ns"]
            if phase["workload"] == "direct":
                if WHERE_RESPONSE not in line or not client["pending"]:
                    continue
                sent_ns = client["pending"].popleft()
                state["eventual_completions"] += 1
                state["deliveries"] += 1
                if by_deadline:
                    state["completed_by_deadline"] += 1
                    state["deliveries_by_deadline"] += 1
                state["latencies_ms"].append((received_ns - sent_ns) / 1_000_000)
                continue

            marker = line.find(b"BENCH|")
            if marker == -1:
                continue
            fields = line[marker:].split(b"|", 5)
            if len(fields) != 6 or fields[1] != phase["token"]:
                continue
            try:
                sender = int(fields[2])
                sent_ns = int(fields[4])
            except ValueError:
                continue
            state["deliveries"] += 1
            if by_deadline:
                state["deliveries_by_deadline"] += 1
            if client["index"] == (sender + 1) % phase["total_connections"]:
                state["eventual_completions"] += 1
                if by_deadline:
                    state["completed_by_deadline"] += 1
                state["latencies_ms"].append((received_ns - sent_ns) / 1_000_000)


def service_events(selector, clients, state, phase, timeout, cursor):
    events, cursor = rotate_ready(selector.select(timeout), cursor)
    for key, mask in events:
        client = key.data
        if client["closed"]:
            continue
        now_ns = time.monotonic_ns()
        if mask & selectors.EVENT_READ:
            process_client_read(selector, client, state, phase, now_ns)
        if mask & selectors.EVENT_WRITE and not client["closed"]:
            flush_client_send(selector, client, state)
    return cursor


def settle_clients(selector, clients, duration=0.1):
    deadline = time.monotonic() + duration
    phase = {
        "workload": "setup", "token": b"", "deadline_ns": 0,
        "total_connections": len(clients), "max_recv_calls": MAX_RECV_CALLS,
        "max_recv_bytes": MAX_RECV_BYTES,
    }
    state = empty_state()
    cursor = 0
    while time.monotonic() < deadline:
        cursor = service_events(selector, clients, state, phase, 0.01, cursor)
    if state["disconnects"]:
        raise RuntimeError(f"disconnects during setup: {sorted(state['disconnects'])}")
    for client in clients:
        client["buffer"] = b""
        client["pending"].clear()


def schedule_for_clients(clients, phase):
    active_positions = {index: position for position, index in enumerate(phase["active_indices"])}
    active_count = len(active_positions)
    target_offers = int(phase["rate"] * phase["duration"])
    schedule = []
    if target_offers == 0:
        return schedule, target_offers
    period_ns = 1_000_000_000 / phase["rate"]
    for client in clients:
        position = active_positions.get(client["index"])
        if position is None or position >= target_offers:
            continue
        heapq.heappush(
            schedule,
            (int(phase["start_ns"] + position * period_ns), position, client),
        )
    return schedule, target_offers


def run_phase(selector, clients, phase, worker_id):
    state = empty_state(worker_id)
    cursor = 0
    while time.monotonic_ns() < phase["start_ns"]:
        remaining = (phase["start_ns"] - time.monotonic_ns()) / 1_000_000_000
        cursor = service_events(
            selector, clients, state, phase, max(0.0, min(0.001, remaining)), cursor
        )
    actual_start_ns = time.monotonic_ns()
    state["start_skew_ms"] = (actual_start_ns - phase["start_ns"]) / 1_000_000
    for client in clients:
        client["last_read_service_ns"] = actual_start_ns
    cpu_start = time.process_time()
    schedule, target_offers = schedule_for_clients(clients, phase)
    active_count = len(phase["active_indices"])
    period_per_client_ns = int(1_000_000_000 * active_count / phase["rate"])
    filler = b"x" * phase["payload_size"]
    while time.monotonic_ns() < phase["deadline_ns"]:
        now_ns = time.monotonic_ns()
        for scheduled_ns, missed, sequence, client in due_offers(
            schedule, now_ns, phase["max_overdue_sends"],
            int(phase["max_offer_lag_ms"] * 1_000_000),
        ):
            state["offered_operations"] += 1
            lag_ms = max(0.0, (now_ns - scheduled_ns) / 1_000_000)
            state["scheduling_lag_ms"].append(lag_ms)
            next_sequence = sequence + active_count
            if next_sequence < target_offers:
                heapq.heappush(
                    schedule,
                    (scheduled_ns + period_per_client_ns, next_sequence, client),
                )
            if missed or client["closed"]:
                state["missed_offers"] += 1
                continue
            sent_ns = time.monotonic_ns()
            frame = b"WHERE\n" if phase["workload"] == "direct" else (
                b"BENCH|" + phase["token"] +
                f"|{client['index']}|{sequence}|{sent_ns}|".encode() + filler + b"\n"
            )
            if not queue_client_frame(
                selector, client, frame, sent_ns, phase["workload"], state
            ):
                state["missed_offers"] += 1

        now_ns = time.monotonic_ns()
        next_ns = schedule[0][0] if schedule else phase["deadline_ns"]
        timeout = max(0.0, min(0.01, (next_ns - now_ns) / 1_000_000_000,
                               (phase["deadline_ns"] - now_ns) / 1_000_000_000))
        cursor = service_events(selector, clients, state, phase, timeout, cursor)

    for scheduled_ns, sequence, _client in schedule:
        if scheduled_ns >= phase["deadline_ns"]:
            continue
        remaining = 1 + (target_offers - 1 - sequence) // active_count
        state["offered_operations"] += remaining
        state["missed_offers"] += remaining
    state["generator_cpu_seconds"] = time.process_time() - cpu_start

    drain_deadline = time.monotonic() + phase["drain"]
    while time.monotonic() < drain_deadline:
        before = state["deliveries"]
        cursor = service_events(selector, clients, state, phase, 0.02, cursor)
        if state["deliveries"] == before and all(
            not client["pending"] and not client["send_buffer"] for client in clients
        ):
            break
    return state


def worker_main(worker_id, indices, active_indices, port, control):
    selector = selectors.DefaultSelector()
    clients = []
    try:
        active = set(active_indices)
        for index in indices:
            client = new_client(index, port, index in active)
            selector.register(client["sock"], selectors.EVENT_READ, client)
            clients.append(client)
            if len(clients) % 16 == 0:
                settle_clients(selector, clients, 0.001)
        control.send({"type": "connected", "worker_id": worker_id})
        while True:
            if control.poll(0.01):
                command = control.recv()
                break
            settle_clients(selector, clients, 0.005)
        if command["type"] != "setup_complete":
            raise RuntimeError("invalid setup command")
        settle_clients(selector, clients)
        control.send({"type": "ready", "worker_id": worker_id})

        while True:
            command = control.recv()
            if command["type"] == "close":
                return
            if command["type"] != "phase":
                raise RuntimeError("invalid worker command")
            phase = command["phase"]
            phase["token"] = phase["token"].encode()
            state = run_phase(selector, clients, phase, worker_id)
            control.send({"type": "result", "state": state})
            for client in clients:
                client["buffer"] = b""
                client["pending"].clear()
    except BaseException as error:
        try:
            control.send({"type": "error", "worker_id": worker_id, "error": repr(error)})
        except (BrokenPipeError, OSError):
            pass
    finally:
        for client in clients:
            client["sock"].close()
        selector.close()
        control.close()


def receive_worker_messages(controls, expected_type, timeout):
    deadline = time.monotonic() + timeout
    messages = []
    for control in controls:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not control.poll(remaining):
            raise RuntimeError(f"worker timed out waiting for {expected_type}")
        message = control.recv()
        if message["type"] == "error":
            raise RuntimeError(f"worker {message['worker_id']}: {message['error']}")
        if message["type"] != expected_type:
            raise RuntimeError(f"expected {expected_type}, received {message['type']}")
        messages.append(message)
    return messages


def wait_for_metrics(path, count, timeout=3):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            lines = path.read_text().splitlines()
            if len(lines) >= count:
                return [json.loads(line) for line in lines]
        time.sleep(0.005)
    return []


def phase_config(args, workload, total_connections, active_connections, duration, token):
    start_ns = time.monotonic_ns() + int(START_DELAY_S * 1_000_000_000)
    return {
        "workload": workload,
        "total_connections": total_connections,
        "active_indices": list(range(active_connections)),
        "rate": args.rate,
        "duration": duration,
        "drain": args.drain,
        "payload_size": args.payload_size if workload == "broadcast" else 0,
        "token": token,
        "start_ns": start_ns,
        "deadline_ns": start_ns + int(duration * 1_000_000_000),
        "max_overdue_sends": args.max_overdue_sends,
        "max_offer_lag_ms": args.max_offer_lag_ms,
        "max_recv_calls": args.max_recv_calls,
        "max_recv_bytes": args.max_recv_bytes,
    }


def run_worker_phase(controls, phase):
    for control in controls:
        control.send({"type": "phase", "phase": phase})
    timeout = START_DELAY_S + phase["duration"] + phase["drain"] + 10
    return [message["state"] for message in receive_worker_messages(controls, "result", timeout)]


def run_trial(server_path, label, workload, total_connections, active_connections, run, args):
    port = free_port()
    environment = os.environ.copy()
    environment.update(CHAT_SERVER_PORT=str(port), CHAT_LOG_LEVEL="off")
    log = tempfile.TemporaryFile(mode="w+t")
    metrics_temp = tempfile.TemporaryDirectory()
    metrics_path = Path(metrics_temp.name) / "server-metrics.jsonl"
    environment["CHAT_METRICS_FILE"] = str(metrics_path)
    server = subprocess.Popen(
        [str(server_path)], cwd=ROOT, env=environment,
        stdout=subprocess.DEVNULL, stderr=log,
    )
    worker_count = min(total_connections, args.workers)
    partitions = partition_clients(list(range(total_connections)), worker_count)
    context = multiprocessing.get_context("spawn")
    processes = []
    controls = []
    server_metrics = None
    state = empty_state()
    cpu_start = cpu_end = None
    server_crashed = False

    try:
        for worker_id, indices in enumerate(partitions):
            parent, child = context.Pipe()
            process = context.Process(
                target=worker_main,
                args=(worker_id, indices, list(range(active_connections)), port, child),
            )
            process.start()
            child.close()
            controls.append(parent)
            processes.append(process)
        receive_worker_messages(controls, "connected", max(20, total_connections / 100))
        for control in controls:
            control.send({"type": "setup_complete"})
        receive_worker_messages(controls, "ready", 10)

        if args.warmup:
            warmup = phase_config(
                args, workload, total_connections, active_connections,
                args.warmup, f"warmup-{run}",
            )
            warmup_state = merge_states(run_worker_phase(controls, warmup))
            if warmup_state["disconnects"] or warmup_state["send_errors"]:
                raise RuntimeError(
                    "client failure during warmup: "
                    f"disconnects={sorted(warmup_state['disconnects'])} "
                    f"send_errors={warmup_state['send_errors']}"
                )

        server.send_signal(signal.SIGUSR1)
        wait_for_metrics(metrics_path, 1)
        phase = phase_config(
            args, workload, total_connections, active_connections,
            args.duration, f"{total_connections}-{active_connections}-{run}",
        )
        for control in controls:
            control.send({"type": "phase", "phase": phase})
        while time.monotonic_ns() < phase["start_ns"]:
            time.sleep(0.001)
        cpu_start = server_cpu_seconds(server.pid)
        while time.monotonic_ns() < phase["deadline_ns"]:
            if server.poll() is not None:
                server_crashed = True
                break
            time.sleep(0.002)
        cpu_end = server_cpu_seconds(server.pid)
        if server.poll() is None:
            server.send_signal(signal.SIGUSR2)
            records = wait_for_metrics(metrics_path, 2)
            if len(records) >= 2:
                server_metrics = records[-1]
        timeout = args.drain + 10
        states = [message["state"] for message in receive_worker_messages(controls, "result", timeout)]
        state = merge_states(states)
    finally:
        for control in controls:
            try:
                control.send({"type": "close"})
            except (BrokenPipeError, EOFError, OSError):
                pass
        for process in processes:
            process.join(timeout=2)
            if process.is_alive():
                process.terminate()
                process.join()
        for control in controls:
            control.close()
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=2)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()

    log.seek(0)
    disconnect_reasons = [line.strip() for line in log if is_disconnect_log(line)]
    log.close()
    metrics_temp.cleanup()
    expected_deliveries = state["sent_operations"] * (
        total_connections - 1 if workload == "broadcast" else 1
    )
    latency = state["latencies_ms"]
    server_cpu_percent = (
        (cpu_end - cpu_start) / args.duration * 100
        if cpu_start is not None and cpu_end is not None else None
    )
    result = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "label": label,
        "server": str(server_path),
        "workload": workload,
        "total_connections": total_connections,
        "active_connections": active_connections,
        "clients": total_connections,
        "load_workers": worker_count,
        "run": run,
        "duration_s": args.duration,
        "global_offered_rate_s": args.rate,
        "payload_bytes": args.payload_size if workload == "broadcast" else 0,
        "offered_operations": state["offered_operations"],
        "sent_operations": state["sent_operations"],
        "missed_offers": state["missed_offers"],
        "offered_msg_s": state["sent_operations"] / args.duration,
        "completed_by_deadline": state["completed_by_deadline"],
        "eventual_completions": state["eventual_completions"],
        "completed_by_deadline_msg_s": state["completed_by_deadline"] / args.duration,
        "delivered": state["deliveries"],
        "delivered_by_deadline_msg_s": state["deliveries_by_deadline"] / args.duration,
        "delivery_ratio": state["deliveries"] / expected_deliveries if expected_deliveries else 1.0,
        "latency_samples": len(latency),
        "p50_ms": percentile(latency, 0.50),
        "p95_ms": percentile(latency, 0.95),
        "p99_ms": percentile(latency, 0.99),
        "generator_cpu_percent": state["generator_cpu_seconds"] / args.duration * 100,
        "server_cpu_percent": server_cpu_percent,
        "scheduling_lag_p50_ms": percentile(state["scheduling_lag_ms"], 0.50),
        "scheduling_lag_p95_ms": percentile(state["scheduling_lag_ms"], 0.95),
        "scheduling_lag_p99_ms": percentile(state["scheduling_lag_ms"], 0.99),
        "max_read_service_gap_ms": state["max_read_service_gap_ms"],
        "worker_start_skew_ms": state["start_skew_ms"],
        "client_disconnects": len(state["disconnects"]),
        "send_errors": state["send_errors"],
        "client_partial_sends": state["client_partial_sends"],
        "client_send_eagain": state["client_send_eagain"],
        "server_crashed": server_crashed,
        "disconnect_reasons": " | ".join(disconnect_reasons),
    }
    for key, value in (server_metrics or {}).items():
        if key != "status":
            result[key] = value
    return result


def display(value, unit=""):
    return "n/a" if value is None else f"{value:.2f}{unit}"


def print_result(row):
    print(
        f"{row['workload']:9} total={row['total_connections']:5} "
        f"active={row['active_connections']:5} run={row['run']} workers={row['load_workers']:2} "
        f"offered={row['offered_msg_s']:.1f}/s missed={row['missed_offers']} "
        f"completed={row['completed_by_deadline_msg_s']:.1f}/s "
        f"ratio={row['delivery_ratio']:.4f} p50={display(row['p50_ms'], 'ms')} "
        f"p95={display(row['p95_ms'], 'ms')} cpu={display(row['server_cpu_percent'], '%')} "
        f"generator_cpu={display(row['generator_cpu_percent'], '%')} "
        f"lag_p95={display(row['scheduling_lag_p95_ms'], 'ms')} "
        f"disconnects={row['client_disconnects']} send_errors={row['send_errors']}",
        flush=True,
    )


def print_summary(rows):
    print("\nmedian across runs")
    for key in sorted({(row["workload"], row["total_connections"], row["active_connections"]) for row in rows}):
        workload, total, active = key
        group = [row for row in rows if (
            row["workload"], row["total_connections"], row["active_connections"]
        ) == key]
        print(
            f"{workload:9} total={total:5} active={active:5} "
            f"offered={statistics.median(row['offered_msg_s'] for row in group):.1f}/s "
            f"p50={display(statistics.median(row['p50_ms'] for row in group if row['p50_ms'] is not None), 'ms')} "
            f"p95={display(statistics.median(row['p95_ms'] for row in group if row['p95_ms'] is not None), 'ms')}"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--tiers", type=lambda value: csv_values(value, int), default=[100])
    parser.add_argument("--active-connections", type=int)
    parser.add_argument("--workloads", type=csv_values, default=["direct"])
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--duration", type=float, default=5.0)
    parser.add_argument("--warmup", type=float, default=1.0)
    parser.add_argument("--drain", type=float, default=2.0)
    parser.add_argument("--rate", type=float, default=1000.0, help="fixed global offered operations/s")
    parser.add_argument("--payload-size", type=int, default=128)
    parser.add_argument("--workers", type=int, default=max(1, min(4, os.cpu_count() or 1)))
    parser.add_argument("--max-overdue-sends", type=int, default=MAX_OVERDUE_SENDS)
    parser.add_argument("--max-offer-lag-ms", type=float, default=20.0)
    parser.add_argument("--max-recv-calls", type=int, default=MAX_RECV_CALLS)
    parser.add_argument("--max-recv-bytes", type=int, default=MAX_RECV_BYTES)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    server_path = Path(args.server).resolve()
    if not server_path.is_file() or not os.access(server_path, os.X_OK):
        parser.error(f"server is not executable: {server_path}")
    if any(tier < 1 for tier in args.tiers) or args.runs < 1:
        parser.error("tiers and runs must be positive")
    if any(workload not in {"direct", "broadcast"} for workload in args.workloads):
        parser.error("workloads must be direct and/or broadcast")
    if min(args.duration, args.rate, args.drain) <= 0 or args.warmup < 0:
        parser.error("duration, rate, and drain must be positive; warmup cannot be negative")
    if args.workers < 1 or args.max_overdue_sends < 1 or args.max_recv_calls < 1:
        parser.error("worker and fairness limits must be positive")
    if not 0 <= args.payload_size <= 4000:
        parser.error("payload size is out of range")

    print(
        f"server={server_path} label={args.label} system={platform.system()} {platform.release()} "
        f"python={platform.python_version()} selector={selectors.DefaultSelector.__name__} "
        f"worker_processes={args.workers} global_rate={args.rate}/s",
        flush=True,
    )
    rows = []
    for workload in args.workloads:
        for total in args.tiers:
            active = args.active_connections if args.active_connections is not None else total
            if active < 1 or active > total:
                parser.error("active connections must be between one and total connections")
            if workload == "broadcast" and total < 2:
                parser.error("broadcast needs at least two total connections")
            for run in range(1, args.runs + 1):
                row = run_trial(server_path, args.label, workload, total, active, run, args)
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
