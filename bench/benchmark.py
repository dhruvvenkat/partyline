#!/usr/bin/env python3
import argparse
import os
import socket
import statistics
import subprocess
import threading
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "server"
HOST = "127.0.0.1"
USERNAME_PROMPT = b"enter your username: "


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((HOST, 0))
        return sock.getsockname()[1]


def receive_until(sock, marker, timeout=2.0):
    deadline = time.monotonic() + timeout
    received = b""

    while marker not in received:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(f"timed out waiting for {marker!r}")

        sock.settimeout(remaining)
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError("server closed the connection during setup")
        received += chunk


def connect_client(port, username):
    deadline = time.monotonic() + 2.0

    while time.monotonic() < deadline:
        try:
            sock = socket.create_connection((HOST, port), timeout=0.2)
            receive_until(sock, USERNAME_PROMPT)
            sock.sendall((username + "\n").encode())
            sock.settimeout(0.1)
            return sock
        except (ConnectionRefusedError, socket.timeout, OSError):
            time.sleep(0.02)

    raise RuntimeError("server did not accept connections")


def percentile(samples, fraction):
    ordered = sorted(samples)
    index = min(len(ordered) - 1, int(len(ordered) * fraction))
    return ordered[index]


def run_healthy_benchmark(client_count, sender_count, messages_per_second, duration, payload_size):
    if sender_count >= client_count:
        raise ValueError("sender count must leave at least one observer client")

    port = find_free_port()
    environment = os.environ.copy()
    environment["CHAT_SERVER_PORT"] = str(port)
    print(f"starting server on port {port}", flush=True)
    server = subprocess.Popen(
        [str(SERVER)],
        cwd=ROOT,
        env=environment,
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    clients = []
    stop_readers = threading.Event()
    stop_senders = threading.Event()
    latencies_ms = []
    disconnects = []
    server_disconnects = []
    sent_counts = [0] * sender_count
    lock = threading.Lock()
    collect_server_events = threading.Event()

    def read_server_events():
        for line in server.stderr:
            line = line.rstrip()
            if not line or not collect_server_events.is_set():
                continue
            print(f"server: {line}", flush=True)
            if line.startswith("disconnect "):
                with lock:
                    server_disconnects.append(line)

    server_log_reader = threading.Thread(target=read_server_events, daemon=True)
    server_log_reader.start()

    try:
        collect_server_events.set()
        connection_interval = max(1, client_count // 10)
        for index in range(client_count):
            clients.append(connect_client(port, f"bench-{index}"))
            if (index + 1) % connection_interval == 0 or index + 1 == client_count:
                print(f"connected {index + 1}/{client_count} clients", flush=True)

        observer = clients[-1]

        def read_client(index, sock):
            received = b""
            while not stop_readers.is_set():
                try:
                    chunk = sock.recv(4096)
                except socket.timeout:
                    continue
                except OSError:
                    return

                if not chunk:
                    with lock:
                        disconnects.append(index)
                    return

                received += chunk
                while b"\n" in received:
                    line, received = received.split(b"\n", 1)
                    marker = line.find(b"BENCH|")
                    if index != client_count - 1 or marker == -1:
                        continue

                    fields = line[marker:].split(b"|", 4)
                    if len(fields) < 4:
                        continue

                    try:
                        sent_at_ns = int(fields[3])
                    except ValueError:
                        continue

                    with lock:
                        latencies_ms.append((time.monotonic_ns() - sent_at_ns) / 1_000_000)

        readers = [threading.Thread(target=read_client, args=(index, sock), daemon=True) for index, sock in enumerate(clients)]
        for reader in readers:
            reader.start()

        start = time.monotonic()
        end = start + duration

        def send_messages(index):
            next_send = start
            sequence = 0
            filler = "x" * payload_size

            while not stop_senders.is_set() and next_send < end:
                remaining = next_send - time.monotonic()
                if remaining > 0:
                    time.sleep(remaining)

                message = f"BENCH|{index}|{sequence}|{time.monotonic_ns()}|{filler}\n"
                try:
                    clients[index].sendall(message.encode())
                except OSError:
                    return

                with lock:
                    sent_counts[index] += 1
                sequence += 1
                next_send += 1 / messages_per_second

        senders = [threading.Thread(target=send_messages, args=(index,), daemon=True) for index in range(sender_count)]
        for sender in senders:
            sender.start()

        print(f"measuring healthy workload for {duration:.0f}s", flush=True)
        next_progress = start + 1
        while time.monotonic() < end:
            time.sleep(min(0.1, max(0, end - time.monotonic())))
            if time.monotonic() < next_progress:
                continue

            with lock:
                sent_so_far = sum(sent_counts)
                received_so_far = len(latencies_ms)
                disconnected_so_far = list(disconnects)
                server_disconnects_so_far = list(server_disconnects)
            elapsed_so_far = time.monotonic() - start
            print(
                f"progress {elapsed_so_far:.0f}/{duration:.0f}s "
                f"sent={sent_so_far} observer_received={received_so_far} "
                f"disconnected={disconnected_so_far} "
                f"server_disconnects={len(server_disconnects_so_far)}",
                flush=True,
            )
            next_progress += 1

        for sender in senders:
            sender.join(timeout=1)

        stop_senders.set()
        print("draining final messages", flush=True)
        time.sleep(0.25)
        elapsed = end - start
        stop_readers.set()
        for reader in readers:
            reader.join(timeout=1)

        with lock:
            samples = list(latencies_ms)
            disconnected_clients = list(disconnects)
            disconnect_reasons = list(server_disconnects)

        sent = sum(sent_counts)
        received = len(samples)
        print("healthy benchmark")
        print(f"clients={client_count} senders={sender_count} duration={elapsed:.2f}s")
        print(f"sent={sent} observer_received={received} throughput={received / elapsed:.2f} messages/s")
        if samples:
            print(f"p50_latency={statistics.median(samples):.2f}ms p95_latency={percentile(samples, 0.95):.2f}ms")
        else:
            print("p50_latency=n/a p95_latency=n/a")
        print(f"disconnected_clients={disconnected_clients}")
        if disconnect_reasons:
            print("disconnect_reasons:")
            for reason in disconnect_reasons:
                print(f"  {reason}")
        else:
            print("disconnect_reasons: none")
    finally:
        stop_senders.set()
        stop_readers.set()
        collect_server_events.clear()
        for client in clients:
            client.close()
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=2)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
        server_log_reader.join(timeout=1)


def main():
    parser = argparse.ArgumentParser(description="Healthy-client benchmark for the chat server")
    parser.add_argument("--clients", type=int, default=8)
    parser.add_argument("--senders", type=int, default=2)
    parser.add_argument("--messages-per-second", type=float, default=3.0)
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--payload-size", type=int, default=128)
    args = parser.parse_args()

    run_healthy_benchmark(
        args.clients,
        args.senders,
        args.messages_per_second,
        args.duration,
        args.payload_size,
    )


if __name__ == "__main__":
    main()
