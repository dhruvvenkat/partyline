import json
import os
import signal
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SERVERS = {"poll": ROOT / "poll(2)/server", "epoll": ROOT / "epoll/server"}
PROMPT = b"enter your username: "


def receive_until(sock, marker, timeout=3):
    deadline = time.monotonic() + timeout
    received = b""
    while marker not in received:
        sock.settimeout(max(0.01, deadline - time.monotonic()))
        chunk = sock.recv(65536)
        if not chunk:
            raise AssertionError(f"connection closed before {marker!r}")
        received += chunk
    return received


def drain(sock):
    sock.settimeout(0.03)
    received = b""
    while True:
        try:
            chunk = sock.recv(65536)
        except socket.timeout:
            return received
        if not chunk:
            return received
        received += chunk


class RunningServer:
    def __init__(self, binary, metrics=None):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            self.port = probe.getsockname()[1]
        environment = os.environ.copy()
        environment.update(CHAT_LOG_LEVEL="off", CHAT_SERVER_PORT=str(self.port))
        if metrics is not None:
            environment["CHAT_METRICS_FILE"] = str(metrics)
        self.process = subprocess.Popen(
            [str(binary)], cwd=ROOT, env=environment,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        self.clients = []

    def connect(self, username, receive_buffer=None):
        deadline = time.monotonic() + 2
        while True:
            client = socket.socket()
            if receive_buffer is not None:
                client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
            try:
                client.settimeout(0.2)
                client.connect(("127.0.0.1", self.port))
                break
            except OSError:
                client.close()
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)
        prompt = receive_until(client, PROMPT)
        client.sendall(username.encode() + b"\n")
        receive_until(client, b"joined room main-room")
        self.clients.append(client)
        return client, prompt

    def close(self):
        for client in self.clients:
            client.close()
        self.process.terminate()
        self.process.wait(timeout=3)


def protocol_scenario(binary):
    server = RunningServer(binary)
    try:
        alice, prompt = server.connect("alice")
        bob, _ = server.connect("bob")
        alice_join_notice = receive_until(alice, b"bob joined room main-room")

        alice.sendall(b"WHERE\n")
        where = receive_until(alice, b"you are in room main-room")
        alice.sendall(b"LIST\n")
        room_list = receive_until(alice, b"main-room")

        alice.sendall(b"JOIN blue\n")
        alice_join = receive_until(alice, b"alice joined room blue")
        bob_left_notice = receive_until(bob, b"alice left room main-room")
        drain(alice)
        bob.sendall(b"main-only\n")
        alice.settimeout(0.1)
        isolated = False
        try:
            isolated = alice.recv(1) == b""
        except socket.timeout:
            isolated = True

        bob.sendall(b"JOIN blue\n")
        receive_until(bob, b"bob joined room blue")
        bob_join_notice = receive_until(alice, b"bob joined room blue")
        drain(alice)
        bob.sendall(b"frag")
        alice.settimeout(0.08)
        try:
            premature_fragment = bool(alice.recv(65536))
        except socket.timeout:
            premature_fragment = False
        bob.sendall(b"mented\n")
        fragmented = receive_until(alice, b"> bob: fragmented \n")

        carol, _ = server.connect("carol")
        drain(alice)
        carol.sendall(b"JOIN blue\n")
        receive_until(carol, b"carol joined room blue")
        receive_until(alice, b"carol joined room blue")
        carol.close()
        disconnected = receive_until(alice, b"carol disconnected")

        bob.sendall(b"LEAVE\n")
        left = receive_until(alice, b"bob left room blue")
        receive_until(bob, b"bob joined room main-room")
        bob.sendall(b"QUIT\n")
        bob.settimeout(2)
        quit_closed = bob.recv(1) == b""

        return {
            "prompt": PROMPT in prompt,
            "connection_notice": b"bob joined room main-room" in alice_join_notice,
            "where": b"you are in room main-room" in where,
            "list": b"main-room" in room_list,
            "join": b"alice joined room blue" in alice_join,
            "leave_notice": b"alice left room main-room" in bob_left_notice,
            "rooms_isolated": isolated,
            "join_notice": b"bob joined room blue" in bob_join_notice,
            "fragment_waited": not premature_fragment,
            "fragment": b"> bob: fragmented \n" in fragmented,
            "disconnect": b"carol disconnected" in disconnected,
            "leave": b"bob left room blue" in left,
            "quit": quit_closed,
        }
    finally:
        server.close()


def wait_for_metric_lines(path, count):
    deadline = time.monotonic() + 4
    while time.monotonic() < deadline:
        if path.exists() and len(path.read_text().splitlines()) >= count:
            return [json.loads(line) for line in path.read_text().splitlines()]
        time.sleep(0.01)
    raise AssertionError("metrics were not emitted")


def backpressure_scenario(binary):
    with tempfile.TemporaryDirectory() as temp:
        metrics = Path(temp) / "metrics.jsonl"
        server = RunningServer(binary, metrics)
        try:
            slow, _ = server.connect("slow", receive_buffer=1024)
            sender, _ = server.connect("sender")
            drain(sender)
            server.process.send_signal(signal.SIGUSR1)
            wait_for_metric_lines(metrics, 1)
            sender.settimeout(10)
            # Keep each 32-frame read batch below the 8 KiB write budget so
            # overflow reflects a genuinely blocked receiver, not one burst.
            sender.sendall((b"x" * 200 + b"\n") * 20000)
            sender.sendall(b"WHERE\n")
            sender_alive = b"you are in room main-room" in receive_until(
                sender, b"you are in room main-room", timeout=10
            )
            disconnect_deadline = time.monotonic() + 5
            slow_disconnected = False
            while time.monotonic() < disconnect_deadline:
                slow.settimeout(0.2)
                try:
                    if slow.recv(65536) == b"":
                        slow_disconnected = True
                        break
                except (ConnectionResetError, OSError):
                    slow_disconnected = True
                    break
                except socket.timeout:
                    pass
            server.process.send_signal(signal.SIGUSR2)
            snapshot = wait_for_metric_lines(metrics, 2)[1]
            return {
                "queue_overflow": snapshot["queue_overflows"] > 0,
                "partial_or_blocked_send": (
                    snapshot["partial_writes"] > 0 or snapshot["send_eagain"] > 0
                ),
                "queue_reached_limit": snapshot["queue_high_water_bytes"] > 12000,
                "sender_alive": sender_alive,
                "slow_client_disconnected": slow_disconnected,
            }
        finally:
            server.close()


class ServerParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(["make", "release"], cwd=ROOT, check=True)

    def test_protocol_and_room_behavior_match(self):
        results = {name: protocol_scenario(binary) for name, binary in SERVERS.items()}
        self.assertEqual(results["poll"], results["epoll"])
        self.assertTrue(all(results["poll"].values()), results)

    def test_slow_client_and_queue_overflow_behavior_match(self):
        results = {name: backpressure_scenario(binary) for name, binary in SERVERS.items()}
        self.assertEqual(results["poll"], results["epoll"])
        self.assertTrue(all(results["poll"].values()), results)


if __name__ == "__main__":
    unittest.main()
