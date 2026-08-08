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
CLIENT_COUNT = 192


def receive_until(sock, marker, timeout=3):
    deadline = time.monotonic() + timeout
    received = b""
    while marker not in received:
        sock.settimeout(max(0.01, deadline - time.monotonic()))
        chunk = sock.recv(65536)
        if not chunk:
            raise AssertionError("server disconnected a ready descriptor")
        received += chunk
    return received


def wait_for_metrics(path, count):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if path.exists() and len(path.read_text().splitlines()) >= count:
            return [json.loads(line) for line in path.read_text().splitlines()]
        time.sleep(0.01)
    raise AssertionError("server did not emit metrics")


def connect_with_retry(port):
    deadline = time.monotonic() + 3
    while True:
        try:
            return socket.create_connection(("127.0.0.1", port), timeout=0.2)
        except (ConnectionRefusedError, socket.timeout):
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.01)


class EpollEventCapacityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(["make", "-C", "epoll", "server"], cwd=ROOT, check=True)

    def test_more_than_64_ready_sockets_are_returned_and_serviced(self):
        with tempfile.TemporaryDirectory() as temp:
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                port = probe.getsockname()[1]
            metrics_path = Path(temp) / "metrics.jsonl"
            environment = os.environ.copy()
            environment.update(
                CHAT_LOG_LEVEL="off",
                CHAT_METRICS_FILE=str(metrics_path),
                CHAT_SERVER_PORT=str(port),
            )
            server = subprocess.Popen(
                [str(ROOT / "epoll/server")],
                cwd=ROOT,
                env=environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            clients = []
            stopped = False
            try:
                for index in range(CLIENT_COUNT):
                    client = connect_with_retry(port)
                    receive_until(client, b"enter your username: ")
                    client.sendall(f"ready-{index}\n".encode())
                    clients.append(client)

                time.sleep(0.1)
                for client in clients:
                    client.setblocking(False)
                    try:
                        while client.recv(65536):
                            pass
                    except BlockingIOError:
                        pass
                    client.setblocking(True)

                server.send_signal(signal.SIGUSR1)
                wait_for_metrics(metrics_path, 1)
                server.send_signal(signal.SIGSTOP)
                stopped = True
                for client in clients:
                    client.sendall(b"WHERE\n")
                server.send_signal(signal.SIGCONT)
                stopped = False

                for client in clients:
                    self.assertIn(
                        b"you are in room main-room",
                        receive_until(client, b"you are in room main-room"),
                    )

                server.send_signal(signal.SIGUSR2)
                snapshot = wait_for_metrics(metrics_path, 2)[1]
                self.assertGreater(snapshot["events_per_wait_max"], 64)
                self.assertEqual(snapshot["full_event_batches"], 0)
                self.assertGreaterEqual(snapshot["recv_calls"], CLIENT_COUNT)
            finally:
                if stopped:
                    server.send_signal(signal.SIGCONT)
                for client in clients:
                    client.close()
                server.terminate()
                server.wait(timeout=3)


if __name__ == "__main__":
    unittest.main()
