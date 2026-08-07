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


def wait_for_lines(path, count):
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if path.exists():
            lines = path.read_text().splitlines()
            if len(lines) >= count:
                return [json.loads(line) for line in lines]
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {count} metric records")


class ServerMetricsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(["make", "release"], cwd=ROOT, check=True)

    def test_immediate_response_needs_no_writable_interest_change(self):
        for implementation, relative_server in (
            ("poll", "poll(2)/server"),
            ("epoll", "epoll/server"),
        ):
            with self.subTest(implementation=implementation), tempfile.TemporaryDirectory() as temp:
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
                    [str(ROOT / relative_server)],
                    cwd=ROOT,
                    env=environment,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                client = None
                try:
                    deadline = time.monotonic() + 2
                    while client is None:
                        try:
                            client = socket.create_connection(("127.0.0.1", port), timeout=0.1)
                        except OSError:
                            if time.monotonic() >= deadline:
                                self.fail("server did not start")
                            time.sleep(0.01)
                    client.recv(4096)
                    client.sendall(b"metrics-user\n")
                    time.sleep(0.05)
                    client.setblocking(False)
                    try:
                        while client.recv(4096):
                            pass
                    except BlockingIOError:
                        pass
                    client.settimeout(1)

                    server.send_signal(signal.SIGUSR1)
                    wait_for_lines(metrics_path, 1)
                    client.sendall(b"WHERE\n")
                    self.assertIn(b"you are in room main-room", client.recv(4096))
                    server.send_signal(signal.SIGUSR2)
                    snapshot = wait_for_lines(metrics_path, 2)[1]

                    self.assertGreaterEqual(snapshot["immediate_write_successes"], 1)
                    self.assertGreaterEqual(snapshot["send_calls"], 1)
                    self.assertGreaterEqual(snapshot["recv_calls"], 1)
                    self.assertEqual(snapshot["epoll_ctl_mod"], 0)
                finally:
                    if client is not None:
                        client.close()
                    server.terminate()
                    server.wait(timeout=2)


if __name__ == "__main__":
    unittest.main()
