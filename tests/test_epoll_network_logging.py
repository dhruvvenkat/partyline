import os
import socket
import subprocess
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "epoll" / "server"
PROMPT = b"enter your username: "


class EpollNetworkLoggingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(["make", "-C", "epoll", "server"], cwd=ROOT, check=True)

    def test_logs_network_lifecycle_and_io_as_structured_lines(self):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]

        environment = os.environ.copy()
        environment["CHAT_SERVER_PORT"] = str(port)
        environment["CHAT_LOG_LEVEL"] = "debug"
        server = subprocess.Popen(
            [str(SERVER)],
            cwd=ROOT,
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )

        client = None
        try:
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                try:
                    client = socket.create_connection(("127.0.0.1", port), timeout=0.2)
                    break
                except ConnectionRefusedError:
                    time.sleep(0.02)
            self.assertIsNotNone(client, "server did not start listening")

            received = b""
            while PROMPT not in received:
                received += client.recv(4096)
            client.sendall(b"logger\nWHERE\nQUIT\n")
            client.settimeout(2)
            while client.recv(4096):
                pass
        finally:
            if client is not None:
                client.close()
            server.terminate()
            try:
                server.wait(timeout=2)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()

        logs = server.stderr.read()
        server.stderr.close()
        self.assertIn(" event=listener.ready ", logs)
        self.assertIn(" event=client.accepted ", logs)
        self.assertIn(" event=client.authenticated ", logs)
        self.assertIn(" event=socket.received ", logs)
        self.assertIn(" event=client.disconnected ", logs)
        for line in logs.splitlines():
            self.assertRegex(
                line,
                r"^timestamp=\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z "
                r"level=(debug|info|warn|error) event=[a-z_.]+(?: |$)",
            )


if __name__ == "__main__":
    unittest.main()
