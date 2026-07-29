import socket
import subprocess
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "tests"
SERVER = ROOT / "server"
HOST = "127.0.0.1"
PORT = 1234


def receive_until(sock, marker, timeout=2.0):
    deadline = time.monotonic() + timeout
    received = b""

    while marker not in received:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"timed out waiting for {marker!r}; received {received!r}")

        sock.settimeout(remaining)
        chunk = sock.recv(4096)
        if not chunk:
            raise AssertionError("server closed the connection unexpectedly")
        received += chunk

    return received


def wait_for_close(sock, timeout=2.0):
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        try:
            if sock.recv(1) == b"":
                return
        except (socket.timeout, ConnectionResetError):
            pass

    raise AssertionError("sender was not disconnected")


class ChatServerTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(["make", "-B", "server"], cwd=ROOT, check=True)

    def setUp(self):
        self.server = subprocess.Popen(
            [str(SERVER)],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        self.clients = []
        time.sleep(0.05)
        if self.server.poll() is not None:
            raise unittest.SkipTest("port 1234 is already in use; stop the existing server first")
        self.receiver = self.connect_client("receiver")
        self.drain(self.receiver)

    def tearDown(self):
        for client in self.clients:
            client.close()

        if self.server.poll() is None:
            self.server.terminate()
            try:
                self.server.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.server.kill()
                self.server.wait()

    def connect_client(self, username):
        deadline = time.monotonic() + 2
        last_error = None

        while time.monotonic() < deadline:
            if self.server.poll() is not None:
                raise AssertionError("test server exited; port 1234 may already be in use")

            sock = None
            try:
                sock = socket.create_connection((HOST, PORT), timeout=0.2)
                sock.settimeout(1)
                receive_until(sock, b"enter your username: ")
                sock.sendall((username + "\n").encode())
                self.clients.append(sock)
                return sock
            except (ConnectionRefusedError, socket.timeout, OSError) as error:
                last_error = error
                if sock is not None:
                    sock.close()
                time.sleep(0.02)

        raise AssertionError(f"could not connect to server: {last_error}")

    @staticmethod
    def drain(sock):
        sock.settimeout(0.05)
        while True:
            try:
                if not sock.recv(4096):
                    return
            except socket.timeout:
                return

    def assert_receiver_alive(self):
        self.receiver.sendall(b"WHERE\n")
        response = receive_until(self.receiver, b"you are in room main-room")
        self.assertIn(b"you are in room main-room", response)
