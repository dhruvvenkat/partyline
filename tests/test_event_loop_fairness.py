import socket
import threading
import time

from server_test_support import ChatServerTestCase


class EventLoopFairnessTests(ChatServerTestCase):
    def test_healthy_client_is_serviced_during_flood(self):
        sender = self.connect_client("flooder")
        self.drain(self.receiver)
        sender.settimeout(0.1)
        self.receiver.settimeout(0.1)

        response_received = threading.Event()
        stop_reader = threading.Event()
        stop_flood = threading.Event()
        received = bytearray()

        def read_receiver():
            while not stop_reader.is_set():
                try:
                    data = self.receiver.recv(4096)
                except socket.timeout:
                    continue
                except OSError:
                    return

                if not data:
                    return
                received.extend(data)
                if b"you are in room main-room" in received:
                    response_received.set()

        def flood_sender():
            payload = b"flood\n" * 32
            while not stop_flood.is_set():
                try:
                    sender.sendall(payload)
                except (socket.timeout, BrokenPipeError, ConnectionResetError, OSError):
                    return

        reader = threading.Thread(target=read_receiver)
        flooder = threading.Thread(target=flood_sender)
        reader.start()
        flooder.start()

        try:
            self.receiver.sendall(b"WHERE\n")
            self.assertTrue(
                response_received.wait(timeout=1.0),
                "healthy client did not receive WHERE response during flood",
            )
        finally:
            stop_flood.set()
            stop_reader.set()
            sender.close()
            self.receiver.close()
            flooder.join(timeout=1)
            reader.join(timeout=1)

        self.assertIn(b"you are in room main-room", received)
