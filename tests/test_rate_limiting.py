from server_test_support import ChatServerTestCase, receive_until


class BurstTrafficTests(ChatServerTestCase):
    def test_sender_can_exceed_old_rate_limit(self):
        sender = self.connect_client("sender")
        self.drain(self.receiver)

        sender.sendall(b"spam\n" * 5)
        sender.sendall(b"WHERE\n")
        response = receive_until(sender, b"you are in room main-room")
        self.assertIn(b"you are in room main-room", response)
