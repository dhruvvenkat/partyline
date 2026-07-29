from server_test_support import ChatServerTestCase, wait_for_close


class RateLimitingTests(ChatServerTestCase):
    def test_rate_limit_disconnects_sender(self):
        sender = self.connect_client("sender")
        self.drain(self.receiver)

        sender.sendall(b"spam\n" * 5)

        wait_for_close(sender)
        self.assert_receiver_alive()
