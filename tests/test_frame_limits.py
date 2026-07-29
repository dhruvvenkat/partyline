from server_test_support import ChatServerTestCase, ROOT, wait_for_close


class FrameLimitTests(ChatServerTestCase):
    def test_frame_limit_disconnects_sender(self):
        sender = self.connect_client("sender")
        self.drain(self.receiver)
        payload = (ROOT / "tests" / "framing-test.txt").read_bytes()

        self.assertGreater(len(payload), 4096)
        self.assertNotIn(b"\n", payload)
        sender.sendall(payload)

        wait_for_close(sender)
        self.assert_receiver_alive()
