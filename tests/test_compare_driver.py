import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bench.compare import execution_order, new_result_directory


class CompareDriverTests(unittest.TestCase):
    def test_alternates_order_and_never_reuses_result_directory(self):
        implementations = ("poll", "epoll")
        self.assertEqual(execution_order(implementations, 1), implementations)
        self.assertEqual(execution_order(implementations, 2), ("epoll", "poll"))

        with tempfile.TemporaryDirectory() as directory:
            with patch("bench.compare.command_output", return_value="abc1234"):
                first = new_result_directory(Path(directory), "smoke")
                second = new_result_directory(Path(directory), "smoke")
            self.assertNotEqual(first, second)
            self.assertTrue(first.is_dir())
            self.assertTrue(second.is_dir())


if __name__ == "__main__":
    unittest.main()
