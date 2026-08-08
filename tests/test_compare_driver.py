import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bench.compare import execution_order, experiment_groups, new_result_directory


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

    def test_workloads_are_split_into_independent_groups(self):
        args = SimpleNamespace(
            experiments="sparse,dense,broadcast",
            sparse_tiers="1000", sparse_active="1,10", sparse_rate=500.0,
            dense_tiers="100", dense_rates="1000,5000",
            broadcast_tiers="10", broadcast_rates="100,500",
        )
        groups = experiment_groups(args)
        self.assertEqual(
            [(group["experiment"], group["total"], group["active"]) for group in groups],
            [
                ("sparse", 1000, 1), ("sparse", 1000, 10),
                ("dense", 100, 100), ("broadcast", 10, 10),
            ],
        )
        self.assertEqual(groups[2]["rates"], [1000.0, 5000.0])


if __name__ == "__main__":
    unittest.main()
