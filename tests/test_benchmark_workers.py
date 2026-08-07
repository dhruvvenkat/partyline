import unittest

from bench.benchmark import is_disconnect_log, merge_states, partition_clients


class BenchmarkWorkerTests(unittest.TestCase):
    def test_partitions_and_merges_worker_state(self):
        clients = [{"index": index} for index in range(5)]
        self.assertEqual(
            [[client["index"] for client in group] for group in partition_clients(clients, 2)],
            [[0, 2, 4], [1, 3]],
        )

        base = {
            "offered": 1,
            "completed": 1,
            "completed_by_deadline": 1,
            "deliveries": 2,
            "deliveries_by_deadline": 2,
            "send_errors": 0,
            "latencies_ms": [0.1],
            "scheduler_lag_ms": [0.2],
            "disconnects": set(),
        }
        other = base | {
            "offered": 2,
            "completed": 2,
            "deliveries": 4,
            "latencies_ms": [0.3],
            "disconnects": {3},
        }
        merged = merge_states([base, other])
        self.assertEqual(merged["offered"], 3)
        self.assertEqual(merged["latencies_ms"], [0.1, 0.3])
        self.assertEqual(merged["disconnects"], {3})
        self.assertTrue(is_disconnect_log("disconnect fd=3 reason=test"))
        self.assertTrue(
            is_disconnect_log(
                "timestamp=now level=info event=client.disconnected fd=3"
            )
        )
        self.assertFalse(is_disconnect_log("event=client.accepted fd=3"))


if __name__ == "__main__":
    unittest.main()
