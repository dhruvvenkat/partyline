import heapq
import selectors
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from bench.benchmark import (
    SERVER_METRIC_FIELDS,
    classify_result,
    due_offers,
    empty_state,
    flush_client_send,
    is_disconnect_log,
    merge_states,
    partition_clients,
    process_client_read,
    rotate_ready,
    run_worker_phase,
    valid_summary_groups,
)


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

    def test_catch_up_is_bounded_and_late_offers_are_missed(self):
        schedule = [(index, index, object()) for index in range(5)]
        heapq.heapify(schedule)
        ready = due_offers(schedule, now_ns=100, max_count=2, max_lag_ns=10)
        self.assertEqual(len(ready), 2)
        self.assertTrue(all(offer[1] for offer in ready))
        self.assertEqual(len(schedule), 3)

    def test_ready_clients_rotate_fairly(self):
        events = [(index, selectors.EVENT_READ) for index in range(3)]
        first, cursor = rotate_ready(events, 0)
        second, _ = rotate_ready(events, cursor)
        self.assertEqual([item[0] for item in first], [0, 1, 2])
        self.assertEqual([item[0] for item in second], [1, 2, 0])

    def test_reads_are_bounded_per_socket(self):
        class FakeSocket:
            def __init__(self):
                self.calls = 0

            def recv(self, _size):
                self.calls += 1
                return b"xxxx"

        client = {
            "index": 0,
            "sock": FakeSocket(),
            "active": True,
            "buffer": b"",
            "pending": [],
            "send_buffer": bytearray(),
            "closed": False,
            "last_read_service_ns": 0,
        }
        phase = {
            "workload": "direct",
            "deadline_ns": 10,
            "max_recv_calls": 2,
            "max_recv_bytes": 8,
        }
        process_client_read(object(), client, empty_state(), phase, 1)
        self.assertEqual(client["sock"].calls, 2)
        self.assertEqual(client["buffer"], b"xxxxxxxx")

    def test_partial_client_sends_are_retried(self):
        class FakeSocket:
            def __init__(self):
                self.calls = 0

            def send(self, data):
                self.calls += 1
                return 2 if self.calls == 1 else len(data)

        class FakeSelector:
            def modify(self, *_args):
                pass

        client = {"sock": FakeSocket(), "send_buffer": bytearray(b"abcdef")}
        state = empty_state()
        flush_client_send(FakeSelector(), client, state)
        self.assertEqual(client["send_buffer"], b"")
        self.assertEqual(state["client_partial_sends"], 1)

    def test_workers_receive_one_synchronized_start_time(self):
        class FakeControl:
            def __init__(self):
                self.messages = []

            def send(self, message):
                self.messages.append(message)

        controls = [FakeControl(), FakeControl()]
        phase = {"start_ns": 123456, "duration": 0, "drain": 0}
        replies = [
            {"type": "result", "state": empty_state(0)},
            {"type": "result", "state": empty_state(1)},
        ]
        with patch("bench.benchmark.receive_worker_messages", return_value=replies):
            run_worker_phase(controls, phase)
        self.assertEqual(
            [control.messages[0]["phase"]["start_ns"] for control in controls],
            [123456, 123456],
        )

    def test_invalid_trials_never_enter_valid_medians(self):
        valid = {
            "valid": True, "workload": "direct", "total_connections": 10,
            "active_connections": 1, "p50_ms": 2.0,
        }
        invalid = valid | {"valid": False, "p50_ms": 1000.0}
        groups = valid_summary_groups([invalid, valid])
        self.assertEqual(groups[("direct", 10, 1)], [valid])

    def test_validity_reports_each_failed_criterion(self):
        row = {field: 0 for field in SERVER_METRIC_FIELDS}
        row.update(
            harness_error="", server_crashed=False, client_disconnects=1,
            server_disconnects=1, send_errors=1, delivery_ratio=0.5,
            missed_offer_ratio=0.5, scheduling_lag_p99_ms=50,
            max_read_service_gap_ms=50, queue_overflows=1,
        )
        args = SimpleNamespace(
            allow_disconnects=False, min_delivery_ratio=0.999,
            max_missed_offer_ratio=0.01, max_scheduler_lag_ms=20,
            max_read_service_gap_ms=20, allow_queue_overflow=False,
        )
        valid, reasons = classify_result(row, args)
        self.assertFalse(valid)
        for reason in (
            "unexpected disconnects", "unexpected send errors", "delivery ratio",
            "missed offer ratio", "scheduler p99", "read-service gap",
            "unexpected queue overflows",
        ):
            self.assertIn(reason, reasons)


if __name__ == "__main__":
    unittest.main()
