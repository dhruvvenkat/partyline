import tempfile
import unittest
from pathlib import Path

from bench.benchmark import PERF_BASE_EVENTS, available_perf_events, profiler_command


class ProfileDriverTests(unittest.TestCase):
    def test_only_available_syscall_tracepoints_are_requested(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "sys_enter_poll").mkdir()
            events = available_perf_events((root,))
        self.assertEqual(events[:len(PERF_BASE_EVENTS)], list(PERF_BASE_EVENTS))
        self.assertIn("syscalls:sys_enter_poll", events)
        self.assertNotIn("syscalls:sys_enter_epoll_wait", events)

    def test_perf_and_strace_attach_to_the_server_pid(self):
        perf = profiler_command("perf", Path("perf.txt"), 123, ["task-clock"])
        trace = profiler_command("strace", Path("strace.txt"), 123)
        self.assertEqual(perf[-2:], ["-p", "123"])
        self.assertEqual(trace[-2:], ["-p", "123"])


if __name__ == "__main__":
    unittest.main()
