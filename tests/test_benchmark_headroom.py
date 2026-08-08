import csv
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class BenchmarkHeadroomTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(["make", "-C", "epoll", "server"], cwd=ROOT, check=True)

    def test_generator_sustains_five_times_the_smoke_rate(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "headroom.csv"
            subprocess.run(
                [
                    "python3", "bench/benchmark.py",
                    "--server", "epoll/server", "--label", "epoll",
                    "--tiers", "10", "--active-connections", "5",
                    "--workloads", "direct", "--rate", "5000",
                    "--runs", "1", "--duration", "0.5", "--warmup", "0.1",
                    "--drain", "0.5", "--workers", "2",
                    "--max-offer-lag-ms", "50", "--output", str(output),
                ],
                cwd=ROOT,
                check=True,
                stdout=subprocess.DEVNULL,
            )
            with output.open() as source:
                row = next(csv.DictReader(source))
            self.assertLess(int(row["missed_offers"]) / int(row["offered_operations"]), 0.01)
            self.assertLess(float(row["scheduling_lag_p95_ms"]), 10)
            self.assertEqual(int(row["client_disconnects"]), 0)
            self.assertEqual(int(row["send_errors"]), 0)


if __name__ == "__main__":
    unittest.main()
