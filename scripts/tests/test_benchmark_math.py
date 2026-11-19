import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark_math import end_to_end_seconds, throughput_per_second


class BenchmarkMathTest(unittest.TestCase):
    def test_end_to_end_duration_uses_epoch_milliseconds_and_seconds(self):
        self.assertEqual(2.5, end_to_end_seconds(1_000, 3.5))

    def test_end_to_end_duration_never_becomes_negative(self):
        self.assertEqual(0.0, end_to_end_seconds(5_000, 4.0))

    def test_throughput_uses_only_validated_unique_outputs(self):
        self.assertEqual(4.0, throughput_per_second(20, 5.0))

    def test_throughput_is_monotonic_with_count_and_inverse_with_duration(self):
        baseline = throughput_per_second(10, 2.0)
        self.assertGreater(throughput_per_second(20, 2.0), baseline)
        self.assertLess(throughput_per_second(10, 4.0), baseline)

    def test_zero_duration_has_zero_throughput(self):
        self.assertEqual(0.0, throughput_per_second(10, 0.0))

    def test_rejects_negative_counts_and_timestamps(self):
        with self.assertRaises(ValueError):
            end_to_end_seconds(-1, 1.0)
        with self.assertRaises(ValueError):
            throughput_per_second(-1, 1.0)


if __name__ == "__main__":
    unittest.main()
