import unittest

from stress_policy import evaluate_step


class StressPolicyTest(unittest.TestCase):
    def test_stops_when_validation_fails_or_any_error_is_counted(self):
        self.assertEqual(("validation-error", 0), evaluate_step("FAIL", 0, 50, 0, 0, 0))
        self.assertEqual(("validation-error", 0), evaluate_step("PASS", 1, 50, 0, 0, 0))

    def test_stops_when_memory_exceeds_85_percent_or_is_unavailable(self):
        self.assertEqual(("memory-over-85-percent", 0),
                         evaluate_step("PASS", 0, 85.1, 0, 0, 0))
        self.assertEqual(("memory-metric-unavailable", 0),
                         evaluate_step("PASS", 0, None, 0, 0, 0))

    def test_stops_after_three_consecutive_lag_increases(self):
        reason, rising = evaluate_step("PASS", 0, 70, 11, 10, 0)
        self.assertIsNone(reason)
        self.assertEqual(1, rising)
        reason, rising = evaluate_step("PASS", 0, 70, 12, 11, rising)
        self.assertIsNone(reason)
        self.assertEqual(2, rising)
        self.assertEqual(("consumer-lag-rising-three-steps", 3),
                         evaluate_step("PASS", 0, 70, 13, 12, rising))

    def test_resets_lag_rising_streak_when_lag_stabilizes_or_falls(self):
        self.assertEqual((None, 0), evaluate_step("PASS", 0, 70, 10, 10, 2))
        self.assertEqual((None, 0), evaluate_step("PASS", 0, 70, 9, 10, 2))

    def test_stops_if_consumer_lag_cannot_be_measured(self):
        self.assertEqual(("consumer-lag-unavailable", 0),
                         evaluate_step("PASS", 0, 70, None, 10, 0))


if __name__ == "__main__":
    unittest.main()
