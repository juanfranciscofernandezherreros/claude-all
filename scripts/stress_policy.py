"""Safety policy for incremental Kafka stress runs."""


def evaluate_step(status, error_count, memory_percent, current_lag, previous_lag, rising_steps):
    """Return (stop_reason, updated_rising_steps) for one completed stress step."""
    has_validation_errors = status != "PASS" or error_count > 0
    if has_validation_errors:
        return "validation-error", 0
    if memory_percent is None:
        return "memory-metric-unavailable", 0
    if memory_percent > 85:
        return "memory-over-85-percent", 0
    if current_lag is None:
        return "consumer-lag-unavailable", 0

    hasConsumerLagIncreased = previous_lag is not None and current_lag > previous_lag
    if hasConsumerLagIncreased:
        rising_steps += 1
    else:
        rising_steps = 0

    hasConsumerLagRisenForThreeSteps = rising_steps >= 3
    if hasConsumerLagRisenForThreeSteps:
        return "consumer-lag-rising-three-steps", rising_steps
    return None, rising_steps
