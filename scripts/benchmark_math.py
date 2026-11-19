"""Pure calculations shared by benchmark reporting and tests."""


def end_to_end_seconds(publish_start_epoch_ms: int, validated_at_epoch_seconds: float) -> float:
    if publish_start_epoch_ms < 0 or validated_at_epoch_seconds < 0:
        raise ValueError("timestamps must be non-negative")
    return max(0.0, validated_at_epoch_seconds - publish_start_epoch_ms / 1000.0)


def throughput_per_second(validated_unique_outputs: int, end_to_end_duration_seconds: float) -> float:
    if validated_unique_outputs < 0 or end_to_end_duration_seconds < 0:
        raise ValueError("counts and durations must be non-negative")
    return validated_unique_outputs / end_to_end_duration_seconds if end_to_end_duration_seconds > 0 else 0.0
