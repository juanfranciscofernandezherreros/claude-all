package com.kafkalab.validation;

/** Percentile calculations over an already sorted latency sample. */
public final class LatencyStatistics {
    private LatencyStatistics() {
    }

    public static long percentile(long[] sorted, int count, double percentile) {
        if (sorted == null) throw new IllegalArgumentException("sorted samples must not be null");
        if (count < 1 || count > sorted.length) throw new IllegalArgumentException("count must be within sample bounds");
        if (percentile < 0.0 || percentile > 1.0 || Double.isNaN(percentile)) {
            throw new IllegalArgumentException("percentile must be between 0 and 1");
        }
        int index = Math.max(0, (int) Math.ceil(percentile * count) - 1);
        return sorted[index];
    }
}
