package com.kafkalab.validation;

/** Percentile calculations over an already sorted latency sample. */
public final class LatencyStatistics {
    private LatencyStatistics() {
    }

    public static long percentile(long[] sorted, int count, double percentile) {
        boolean hasNoSamples = sorted == null;
        if (hasNoSamples) throw new IllegalArgumentException("sorted samples must not be null");

        boolean hasInvalidSampleCount = count < 1 || count > sorted.length;
        if (hasInvalidSampleCount) {
            throw new IllegalArgumentException("count must be within sample bounds");
        }

        boolean isPercentileOutOfRange = percentile < 0.0 || percentile > 1.0;
        boolean isPercentileNotANumber = Double.isNaN(percentile);
        if (isPercentileOutOfRange || isPercentileNotANumber) {
            throw new IllegalArgumentException("percentile must be between 0 and 1");
        }
        int index = Math.max(0, (int) Math.ceil(percentile * count) - 1);
        return sorted[index];
    }
}
