package com.kafkalab.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class LatencyStatisticsTest {
    @Test
    void computesNearestRankPercentilesOnSortedSamples() {
        long[] sorted = {10, 20, 30, 40};
        assertEquals(10, LatencyStatistics.percentile(sorted, 4, 0.00));
        assertEquals(20, LatencyStatistics.percentile(sorted, 4, 0.50));
        assertEquals(40, LatencyStatistics.percentile(sorted, 4, 0.95));
        assertEquals(40, LatencyStatistics.percentile(sorted, 4, 0.99));
        assertEquals(40, LatencyStatistics.percentile(sorted, 4, 1.00));
    }

    @Test
    void handlesSingleSampleAndRejectsInvalidInputs() {
        assertEquals(7, LatencyStatistics.percentile(new long[]{7}, 1, 0.99));
        assertThrows(IllegalArgumentException.class, () -> LatencyStatistics.percentile(new long[]{}, 0, 0.5));
        assertThrows(IllegalArgumentException.class, () -> LatencyStatistics.percentile(new long[]{1}, 2, 0.5));
        assertThrows(IllegalArgumentException.class, () -> LatencyStatistics.percentile(new long[]{1}, 1, 1.1));
        assertThrows(IllegalArgumentException.class, () -> LatencyStatistics.percentile(new long[]{1}, 1, Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> LatencyStatistics.percentile(null, 1, 0.5));
    }
}
