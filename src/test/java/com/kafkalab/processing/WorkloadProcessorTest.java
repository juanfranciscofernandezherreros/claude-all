package com.kafkalab.processing;

import static org.junit.jupiter.api.Assertions.*;
import com.kafkalab.config.LabProperties.Workload;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class WorkloadProcessorTest {
    @Test void profilesAreDeterministicAndDistinct() {
        WorkloadProcessor processor = new WorkloadProcessor();
        byte[] light = processor.apply("payload", Workload.LIGHT);
        assertArrayEquals(light, processor.apply("payload", Workload.LIGHT));
        assertFalse(Arrays.equals(light, processor.apply("payload", Workload.MEDIUM)));
        assertFalse(Arrays.equals(processor.apply("payload", Workload.MEDIUM), processor.apply("payload", Workload.HEAVY)));
    }
    @Test void appliesExactlyOneTenAndOneHundredSha256Rounds() {
        WorkloadProcessor processor = new WorkloadProcessor();
        assertEquals("239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5",
                java.util.HexFormat.of().formatHex(processor.apply("payload", Workload.LIGHT)));
        assertEquals("8f2abd7f60f294f8fadf9ffe43543d8d9323fa664b5afb2d2ca58eefa838efee",
                java.util.HexFormat.of().formatHex(processor.apply("payload", Workload.MEDIUM)));
        assertEquals("449ece24b9648383301d05bc9ad142e25acde5fba2ead54b4c990124aad1caa3",
                java.util.HexFormat.of().formatHex(processor.apply("payload", Workload.HEAVY)));
    }

}
