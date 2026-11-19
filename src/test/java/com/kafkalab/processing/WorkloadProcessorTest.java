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
        assertEquals("e78731bb4b42a923b88955eb380c2dd368353e597783bf2e8f2c52dbf6944e8d",
                java.util.HexFormat.of().formatHex(processor.apply("payload", Workload.MEDIUM)));
        assertEquals("0048668f956bd8998b4fd7d7e774fe43d6a31536cf7b0f66c3da86eb6a526298",
                java.util.HexFormat.of().formatHex(processor.apply("payload", Workload.HEAVY)));
    }

}
