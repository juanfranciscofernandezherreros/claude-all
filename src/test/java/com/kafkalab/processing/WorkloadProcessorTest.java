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
}
