package com.kafkalab.generator;

import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class MessageIdFactory {
    public String newRunId() { return UUID.randomUUID().toString(); }
    public String create(String runId, long sequence) {
        if (runId == null || runId.isBlank() || sequence < 1) throw new IllegalArgumentException("runId and positive sequence are required");
        return runId + "-" + sequence;
    }
}
