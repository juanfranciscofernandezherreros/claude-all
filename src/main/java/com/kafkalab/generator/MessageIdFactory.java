package com.kafkalab.generator;

import com.kafkalab.exception.InvalidMessageIdException;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class MessageIdFactory {
    public String newRunId() { return UUID.randomUUID().toString(); }

    public String create(String runId, long sequence) {
        boolean hasInvalidRunId = runId == null || runId.isBlank();
        boolean hasNonPositiveSequence = sequence < 1;
        if (hasInvalidRunId || hasNonPositiveSequence) {
            throw new InvalidMessageIdException("runId and positive sequence are required");
        }
        return runId + "-" + sequence;
    }
}
