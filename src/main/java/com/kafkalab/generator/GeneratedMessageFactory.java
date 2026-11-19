package com.kafkalab.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalab.model.InputMessage;
import java.time.Instant;
import java.util.concurrent.CompletionException;
import org.springframework.stereotype.Component;

/** Creates the domain content and JSON representation of generated records. */
@Component
public class GeneratedMessageFactory {
    private final ObjectMapper mapper;
    private final MessageIdFactory ids;

    public GeneratedMessageFactory(ObjectMapper mapper, MessageIdFactory ids) {
        this.mapper = mapper;
        this.ids = ids;
    }

    public String serialize(String runId, long sequence, String payload) {
        InputMessage message = new InputMessage(
                1,
                ids.create(runId, sequence),
                Instant.now(),
                "BENCHMARK",
                new InputMessage.Payload(sequence, payload));
        try {
            return mapper.writeValueAsString(message);
        } catch (Exception e) {
            throw new CompletionException("Could not serialize generated message", e);
        }
    }
}
