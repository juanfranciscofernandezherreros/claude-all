package com.kafkalab.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class MessageEnvelopeContractTest {
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void outputMessageJsonKeepsStableEnvelopeAndResultFields() throws Exception {
        OutputMessage output = new OutputMessage(1, "run-1", Instant.parse("2025-01-01T00:00:00Z"),
                Instant.parse("2025-01-01T00:00:00.010Z"), "OK",
                new OutputMessage.Result(1, "a".repeat(64)));

        JsonNode json = mapper.readTree(mapper.writeValueAsBytes(output));
        assertTrue(json.has("schemaVersion"));
        assertTrue(json.has("messageId"));
        assertTrue(json.has("createdAt"));
        assertTrue(json.has("processedAt"));
        assertTrue(json.has("status"));
        assertEquals(1, json.path("result").path("sequence").asInt());
        assertEquals(64, json.path("result").path("checksum").asText().length());
        assertEquals(output, mapper.treeToValue(json, OutputMessage.class));
    }

    @Test
    void dltMessageJsonKeepsOriginalPayloadAndFailureMetadata() throws Exception {
        DltMessage dlt = new DltMessage("{\"messageId\":\"run-2\"}", "InvalidInputMessageException",
                "invalid payload", Instant.parse("2025-01-01T00:00:01Z"));

        JsonNode json = mapper.readTree(mapper.writeValueAsBytes(dlt));
        assertTrue(json.has("originalPayload"));
        assertEquals("InvalidInputMessageException", json.path("errorType").asText());
        assertEquals("invalid payload", json.path("errorMessage").asText());
        assertTrue(json.has("failedAt"));
        assertEquals(dlt, mapper.treeToValue(json, DltMessage.class));
    }
}
