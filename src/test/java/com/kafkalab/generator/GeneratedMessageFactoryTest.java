package com.kafkalab.generator;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class GeneratedMessageFactoryTest {
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final GeneratedMessageFactory factory =
            new GeneratedMessageFactory(mapper, new MessageIdFactory());

    @Test
    void createsMessageWithDeterministicIdSequenceAndPayload() throws Exception {
        JsonNode node = mapper.readTree(factory.serialize("run-123", 42, "payload-data"));

        assertEquals(1, node.get("schemaVersion").asInt());
        assertEquals("run-123-42", node.get("messageId").asText());
        assertEquals("BENCHMARK", node.get("eventType").asText());
        assertEquals(42L, node.at("/payload/sequence").asLong());
        assertEquals("payload-data", node.at("/payload/data").asText());
        assertDoesNotThrow(() -> Instant.parse(node.get("createdAt").asText()));
    }

    @Test
    void rejectsInvalidSequenceThroughMessageIdContract() {
        assertThrows(IllegalArgumentException.class,
                () -> factory.serialize("run-123", 0, "payload"));
    }

    @Test
    void wrapsSerializationFailuresWithContext() {
        ObjectMapper brokenMapper = new ObjectMapper() {
            @Override
            public String writeValueAsString(Object value) throws com.fasterxml.jackson.core.JsonProcessingException {
                throw new com.fasterxml.jackson.core.JsonProcessingException("forced") {};
            }
        };
        GeneratedMessageFactory broken = new GeneratedMessageFactory(brokenMapper, new MessageIdFactory());
        var failure = assertThrows(java.util.concurrent.CompletionException.class,
                () -> broken.serialize("run-123", 1, "payload"));
        assertTrue(failure.getMessage().contains("Could not serialize"));
    }
}
