package com.kafkalab.model;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class InputMessageTest {
    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test void acceptsValidVersionedMessage() {
        InputMessage message = new InputMessage(1, "run-1", Instant.parse("2025-01-01T00:00:00Z"),
                "BENCHMARK", new InputMessage.Payload(1, "payload"));
        assertTrue(validator.validate(message).isEmpty());
    }

    @Test void rejectsMissingRequiredFields() {
        InputMessage message = new InputMessage(0, " ", null, "", new InputMessage.Payload(-1, null));
        assertFalse(validator.validate(message).isEmpty());
    }
    @Test void roundTripsUnicodePayloadAndTimestampAsJson() throws Exception {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        InputMessage original = new InputMessage(1, "run-7", Instant.parse("2025-01-01T00:00:00Z"),
                "BENCHMARK", new InputMessage.Payload(7, "café-東京-🙂"));
        String json = mapper.writeValueAsString(original);
        InputMessage decoded = mapper.readValue(json, InputMessage.class);
        assertEquals(original, decoded);
        assertEquals(original.payload(), decoded.payload());
    }

    @Test void rejectsMalformedJson() {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        assertThrows(JsonProcessingException.class, () -> mapper.readValue("{", InputMessage.class));
    }

    @Test void acceptsMinimumSequenceAndSupportedPayloadByteBoundaries() {
        for (int size : new int[]{1024, 102400}) {
            String data = "x".repeat(size);
            InputMessage message = new InputMessage(1, "run-0", Instant.EPOCH,
                    "BENCHMARK", new InputMessage.Payload(0, data));
            assertTrue(validator.validate(message).isEmpty());
            assertEquals(size, message.payload().data().getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
        }
    }

}
