package com.kafkalab.model;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
