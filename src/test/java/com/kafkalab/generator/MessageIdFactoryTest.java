package com.kafkalab.generator;

import static org.junit.jupiter.api.Assertions.*;

import com.kafkalab.exception.InvalidMessageIdException;
import org.junit.jupiter.api.Test;

class MessageIdFactoryTest {
    @Test void buildsStableRunSequenceIds() {
        MessageIdFactory factory = new MessageIdFactory();
        assertEquals("run-1", factory.create("run", 1));
        assertEquals("run-42", factory.create("run", 42));
        assertNotEquals(factory.create("run", 1), factory.create("run", 2));
    }
    @Test void rejectsInvalidInput() {
        MessageIdFactory factory = new MessageIdFactory();
        assertThrows(InvalidMessageIdException.class, () -> factory.create("", 1));
        assertThrows(InvalidMessageIdException.class, () -> factory.create("run", 0));
    }
}
