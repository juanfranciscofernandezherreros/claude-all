package com.kafkalab.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

class ChecksumServiceTest {
    @Test void matchesKnownSha256Vector() {
        assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                new ChecksumService().sha256("abc"));
    }
    @Test void handlesUtf8Deterministically() {
        ChecksumService service = new ChecksumService();
        assertEquals(service.sha256("café"), service.sha256("café"));
    }
}
