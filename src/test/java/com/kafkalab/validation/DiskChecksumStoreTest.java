package com.kafkalab.validation;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DiskChecksumStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void storesAndComparesChecksumsBySequenceAndDeletesTemporaryFiles() throws Exception {
        String first = "ab".repeat(32);
        String second = "cd".repeat(32);
        try (DiskChecksumStore store = new DiskChecksumStore(tempDir, 3)) {
            store.writeExpected(1, first);
            store.writeActual(1, first);
            store.writeExpected(2, first);
            store.writeActual(2, second);

            assertTrue(store.matches(1));
            assertFalse(store.matches(2));
        }
        try (var files = Files.list(tempDir)) {
            assertEquals(0, files.count());
        }
    }

    @Test
    void rejectsInvalidSequencesAndChecksumWidths() throws Exception {
        try (DiskChecksumStore store = new DiskChecksumStore(tempDir, 1)) {
            assertThrows(IllegalArgumentException.class, () -> store.writeExpected(0, "00".repeat(32)));
            assertThrows(IllegalArgumentException.class, () -> store.writeExpected(1, "00"));
        }
    }
}
