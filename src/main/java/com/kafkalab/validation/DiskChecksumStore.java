package com.kafkalab.validation;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;

/**
 * Stores expected and actual SHA-256 values in fixed-width temporary files,
 * keeping the validator from retaining checksum strings for every message.
 */
final class DiskChecksumStore implements AutoCloseable {
    private static final int CHECKSUM_BYTES = 32;

    private final long expectedCount;
    private final Path expectedPath;
    private final Path actualPath;
    private final RandomAccessFile expectedFile;
    private final RandomAccessFile actualFile;

    DiskChecksumStore(Path directory, long expectedCount) throws IOException {
        if (expectedCount < 1) throw new IllegalArgumentException("expectedCount must be positive");
        Files.createDirectories(directory);
        Path expected = Files.createTempFile(directory, "expected-checksums-", ".bin");
        Path actual = Files.createTempFile(directory, "actual-checksums-", ".bin");
        RandomAccessFile expectedStore = null;
        RandomAccessFile actualStore = null;
        try {
            expectedStore = new RandomAccessFile(expected.toFile(), "rw");
            actualStore = new RandomAccessFile(actual.toFile(), "rw");
            long bytes = Math.multiplyExact(expectedCount, CHECKSUM_BYTES);
            expectedStore.setLength(bytes);
            actualStore.setLength(bytes);
            this.expectedCount = expectedCount;
            this.expectedPath = expected;
            this.actualPath = actual;
            this.expectedFile = expectedStore;
            this.actualFile = actualStore;
        } catch (IOException | RuntimeException e) {
            if (expectedStore != null) try { expectedStore.close(); } catch (IOException ignored) { }
            if (actualStore != null) try { actualStore.close(); } catch (IOException ignored) { }
            Files.deleteIfExists(expected);
            Files.deleteIfExists(actual);
            throw e;
        }
    }

    void writeExpected(long sequence, String checksum) throws IOException {
        write(expectedFile, sequence, checksum);
    }

    void writeActual(long sequence, String checksum) throws IOException {
        write(actualFile, sequence, checksum);
    }

    boolean matches(long sequence) throws IOException {
        byte[] expected = read(expectedFile, sequence);
        byte[] actual = read(actualFile, sequence);
        return Arrays.equals(expected, actual);
    }

    private void write(RandomAccessFile file, long sequence, String checksum) throws IOException {
        byte[] bytes = HexFormat.of().parseHex(checksum);
        if (bytes.length != CHECKSUM_BYTES) {
            throw new IllegalArgumentException("Checksum must contain exactly 32 bytes");
        }
        file.seek(offset(sequence));
        file.write(bytes);
    }

    private byte[] read(RandomAccessFile file, long sequence) throws IOException {
        byte[] bytes = new byte[CHECKSUM_BYTES];
        file.seek(offset(sequence));
        file.readFully(bytes);
        return bytes;
    }

    private long offset(long sequence) {
        if (sequence < 1 || sequence > expectedCount) {
            throw new IllegalArgumentException("sequence must be between 1 and " + expectedCount);
        }
        return Math.multiplyExact(sequence - 1, CHECKSUM_BYTES);
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        try { expectedFile.close(); } catch (IOException e) { failure = e; }
        try { actualFile.close(); } catch (IOException e) { if (failure == null) failure = e; }
        try { Files.deleteIfExists(expectedPath); } catch (IOException e) { if (failure == null) failure = e; }
        try { Files.deleteIfExists(actualPath); } catch (IOException e) { if (failure == null) failure = e; }
        if (failure != null) throw failure;
    }
}
