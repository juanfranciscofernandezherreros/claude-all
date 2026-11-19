package com.kafkalab.processing;

import com.kafkalab.config.LabProperties.Workload;
import com.kafkalab.exception.ChecksumUnavailableException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.springframework.stereotype.Component;

@Component
public class WorkloadProcessor {
    private volatile int workSink;
    public byte[] apply(String value, Workload workload) {
        int rounds = switch (workload) { case LIGHT -> 1; case MEDIUM -> 10; case HEAVY -> 100; };
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] current = value.getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < rounds; i++) current = digest.digest(current);
            workSink = current[0] & 0xff;
            return current;
        } catch (NoSuchAlgorithmException impossible) {
            throw new ChecksumUnavailableException("SHA-256 is not available", impossible);
        }
    }
}
