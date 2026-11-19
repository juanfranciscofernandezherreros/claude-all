package com.kafkalab.processing;

import com.kafkalab.config.LabProperties.Workload;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.springframework.stereotype.Service;

@Service
public class WorkloadProcessor {
    public byte[] apply(String value, Workload workload) {
        int rounds = switch (workload) { case LIGHT -> 1; case MEDIUM -> 10; case HEAVY -> 100; };
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] current = value.getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < rounds; i++) current = digest.digest(current);
            return current;
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is not available", impossible);
        }
    }
}
