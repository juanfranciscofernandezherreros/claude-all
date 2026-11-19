package com.kafkalab.model;

import java.time.Instant;

public record DltMessage(String originalPayload, String errorType, String errorMessage, Instant failedAt) {}
