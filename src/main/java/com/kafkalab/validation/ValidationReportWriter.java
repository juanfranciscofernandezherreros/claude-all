package com.kafkalab.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.springframework.stereotype.Component;

/** Owns the external JSON and console representation of a validation report. */
@Component
public class ValidationReportWriter {
    private final ObjectMapper mapper;

    public ValidationReportWriter(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public void write(Path reportPath, Map<String, Object> report) throws IOException {
        String formattedReport = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report);
        mapper.writerWithDefaultPrettyPrinter().writeValue(reportPath.toFile(), report);
        System.out.println(formattedReport);
    }
}
