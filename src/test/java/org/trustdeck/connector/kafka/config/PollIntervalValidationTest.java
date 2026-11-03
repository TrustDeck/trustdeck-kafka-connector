/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

/** Verifies that the batch time budget plus one in-flight attempt fits into max.poll.interval.ms. */
class PollIntervalValidationTest {

    /** The shipped defaults (300s, 120s, 5s, 30s) are consistent. */
    @Test
    void acceptsDefaults() {
        assertDoesNotThrow(() -> KafkaConfiguration.validatePollInterval(settings("300000"), properties(Duration.ofSeconds(120))));
        assertDoesNotThrow(() -> KafkaConfiguration.validatePollInterval(new HashMap<>(), properties(Duration.ofSeconds(120))));
    }

    /** A budget that leaves no room for the in-flight attempt is rejected at startup. */
    @Test
    void rejectsBudgetWithoutHeadroom() {
        assertThrows(IllegalArgumentException.class,
                () -> KafkaConfiguration.validatePollInterval(settings("300000"), properties(Duration.ofSeconds(240))));
    }

    private Map<String, Object> settings(String pollInterval) {
        Map<String, Object> settings = new HashMap<>();
        settings.put("max.poll.interval.ms", pollInterval);
        return settings;
    }

    private ConnectorProperties properties(Duration budget) {
        ConnectorProperties properties = new ConnectorProperties();
        properties.setConnectTimeout(Duration.ofSeconds(5));
        properties.setReadTimeout(Duration.ofSeconds(30));
        properties.getRecovery().setBatchTimeBudget(budget);
        return properties;
    }
}
