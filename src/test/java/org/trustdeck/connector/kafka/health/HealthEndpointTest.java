/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.health;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;

/** Verifies the externally exposed liveness and readiness HTTP contracts. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.kafka.bootstrap-servers=localhost:9092",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.listener.missing-topics-fatal=false",
        "connector.preflight.enabled=false",
        "spring.kafka.properties.security.protocol=PLAINTEXT",
        "connector.operation=CREATE_PSEUDONYM",
        "connector.source-topic=health-source",
        "connector.group-id=health-group",
        "connector.trustdeck.service-url=https://backend.example.test",
        "connector.trustdeck.token-endpoint=https://identity.example.test/token",
        "connector.trustdeck.client-id=test-client",
        "connector.trustdeck.client-secret=test-secret",
        "connector.trustdeck.project=health-project",
        "connector.mapping.common.domain-constant=health-domain",
        "connector.mapping.common.identifier-path=/identifier",
        "connector.mapping.common.id-type-path=/idType",
        "connector.recovery.dlt-topic=health-dlt",
        "connector.recovery.review-topic=health-review"
})
class HealthEndpointTest {

    @LocalServerPort
    private int port;

    /** Liveness remains up for a healthy connector. */
    @Test
    void livenessIsUp() {
        ResponseEntity<String> response = client().get().uri("/actuator/health/liveness").retrieve()
                .toEntity(String.class);

        assertEquals(200, response.getStatusCode().value());
    }

    /** Readiness is available separately from liveness. */
    @Test
    void readinessIsExposed() {
        ResponseEntity<String> response = client().get().uri("/actuator/health/readiness").retrieve()
                .toEntity(String.class);

        assertEquals(200, response.getStatusCode().value());
    }

    private RestClient client() {
        return RestClient.builder().baseUrl("http://localhost:" + port).build();
    }
}
