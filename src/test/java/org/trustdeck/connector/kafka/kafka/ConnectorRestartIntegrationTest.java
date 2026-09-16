/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.trustdeck.connector.kafka.TrustDeckKafkaConnectorApplication;

/** Verifies source offsets and redelivery across fresh connector application contexts. */
@Testcontainers
class ConnectorRestartIntegrationTest {

    private static final String TOPIC = "connector-restart-" + UUID.randomUUID();
    private static final String DLT = TOPIC + ".DLT";
    private static final String GROUP = "connector-restart-group-" + UUID.randomUUID();

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.8.0"));

    private HttpServer trustDeck;
    private final List<String> markers = new CopyOnWriteArrayList<>();
    private final AtomicBoolean failBlocked = new AtomicBoolean(true);
    private ConfigurableApplicationContext context;

    /** Creates isolated Kafka topics and an HTTP stub for each test. */
    @BeforeEach
    void start() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1), new NewTopic(DLT, 1, (short) 1)))
                    .all().get(15, TimeUnit.SECONDS);
        }
        trustDeck = HttpServer.create(new InetSocketAddress(0), 0);
        trustDeck.createContext("/", this::respond);
        trustDeck.start();
    }

    /** Closes the connector context and HTTP stub created by the test. */
    @AfterEach
    void stop() {
        if (context != null) {
            context.close();
        }
        trustDeck.stop(0);
    }

    /** Proves committed prefixes survive restart while an uncommitted record is redelivered. */
    @Test
    void preservesCommittedPrefixAndRedeliversBlockedRecord() throws Exception {
        context = startConnector();
        var first = send("committed");
        awaitCommitted(first.offset() + 1);
        context.close();
        context = null;
        awaitMarkerCount(1);

        context = startConnector();
        var blocked = send("blocked");
        awaitMarkerCount(2);
        assertEquals(first.offset() + 1, committedOffset());
        assertTrue(groupHasMember());
        context.close();
        context = null;

        failBlocked.set(false);
        context = startConnector();
        awaitMarkerCount(3);
        awaitCommitted(blocked.offset() + 1);

        assertEquals(List.of("committed", "blocked", "blocked"), markers);
    }

    /** Starts a fresh application context with the same broker and group. */
    private ConfigurableApplicationContext startConnector() {
        Map<String, Object> properties = Map.ofEntries(
                Map.entry("spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers()),
                Map.entry("spring.kafka.properties.security.protocol", "PLAINTEXT"),
                Map.entry("spring.kafka.consumer.properties.security.protocol", "PLAINTEXT"),
                Map.entry("spring.kafka.producer.properties.security.protocol", "PLAINTEXT"),
                Map.entry("spring.kafka.consumer.auto-offset-reset", "earliest"),
                Map.entry("spring.kafka.consumer.max-poll-records", "1"),
                Map.entry("spring.kafka.listener.missing-topics-fatal", "false"),
                Map.entry("connector.operation", "CREATE_ENTITY"),
                Map.entry("connector.source-topic", TOPIC),
                Map.entry("connector.group-id", GROUP),
                Map.entry("connector.recovery.dlt-topic", DLT),
                Map.entry("connector.recovery.review-topic", TOPIC + ".REVIEW"),
                Map.entry("connector.preflight.enabled", "false"),
                Map.entry("connector.trustdeck.service-url", "http://localhost:" + trustDeck.getAddress().getPort()),
                Map.entry("connector.trustdeck.token-endpoint", "http://localhost:" + trustDeck.getAddress().getPort()
                        + "/token"),
                Map.entry("connector.trustdeck.client-id", "restart-client"),
                Map.entry("connector.trustdeck.client-secret", "restart-secret"),
                Map.entry("connector.trustdeck.project", "synthetic-project"),
                Map.entry("connector.trustdeck.entity-type", "synthetic-type"),
                Map.entry("connector.mapping.common.entity-fields[0].source-path", "/marker"),
                Map.entry("connector.mapping.common.entity-fields[0].target-path", "marker"));
        return new SpringApplicationBuilder(TrustDeckKafkaConnectorApplication.class)
                .web(WebApplicationType.NONE)
                .run(properties.entrySet().stream().map(entry -> "--" + entry.getKey() + "=" + entry.getValue())
                        .toArray(String[]::new));
    }

    /** Sends one distinguishable record to the only source partition. */
    private org.apache.kafka.clients.producer.RecordMetadata send(String marker) throws Exception {
        Map<String, Object> settings = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(settings)) {
            return producer.send(new ProducerRecord<>(TOPIC, 0, marker.getBytes(StandardCharsets.UTF_8),
                    ("{\"marker\":\"" + marker + "\"}").getBytes(StandardCharsets.UTF_8))).get(15,
                            TimeUnit.SECONDS);
        }
    }

    /** Waits for a source offset commit with a bounded diagnostic timeout. */
    private void awaitCommitted(long expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            if (committedOffset() >= expected) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Expected committed offset " + expected + " but saw " + committedOffset());
    }

    /** Reads the connector group's broker-observed offset. */
    private long committedOffset() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            var offset = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS).get(new TopicPartition(TOPIC, 0));
            return offset == null ? 0 : offset.offset();
        }
    }

    /** Confirms the consumer remains a group member while processing is paused. */
    private boolean groupHasMember() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            return !admin.describeConsumerGroups(List.of(GROUP)).all().get(5, TimeUnit.SECONDS).get(GROUP).members().isEmpty();
        }
    }

    /** Waits until the controlled HTTP stub has observed the requested number of writes. */
    private void awaitMarkerCount(int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (markers.size() < expected && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(expected, markers.size());
    }

    /** Responds to OAuth, preflight, and synthetic entity requests. */
    private void respond(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (path.equals("/token")) {
            write(exchange, 200, "{\"access_token\":\"restart-token\",\"expires_in\":300}");
            return;
        }
        if (!"POST".equals(exchange.getRequestMethod())) {
            write(exchange, 200, "{}");
            return;
        }
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        String marker = body.replaceAll(".*\\\"marker\\\":\\\"([^\\\"]+)\\\".*", "$1");
        markers.add(marker);
        if (marker.equals("blocked") && failBlocked.get()) {
            write(exchange, 503, "{\"error\":\"temporarily unavailable\"}");
        } else {
            write(exchange, 201, "{\"trustdeckID\":\"11111111-1111-1111-1111-111111111111\",\"data\":{}}");
        }
    }

    /** Writes one bounded HTTP response. */
    private void write(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
