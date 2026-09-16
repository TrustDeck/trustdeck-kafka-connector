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
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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

/** Verifies terminal unsafe batch handling at the broker boundary. */
@Testcontainers
class KafkaFailureIntegrationTest {

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.8.0"));

    private String source;
    private String dlt;
    private String group;
    private HttpServer trustDeck;
    private ConfigurableApplicationContext context;
    private int producerRequestSize = 1_048_576;

    /** Creates unique topics and starts the controlled TrustDeck endpoint. */
    @BeforeEach
    void start() throws Exception {
        source = "connector-failure-" + UUID.randomUUID();
        dlt = source + ".DLT";
        group = source + "-group";
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(source, 1, (short) 1), new NewTopic(dlt, 1, (short) 1)))
                    .all().get(15, TimeUnit.SECONDS);
        }
        trustDeck = HttpServer.create(new InetSocketAddress(0), 0);
        trustDeck.createContext("/", this::respond);
        trustDeck.start();
    }

    /** Closes the fresh connector context and controlled endpoint. */
    @AfterEach
    void stop() {
        if (context != null) {
            context.close();
        }
        trustDeck.stop(0);
    }

    /** Publishes an unsafe 206 outcome to DLT before acknowledging the source batch. */
    @Test
    void unsafePartialBatchPublishesUnknownOutcomeBeforeCommit() throws Exception {
        context = startConnector();
        var first = send("{\"identifier\":\"one\",\"idType\":\"type\",\"psn\":\"p1\"}");
        send("{\"identifier\":\"two\",\"idType\":\"type\",\"psn\":\"p2\"}");

        JsonNode envelope = new ObjectMapper().readTree(pollDlt());
        assertEquals("UNKNOWN_OUTCOME", envelope.get("status").asText());
        assertEquals(first.offset(), envelope.get("offset").asLong());
        awaitCommitted(first.partition(), first.offset() + 2);
    }

    /** Leaves the source offset uncommitted when the real recovery producer rejects its envelope. */
    @Test
    void recoveryProducerFailureLeavesSourceOffsetUncommitted() throws Exception {
        producerRequestSize = 1_000;
        context = startConnector();
        var record = send("{\"identifier\":\"one\",\"idType\":\"type\",\"psn\":\""
                + "x".repeat(4_000) + "\"}");

        Thread.sleep(2_000);

        assertEquals(-1, committedOffset(record.partition()));
        assertTrue(groupHasMember());
    }

    private ConfigurableApplicationContext startConnector() {
        Map<String, Object> settings = Map.ofEntries(
                Map.entry("spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers()),
                Map.entry("spring.kafka.properties.security.protocol", "PLAINTEXT"),
                Map.entry("spring.kafka.consumer.properties.security.protocol", "PLAINTEXT"),
                Map.entry("spring.kafka.producer.properties.security.protocol", "PLAINTEXT"),
                Map.entry("spring.kafka.producer.properties.max.request.size", Integer.toString(producerRequestSize)),
                Map.entry("spring.kafka.producer.properties.delivery.timeout.ms", "3000"),
                Map.entry("spring.kafka.producer.properties.request.timeout.ms", "1000"),
                Map.entry("spring.kafka.producer.properties.retries", "0"),
                Map.entry("spring.kafka.consumer.auto-offset-reset", "earliest"),
                Map.entry("spring.kafka.consumer.max-poll-records", "2"),
                Map.entry("spring.kafka.listener.missing-topics-fatal", "false"),
                Map.entry("connector.operation", "CREATE_PSEUDONYM"),
                Map.entry("connector.source-topic", source),
                Map.entry("connector.group-id", group),
                Map.entry("connector.recovery.dlt-topic", dlt),
                Map.entry("connector.recovery.review-topic", source + ".REVIEW"),
                Map.entry("connector.preflight.enabled", "false"),
                Map.entry("connector.trustdeck.service-url", serviceUrl()),
                Map.entry("connector.trustdeck.token-endpoint", serviceUrl() + "/token"),
                Map.entry("connector.trustdeck.client-id", "failure-client"),
                Map.entry("connector.trustdeck.client-secret", "failure-secret"),
                Map.entry("connector.trustdeck.project", "synthetic-project"),
                Map.entry("connector.trustdeck.entity-type", "synthetic-type"),
                Map.entry("connector.mapping.common.domain-constant", "synthetic-domain"),
                Map.entry("connector.mapping.common.identifier-path", "/identifier"),
                Map.entry("connector.mapping.common.id-type-path", "/idType"),
                Map.entry("connector.mapping.common.psn-path", "/psn"));
        return new SpringApplicationBuilder(TrustDeckKafkaConnectorApplication.class).web(WebApplicationType.NONE)
                .run(settings.entrySet().stream().map(entry -> "--" + entry.getKey() + "=" + entry.getValue())
                        .toArray(String[]::new));
    }

    private org.apache.kafka.clients.producer.RecordMetadata send(String value) throws Exception {
        Map<String, Object> settings = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(settings)) {
            return producer.send(new ProducerRecord<>(source, 0, null, value.getBytes(StandardCharsets.UTF_8)))
                    .get(15, TimeUnit.SECONDS);
        }
    }

    private String pollDlt() {
        Map<String, Object> settings = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "observer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(settings)) {
            consumer.subscribe(List.of(dlt));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(250));
                if (!records.isEmpty()) {
                    return records.iterator().next().value();
                }
            }
        }
        throw new AssertionError("No recovery envelope was observed");
    }

    private void awaitCommitted(int partition, long expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline && committedOffset(partition) < expected) {
            Thread.sleep(100);
        }
        assertTrue(committedOffset(partition) >= expected);
    }

    private long committedOffset(int partition) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            var offset = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
                    .get(new TopicPartition(source, partition));
            return offset == null ? -1 : offset.offset();
        }
    }

    private boolean groupHasMember() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            ConsumerGroupDescription description = admin.describeConsumerGroups(List.of(group)).all().get(5, TimeUnit.SECONDS)
                    .get(group);
            return !description.members().isEmpty();
        }
    }

    private String serviceUrl() {
        return "http://localhost:" + trustDeck.getAddress().getPort();
    }

    private void respond(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (path.equals("/token")) {
            write(exchange, 200, "{\"access_token\":\"failure-token\",\"expires_in\":300}");
        } else if ("POST".equals(exchange.getRequestMethod()) && path.endsWith("/batch")) {
            write(exchange, 206, "[{\"psn\":\"p1\"}]");
        } else {
            write(exchange, 200, "{}");
        }
    }

    private void write(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
