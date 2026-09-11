/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** Proves that integration tests use a disposable Kafka broker and isolated resources. */
@Testcontainers
class KafkaResourceIntegrationTest {

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.8.0"));

    /** Sends and consumes a unique record through the disposable broker. */
    @Test
    void roundTripUsesIsolatedKafkaResource() throws Exception {
        String topic = "connector-test-" + UUID.randomUUID();
        byte[] key = new byte[] {1, 2};
        byte[] value = new byte[] {3, 4};
        Map<String, Object> producerSettings = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerSettings)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get(15, java.util.concurrent.TimeUnit.SECONDS);
        }

        Map<String, Object> consumerSettings = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "connector-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerSettings)) {
            consumer.subscribe(java.util.List.of(topic));
            var records = consumer.poll(Duration.ofSeconds(15));
            assertEquals(1, records.count());
            assertEquals(java.util.Arrays.toString(key), java.util.Arrays.toString(records.iterator().next().key()));
            assertEquals(java.util.Arrays.toString(value), java.util.Arrays.toString(records.iterator().next().value()));
        }
    }
}
