/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpStatusCode;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.trustdeck.client.exception.TrustDeckResponseException;
import org.trustdeck.connector.kafka.config.ConnectorProperties;
import org.trustdeck.connector.kafka.mapping.MappedMessage;
import org.trustdeck.connector.kafka.operation.PseudonymConflictException;
import org.trustdeck.connector.kafka.recovery.RecoveryPublisher;

/** Verifies safe, confirmed recovery envelope publication. */
class RecoveryPublisherTest {

    /** Ensures recovery retains source bytes but excludes unsafe headers. */
    @Test
    void publishesConfirmedEnvelopeWithSafeProvenance() throws Exception {
        @SuppressWarnings("unchecked")
		KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        ConnectorProperties properties = new ConnectorProperties();
        properties.setOperation("CREATE_ENTITY");
        properties.getRecovery().setDltTopic("source.DLT");
        ObjectMapper mapper = new ObjectMapper();
        CompletableFuture<SendResult<String, String>> confirmation = CompletableFuture.completedFuture(
                new SendResult<>(null, new RecordMetadata(new TopicPartition("source.DLT", 0), 4, 0, 0, 0, 0)));
       when(template.send(eq("source.DLT"), eq("AQI="), anyString())).thenReturn(confirmation);

        byte[] key = {1, 2};
        byte[] value = {3, 4};
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("source", 2, 7, key, value);
        record.headers().add(new RecordHeader("correlation-id", "corr-1".getBytes()));
        record.headers().add(new RecordHeader("authorization", "secret".getBytes()));
        MappedMessage mapped = new MappedMessage("domain", "identifier", "type", "psn", null, null,
                "user", "entity", null, null, null, null, null, null, null, null, null, null);

        new RecoveryPublisher(template, properties, mapper).publishDlt(record, mapped, "INVALID_MAPPING", 2,
                new IllegalArgumentException("bad input"));

         ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
        verify(template).send(eq("source.DLT"), eq("AQI="), jsonCaptor.capture());
        JsonNode envelope = mapper.readTree(jsonCaptor.getValue());
        assertEquals("2", envelope.get("schemaVersion").asText());
        assertEquals("FAILED_DLT", envelope.get("status").asText());
        assertEquals(2, envelope.get("partition").asInt());
        assertEquals(7, envelope.get("offset").asLong());
        assertEquals(Base64.getEncoder().encodeToString(key), envelope.get("keyBase64").asText());
        assertEquals(Base64.getEncoder().encodeToString(value), envelope.get("valueBase64").asText());
        assertEquals("corr-1", envelope.get("safeHeaders").get("correlation-id").asText());
        assertEquals(1, envelope.get("safeHeaders").size());
        assertArrayEquals(key, Base64.getDecoder().decode(envelope.get("keyBase64").asText()));
        assertArrayEquals(value, Base64.getDecoder().decode(envelope.get("valueBase64").asText()));
    }

    /** A pseudonym conflict goes to the review topic with its reason and the 422 status of the wrapped rejection. */
    @Test
    void publishesPseudonymConflictForReview() throws Exception {
        @SuppressWarnings("unchecked")
		KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        ConnectorProperties properties = new ConnectorProperties();
        properties.setOperation("CREATE_PSEUDONYM");
        properties.getRecovery().setReviewTopic("source.review");
        ObjectMapper mapper = new ObjectMapper();
        when(template.send(eq("source.review"), eq(null), anyString())).thenReturn(CompletableFuture.completedFuture(
                new SendResult<>(null, new RecordMetadata(new TopicPartition("source.review", 0), 0, 0, 0, 0, 0))));
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("source", 0, 3, null, new byte[] {1});
        MappedMessage mapped = new MappedMessage("test-domain", "identifier", "type", "psn", null, null,
                null, null, null, null, null, null, null, null, null, null, null, null);

        new RecoveryPublisher(template, properties, mapper).publishConflict(record, mapped, new PseudonymConflictException(
                PseudonymConflictException.Reason.IDENTIFIER_HAS_OTHER_PSEUDONYM,
                new TrustDeckResponseException("rejected", HttpStatusCode.valueOf(422))));

        ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
        verify(template).send(eq("source.review"), eq(null), jsonCaptor.capture());
        JsonNode envelope = mapper.readTree(jsonCaptor.getValue());
        assertEquals("PENDING_CONFLICT_REVIEW", envelope.get("status").asText());
        assertEquals("PSEUDONYM_CONFLICT_IDENTIFIER_HAS_OTHER_PSEUDONYM", envelope.get("failureCategory").asText());
        assertEquals(422, envelope.get("httpStatus").asInt());
        assertEquals("test-domain", envelope.get("resolvedTarget").asText());
    }

    /** Ensures a failed producer confirmation prevents recovery publication from being treated as complete. */
    @Test
    void propagatesRecoveryProducerFailure() {
        @SuppressWarnings("unchecked")
		KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        ConnectorProperties properties = new ConnectorProperties();
        properties.setOperation("CREATE_ENTITY");
        properties.getRecovery().setDltTopic("source.DLT");
         when(template.send(eq("source.DLT"), eq(null), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("source", 0, 0, null, new byte[] {1});

        assertThrows(ExecutionException.class,
                () -> new RecoveryPublisher(template, properties, new ObjectMapper())
                        .publishDlt(record, null, "INVALID_MAPPING", 0, new IllegalArgumentException("bad input")));
    }
}
