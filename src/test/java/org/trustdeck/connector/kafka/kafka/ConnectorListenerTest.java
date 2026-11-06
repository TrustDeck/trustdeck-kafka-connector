/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.kafka;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.ConnectException;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.kafka.support.Acknowledgment;
import org.trustdeck.client.exception.RecordLinkageConflictException;
import org.trustdeck.connector.kafka.config.ConnectorProperties;
import org.trustdeck.connector.kafka.health.OutageCoordinator;
import org.trustdeck.connector.kafka.listener.ConnectorListener;
import org.trustdeck.connector.kafka.mapping.JsonMessageMapper;
import org.trustdeck.connector.kafka.mapping.MappedMessage;
import org.trustdeck.connector.kafka.mapping.MappingException;
import org.trustdeck.connector.kafka.operation.OperationService;
import org.trustdeck.connector.kafka.operation.PseudonymBatchService;
import org.trustdeck.connector.kafka.recovery.RecoveryPublisher;

/** Verifies source offset safety and systemic-failure handling at the listener boundary. */
class ConnectorListenerTest {

    /** Ensures invalid later records cannot acknowledge before an earlier success. */
    @Test
    void acknowledgesRecordsInInputOrder() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        RecoveryPublisher recovery = mock(RecoveryPublisher.class);
        ConnectorProperties properties = properties("CREATE_ENTITY");
        PseudonymBatchService batches = mock(PseudonymBatchService.class);
        OutageCoordinator outage = mock(OutageCoordinator.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ConsumerRecord<byte[], byte[]> first = record(0, new byte[] {1});
        ConsumerRecord<byte[], byte[]> second = record(1, new byte[] {2});
        MappedMessage mapped = mapped();
        when(mapper.map(first.value())).thenReturn(mapped);
        when(mapper.map(second.value())).thenThrow(new MappingException("invalid"));

        new ConnectorListener(mapper, operations, recovery, properties, batches, outage)
                .onMessage(List.of(first, second), acknowledgment);

        InOrder order = inOrder(acknowledgment);
        order.verify(acknowledgment).acknowledge(0);
        order.verify(acknowledgment).acknowledge(1);
        verify(recovery).publishDlt(eq(second), eq(null), eq("INVALID_MAPPING"), eq(0),
                any(MappingException.class));
    }

    /** Ensures network failures pause consumption instead of being drained to the DLT. */
    @Test
    void pausesOnSystemicFailureWithoutAcknowledging() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        RecoveryPublisher recovery = mock(RecoveryPublisher.class);
        ConnectorProperties properties = properties("CREATE_ENTITY");
        PseudonymBatchService batches = mock(PseudonymBatchService.class);
        OutageCoordinator outage = mock(OutageCoordinator.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ConsumerRecord<byte[], byte[]> source = record(0, new byte[] {1});
        when(mapper.map(source.value())).thenReturn(mapped());
        when(operations.execute(any(), any()))
                .thenThrow(new RuntimeException(new ConnectException("backend unavailable")));

        assertThrows(Exception.class, () -> new ConnectorListener(mapper, operations, recovery, properties, batches, outage)
                .onMessage(List.of(source), acknowledgment));

        verify(outage).pause("BACKEND_UNAVAILABLE", "domain", "entity", null);
        verify(acknowledgment, never()).acknowledge(0);
        verify(recovery, never()).publishDlt(any(), any(), anyString(), anyInt(), any());
    }

    /** Ensures a recovery producer failure pauses processing before the source offset is acknowledged. */
    @Test
    void pausesWhenRecoveryPublicationFails() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        RecoveryPublisher recovery = mock(RecoveryPublisher.class);
        ConnectorProperties properties = properties("CREATE_ENTITY");
        PseudonymBatchService batches = mock(PseudonymBatchService.class);
        OutageCoordinator outage = mock(OutageCoordinator.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ConsumerRecord<byte[], byte[]> source = record(0, new byte[] {1});
        when(mapper.map(source.value())).thenThrow(new MappingException("invalid"));
        doThrow(new IllegalStateException("recovery broker unavailable")).when(recovery)
                .publishDlt(eq(source), eq(null), eq("INVALID_MAPPING"), eq(0),
                        any(MappingException.class));

        assertThrows(Exception.class, () -> new ConnectorListener(mapper, operations, recovery, properties, batches, outage)
                .onMessage(List.of(source), acknowledgment));

        verify(outage).pause("RECOVERY_PRODUCER_UNAVAILABLE");
        verify(acknowledgment, never()).acknowledge(0);
    }

    /** Sends a deterministic batch rejection to DLT rather than labeling it unknown. */
    @Test
    void classifiesRejectedBatchAsDlt() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        RecoveryPublisher recovery = mock(RecoveryPublisher.class);
        ConnectorProperties properties = properties("CREATE_PSEUDONYM");
        PseudonymBatchService batches = mock(PseudonymBatchService.class);
        OutageCoordinator outage = mock(OutageCoordinator.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ConsumerRecord<byte[], byte[]> first = record(0, new byte[] {1});
        ConsumerRecord<byte[], byte[]> second = record(1, new byte[] {2});
        when(mapper.map(first.value())).thenReturn(mapped());
        when(mapper.map(second.value())).thenReturn(mapped());
        doThrow(new IllegalArgumentException("batch rejected")).when(batches).execute(anyList());

        new ConnectorListener(mapper, operations, recovery, properties, batches, outage)
                .onMessage(List.of(first, second), acknowledgment);

        verify(recovery, times(2)).publishDlt(any(), any(), eq("TRANSIENT_OPERATION_FAILURE"), eq(0),
                any(IllegalArgumentException.class));
        verify(recovery, never()).publishUnknown(any(), any(), any(), any());
    }

    /** Routes a TrustDeck linkage conflict to the review topic and acknowledges it. */
    @Test
    void publishesLinkageConflictToReview() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        RecoveryPublisher recovery = mock(RecoveryPublisher.class);
        ConnectorProperties properties = properties("CREATE_ENTITY");
        PseudonymBatchService batches = mock(PseudonymBatchService.class);
        OutageCoordinator outage = mock(OutageCoordinator.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ConsumerRecord<byte[], byte[]> source = record(0, new byte[] {1});
        MappedMessage mapped = mapped();
        when(mapper.map(source.value())).thenReturn(mapped);
        when(operations.execute(eq(mapped), any()))
                .thenThrow(mock(RecordLinkageConflictException.class));

        new ConnectorListener(mapper, operations, recovery, properties, batches, outage)
                .onMessage(List.of(source), acknowledgment);

        verify(recovery).publishReview(eq(source), eq(mapped), eq(1),
                any(RecordLinkageConflictException.class));
        verify(acknowledgment).acknowledge(0);
    }

    private ConnectorProperties properties(String operation) {
        ConnectorProperties properties = new ConnectorProperties();
        properties.setOperation(operation);
        properties.setMaxRecordBytes(100);
        return properties;
    }

    private ConsumerRecord<byte[], byte[]> record(long offset, byte[] value) {
        return new ConsumerRecord<>("source", 0, offset, new byte[] {(byte) offset}, value);
    }

    private MappedMessage mapped() {
        return new MappedMessage("domain", "identifier", "type", "psn", null, null,
                null, "entity", null, null, null, null, null, null, null, null, null, null);
    }
}
