/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.listener;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.trustdeck.connector.kafka.health.OutageCoordinator;

/** Verifies that unexpected listener failures neither retry, commit, nor fail on revoked partitions. */
class ListenerErrorHandlerTest {

    private final TopicPartition assigned = new TopicPartition("source", 0);
    private final TopicPartition revoked = new TopicPartition("source", 1);

    /** Offsets are only committed by the listener itself. */
    @Test
    void neverCommitsAfterHandling() {
        assertFalse(handler(mock(OutageCoordinator.class)).isAckAfterHandle());
        assertTrue(handler(mock(OutageCoordinator.class)).seeksAfterHandling());
    }

    /** Assigned partitions rewind to the committed offset; revoked partitions are skipped. */
    @Test
    @SuppressWarnings("unchecked")
    void rewindsAssignedPartitionsToCommittedOffsets() {
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.assignment()).thenReturn(Set.of(assigned));
        when(consumer.committed(Set.of(assigned))).thenReturn(Map.of(assigned, new OffsetAndMetadata(11)));
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        when(container.isRunning()).thenReturn(true);
        OutageCoordinator outage = mock(OutageCoordinator.class);
        Runnable invokeListener = mock(Runnable.class);

        handler(outage).handleBatch(new IllegalStateException("bug"), batch(), consumer, container, invokeListener);

        verify(consumer).seek(assigned, 11);
        verify(consumer, never()).seek(eq(revoked), anyLong());
        verify(invokeListener, never()).run();
        verify(outage).pause(ListenerErrorHandler.UNEXPECTED_LISTENER_FAILURE);
    }

    /** Without a committed offset the batch start is used, and a seek race with a rebalance is tolerated. */
    @Test
    @SuppressWarnings("unchecked")
    void fallsBackToBatchStartAndToleratesRevocation() {
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.assignment()).thenReturn(Set.of(assigned));
        when(consumer.committed(Set.of(assigned))).thenReturn(Map.of());
        doThrow(new IllegalStateException("No current assignment for partition source-0")).when(consumer).seek(assigned, 10);
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        OutageCoordinator outage = mock(OutageCoordinator.class);

        handler(outage).handleBatch(new IllegalStateException("bug"), batch(), consumer, container, () -> { });

        verify(consumer).seek(assigned, 10);
        // A stopped container (shutdown) is not reported as an outage
        verify(outage, never()).pause(anyString());
    }

    private ConsumerRecords<byte[], byte[]> batch() {
        return new ConsumerRecords<>(Map.of(
                assigned, List.of(record(assigned, 10), record(assigned, 11), record(assigned, 12)),
                revoked, List.of(record(revoked, 20))));
    }

    private ConsumerRecord<byte[], byte[]> record(TopicPartition partition, long offset) {
        return new ConsumerRecord<>(partition.topic(), partition.partition(), offset, null, new byte[] {1});
    }

    @SuppressWarnings("unchecked")
    private ListenerErrorHandler handler(OutageCoordinator outage) {
        ObjectProvider<OutageCoordinator> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(outage);
        return new ListenerErrorHandler(provider);
    }
}
