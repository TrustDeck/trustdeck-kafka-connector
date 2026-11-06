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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.http.HttpStatusCode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.web.client.ResourceAccessException;
import org.trustdeck.client.exception.RecordLinkageConflictException;
import org.trustdeck.client.exception.TrustDeckClientLibraryException;
import org.trustdeck.client.exception.TrustDeckResponseException;
import org.trustdeck.connector.kafka.config.ConnectorProperties;
import org.trustdeck.connector.kafka.health.OutageCoordinator;
import org.trustdeck.connector.kafka.listener.ConnectorListener;
import org.trustdeck.connector.kafka.mapping.JsonMessageMapper;
import org.trustdeck.connector.kafka.mapping.MappedMessage;
import org.trustdeck.connector.kafka.mapping.MappingException;
import org.trustdeck.connector.kafka.operation.OperationService;
import org.trustdeck.connector.kafka.operation.PseudonymBatchService;
import org.trustdeck.connector.kafka.operation.PseudonymConflictException;
import org.trustdeck.connector.kafka.recovery.RecoveryPublisher;
import org.trustdeck.connector.kafka.trustdeck.TokenAcquisitionException;

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

        new ConnectorListener(mapper, operations, recovery, properties, batches, outage)
                .onMessage(List.of(source), acknowledgment);

        verify(outage).pause("BACKEND_UNAVAILABLE", "domain", "entity", null);
        verify(acknowledgment, never()).acknowledge(0);
        verify(acknowledgment).nack(0, Duration.ZERO);
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

        new ConnectorListener(mapper, operations, recovery, properties, batches, outage)
                .onMessage(List.of(source), acknowledgment);

        verify(outage).pause("RECOVERY_PRODUCER_UNAVAILABLE");
        verify(acknowledgment, never()).acknowledge(0);
        verify(acknowledgment).nack(0, Duration.ZERO);
    }

    /** A batch that cannot be submitted is processed record by record instead of being dead-lettered. */
    @Test
    void processesRecordsIndividuallyWhenBatchIsNotAccepted() throws Exception {
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
        doThrow(new IllegalArgumentException("batch exceeds byte limit")).when(batches).execute(anyList());

        new ConnectorListener(mapper, operations, recovery, properties, batches, outage)
                .onMessage(List.of(first, second), acknowledgment);

        verify(operations, times(2)).execute(any(), any());
        verify(recovery, never()).publishDlt(any(), any(), any(), anyInt(), any());
        InOrder order = inOrder(acknowledgment);
        order.verify(acknowledgment).acknowledge(0);
        order.verify(acknowledgment).acknowledge(1);
    }

    /**
     * A batch rejected as a whole (422, e.g. every record already exists) falls back to single creates.
     * The duplicate succeeds there; a record TrustDeck still rejects goes to the DLT without retries.
     */
    @Test
    void wholeBatchRejectionFallsBackAndDoesNotRetryRejections() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        RecoveryPublisher recovery = mock(RecoveryPublisher.class);
        PseudonymBatchService batches = mock(PseudonymBatchService.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ConsumerRecord<byte[], byte[]> duplicate = record(0, new byte[] {1});
        ConsumerRecord<byte[], byte[]> invalid = record(1, new byte[] {2});
        when(mapper.map(any())).thenReturn(mapped());
        doThrow(new TrustDeckResponseException("rejected", HttpStatusCode.valueOf(422))).when(batches).execute(anyList());
        when(operations.execute(any(), eq("source/0/1"))).thenThrow(new TrustDeckResponseException("rejected", HttpStatusCode.valueOf(422)));

        new ConnectorListener(mapper, operations, recovery, properties("CREATE_PSEUDONYM"), batches, mock(OutageCoordinator.class))
                .onMessage(List.of(duplicate, invalid), acknowledgment);

        verify(operations, times(1)).execute(any(), eq("source/0/1"));
        verify(recovery).publishDlt(eq(invalid), any(), eq("REJECTED_BY_TRUSTDECK"), eq(0), any(TrustDeckResponseException.class));
        verify(recovery, never()).publishDlt(eq(duplicate), any(), any(), anyInt(), any());
        InOrder order = inOrder(acknowledgment);
        order.verify(acknowledgment).acknowledge(0);
        order.verify(acknowledgment).acknowledge(1);
        verify(acknowledgment, never()).nack(anyInt(), any());
    }

    /** Records confirmed by a partial response are acknowledged; only the others are created singly. */
    @Test
    void partialBatchProcessesOnlyUnconfirmedRecords() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        PseudonymBatchService batches = mock(PseudonymBatchService.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ConsumerRecord<byte[], byte[]> first = record(0, new byte[] {1});
        ConsumerRecord<byte[], byte[]> second = record(1, new byte[] {2});
        ConsumerRecord<byte[], byte[]> third = record(2, new byte[] {3});
        when(mapper.map(any())).thenReturn(mapped());
        when(batches.execute(anyList())).thenReturn(Set.of(1));

        new ConnectorListener(mapper, operations, mock(RecoveryPublisher.class), properties("CREATE_PSEUDONYM"), batches,
                mock(OutageCoordinator.class)).onMessage(List.of(first, second, third), acknowledgment);

        verify(operations).execute(any(), eq("source/0/0"));
        verify(operations, never()).execute(any(), eq("source/0/1"));
        verify(operations).execute(any(), eq("source/0/2"));
        InOrder order = inOrder(acknowledgment);
        order.verify(acknowledgment).acknowledge(0);
        order.verify(acknowledgment).acknowledge(1);
        order.verify(acknowledgment).acknowledge(2);
    }

    /** A fully confirmed batch is acknowledged at once without single creates. */
    @Test
    void completeBatchIsAcknowledgedAtOnce() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        PseudonymBatchService batches = mock(PseudonymBatchService.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(mapper.map(any())).thenReturn(mapped());
        when(batches.execute(anyList())).thenReturn(Set.of(0, 1));

        new ConnectorListener(mapper, operations, mock(RecoveryPublisher.class), properties("CREATE_PSEUDONYM"), batches,
                mock(OutageCoordinator.class)).onMessage(List.of(record(0, new byte[] {1}), record(1, new byte[] {2})), acknowledgment);

        verify(operations, never()).execute(any(), any());
        verify(acknowledgment).acknowledge(1);
    }

    /** A poll spanning two partitions is sent as one batch per partition instead of record by record. */
    @Test
    void splitsMixedPartitionPollIntoBatches() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        PseudonymBatchService batches = mock(PseudonymBatchService.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(mapper.map(any())).thenReturn(mapped());
        when(batches.execute(anyList())).thenReturn(Set.of(0, 1));
        List<ConsumerRecord<byte[], byte[]>> poll = List.of(record(0, 0, new byte[] {1}), record(0, 1, new byte[] {2}),
                record(1, 0, new byte[] {3}), record(1, 1, new byte[] {4}));

        new ConnectorListener(mapper, operations, mock(RecoveryPublisher.class), properties("CREATE_PSEUDONYM"), batches,
                mock(OutageCoordinator.class)).onMessage(poll, acknowledgment);

        verify(batches, times(2)).execute(argThat(chunk -> chunk.size() == 2));
        verify(operations, never()).execute(any(), any());
        InOrder order = inOrder(acknowledgment);
        order.verify(acknowledgment).acknowledge(1);
        order.verify(acknowledgment).acknowledge(3);
    }

    /** Chunks respect the configured batch size; a leftover record forms its own chunk. */
    @Test
    void limitsChunksToBatchSize() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        PseudonymBatchService batches = mock(PseudonymBatchService.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ConnectorProperties properties = properties("CREATE_PSEUDONYM");
        properties.setBatchSize(2);
        when(mapper.map(any())).thenReturn(mapped());
        when(batches.execute(anyList())).thenReturn(Set.of(0, 1), Set.of(0));

        new ConnectorListener(mapper, operations, mock(RecoveryPublisher.class), properties, batches, mock(OutageCoordinator.class))
                .onMessage(List.of(record(0, new byte[] {1}), record(1, new byte[] {2}), record(2, new byte[] {3})), acknowledgment);

        verify(batches).execute(argThat(chunk -> chunk.size() == 2));
        verify(batches).execute(argThat(chunk -> chunk.size() == 1));
        verify(operations, never()).execute(any(), any());
        InOrder order = inOrder(acknowledgment);
        order.verify(acknowledgment).acknowledge(1);
        order.verify(acknowledgment).acknowledge(2);
    }

    /** An invalid record in the middle splits the poll; the records around it are still batched. */
    @Test
    void invalidRecordSplitsChunks() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        RecoveryPublisher recovery = mock(RecoveryPublisher.class);
        PseudonymBatchService batches = mock(PseudonymBatchService.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        List<ConsumerRecord<byte[], byte[]>> poll = List.of(record(0, new byte[] {1}), record(1, new byte[] {2}),
                record(2, new byte[] {3}), record(3, new byte[] {4}), record(4, new byte[] {5}));
        when(mapper.map(any())).thenReturn(mapped());
        when(mapper.map(poll.get(2).value())).thenThrow(new MappingException("invalid"));
        when(batches.execute(anyList())).thenReturn(Set.of(0, 1));

        new ConnectorListener(mapper, operations, recovery, properties("CREATE_PSEUDONYM"), batches, mock(OutageCoordinator.class))
                .onMessage(poll, acknowledgment);

        verify(batches, times(2)).execute(argThat(chunk -> chunk.size() == 2));
        verify(recovery).publishDlt(eq(poll.get(2)), any(), eq("INVALID_MAPPING"), eq(0), any());
        InOrder order = inOrder(acknowledgment);
        order.verify(acknowledgment).acknowledge(1);
        order.verify(acknowledgment).acknowledge(2);
        order.verify(acknowledgment).acknowledge(4);
    }

    /** A contradicting imported pseudonym goes to the review topic, not the DLT. */
    @Test
    void publishesPseudonymConflictToReview() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        RecoveryPublisher recovery = mock(RecoveryPublisher.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ConsumerRecord<byte[], byte[]> source = record(0, new byte[] {1});
        when(mapper.map(source.value())).thenReturn(mapped());
        PseudonymConflictException conflict = new PseudonymConflictException(
                PseudonymConflictException.Reason.IDENTIFIER_HAS_OTHER_PSEUDONYM,
                new TrustDeckResponseException("rejected", HttpStatusCode.valueOf(422)));
        when(operations.execute(any(), any())).thenThrow(conflict);

        new ConnectorListener(mapper, operations, recovery, properties("UPDATE_PSEUDONYM"), mock(PseudonymBatchService.class),
                mock(OutageCoordinator.class)).onMessage(List.of(source), acknowledgment);

        verify(recovery).publishConflict(source, mapped(), conflict);
        verify(recovery, never()).publishDlt(any(), any(), any(), anyInt(), any());
        verify(operations, times(1)).execute(any(), any());
        verify(acknowledgment).acknowledge(0);
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

    /** A TrustDeck read timeout is wrapped two levels deep by the client and must still pause consumption. */
    @Test
    void pausesOnWrappedTrustDeckTimeout() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        RecoveryPublisher recovery = mock(RecoveryPublisher.class);
        ConnectorProperties properties = properties("CREATE_ENTITY");
        OutageCoordinator outage = mock(OutageCoordinator.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ConsumerRecord<byte[], byte[]> first = record(0, new byte[] {1});
        ConsumerRecord<byte[], byte[]> second = record(1, new byte[] {2});
        MappedMessage firstMapped = mapped();
        MappedMessage secondMapped = mapped();
        when(mapper.map(first.value())).thenReturn(firstMapped);
        when(mapper.map(second.value())).thenReturn(secondMapped);
        when(operations.execute(any(), eq("source/0/1"))).thenThrow(new TrustDeckClientLibraryException("TrustDeck request failed.",
                new ResourceAccessException("I/O error", new SocketTimeoutException("Read timed out"))));

        new ConnectorListener(mapper, operations, recovery, properties, mock(PseudonymBatchService.class), outage)
                .onMessage(List.of(first, second), acknowledgment);

        verify(acknowledgment).acknowledge(0);
        verify(acknowledgment).nack(1, Duration.ZERO);
        verify(outage).pause("BACKEND_UNAVAILABLE", "domain", "entity", null);
        verify(operations, times(1)).execute(any(), eq("source/0/1"));
        verify(recovery, never()).publishDlt(any(), any(), anyString(), anyInt(), any());
    }

    /** An unreachable identity provider pauses consumption instead of draining records. */
    @Test
    void pausesOnTokenAcquisitionTimeout() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        RecoveryPublisher recovery = mock(RecoveryPublisher.class);
        OutageCoordinator outage = mock(OutageCoordinator.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ConsumerRecord<byte[], byte[]> source = record(0, new byte[] {1});
        when(mapper.map(source.value())).thenReturn(mapped());
        when(operations.execute(any(), any())).thenThrow(new TrustDeckClientLibraryException("Acquiring an access token failed.",
                new TokenAcquisitionException("OAuth token acquisition failed.", new HttpTimeoutException("request timed out"))));

        new ConnectorListener(mapper, operations, recovery, properties("CREATE_ENTITY"), mock(PseudonymBatchService.class), outage)
                .onMessage(List.of(source), acknowledgment);

        verify(outage).pause("BACKEND_UNAVAILABLE", "domain", "entity", null);
        verify(acknowledgment).nack(0, Duration.ZERO);
        verify(recovery, never()).publishDlt(any(), any(), anyString(), anyInt(), any());
    }

    /** A systemic failure of a pseudonym batch redelivers the whole poll. */
    @Test
    void redeliversWholeBatchOnSystemicBatchFailure() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        RecoveryPublisher recovery = mock(RecoveryPublisher.class);
        PseudonymBatchService batches = mock(PseudonymBatchService.class);
        OutageCoordinator outage = mock(OutageCoordinator.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ConsumerRecord<byte[], byte[]> first = record(0, new byte[] {1});
        ConsumerRecord<byte[], byte[]> second = record(1, new byte[] {2});
        when(mapper.map(first.value())).thenReturn(mapped());
        when(mapper.map(second.value())).thenReturn(mapped());
        doThrow(new TrustDeckResponseException("unavailable", HttpStatusCode.valueOf(503))).when(batches).execute(anyList());

        new ConnectorListener(mapper, mock(OperationService.class), recovery, properties("CREATE_PSEUDONYM"), batches, outage)
                .onMessage(List.of(first, second), acknowledgment);

        verify(outage).pause("BACKEND_UNAVAILABLE", "domain", "entity", null);
        verify(acknowledgment).nack(0, Duration.ZERO);
        verify(acknowledgment, never()).acknowledge();
        verify(recovery, never()).publishDlt(any(), any(), anyString(), anyInt(), any());
    }

    /** Once the batch budget is spent, later records are redelivered rather than processed or dead-lettered. */
    @Test
    void redeliversRemainingRecordsWhenBudgetIsExhausted() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        RecoveryPublisher recovery = mock(RecoveryPublisher.class);
        ConnectorProperties properties = properties("CREATE_ENTITY");
        properties.getRecovery().setBatchTimeBudget(Duration.ofNanos(1));
        OutageCoordinator outage = mock(OutageCoordinator.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ConsumerRecord<byte[], byte[]> first = record(0, new byte[] {1});
        ConsumerRecord<byte[], byte[]> second = record(1, new byte[] {2});
        when(mapper.map(any())).thenReturn(mapped());

        new ConnectorListener(mapper, operations, recovery, properties, mock(PseudonymBatchService.class), outage)
                .onMessage(List.of(first, second), acknowledgment);

        verify(operations, times(1)).execute(any(), eq("source/0/0"));
        verify(operations, never()).execute(any(), eq("source/0/1"));
        verify(acknowledgment).acknowledge(0);
        verify(acknowledgment).nack(1, Duration.ZERO);
        verify(outage, never()).pause(anyString(), any(), any(), any());
    }

    /** The first record of a poll always completes its retries so every poll makes progress. */
    @Test
    void firstRecordIsExemptFromBudget() throws Exception {
        JsonMessageMapper mapper = mock(JsonMessageMapper.class);
        OperationService operations = mock(OperationService.class);
        RecoveryPublisher recovery = mock(RecoveryPublisher.class);
        ConnectorProperties properties = properties("CREATE_ENTITY");
        properties.getRecovery().setBatchTimeBudget(Duration.ofNanos(1));
        properties.getRecovery().setMaxAttempts(1);
        properties.getRecovery().setInitialBackoff(Duration.ZERO);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ConsumerRecord<byte[], byte[]> source = record(0, new byte[] {1});
        when(mapper.map(source.value())).thenReturn(mapped());
        when(operations.execute(any(), any())).thenThrow(new IllegalStateException("record-local failure"));

        new ConnectorListener(mapper, operations, recovery, properties, mock(PseudonymBatchService.class), mock(OutageCoordinator.class))
                .onMessage(List.of(source), acknowledgment);

        verify(operations, times(2)).execute(any(), any());
        verify(recovery).publishDlt(eq(source), any(), eq("TRANSIENT_OPERATION_FAILURE"), eq(1), any(IllegalStateException.class));
        verify(acknowledgment).acknowledge(0);
        verify(acknowledgment, never()).nack(anyInt(), any());
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

    private ConsumerRecord<byte[], byte[]> record(int partition, long offset, byte[] value) {
        return new ConsumerRecord<>("source", partition, offset, new byte[] {(byte) offset}, value);
    }

    private MappedMessage mapped() {
        return new MappedMessage("domain", "identifier", "type", "psn", null, null,
                null, "entity", null, null, null, null, null, null, null, null, null, null);
    }
}
