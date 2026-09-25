/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.kafka;

import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.trustdeck.connector.kafka.config.ConnectorProperties;
import org.trustdeck.connector.kafka.mapping.JsonMessageMapper;
import org.trustdeck.connector.kafka.mapping.MappedMessage;
import org.trustdeck.connector.kafka.operation.OperationService;

/** Performs one operator-initiated replay after validating a recovery envelope. */
@Service
public class ManualReplayService {

    private final JsonMessageMapper mapper;
    private final OperationService operations;
    private final ConnectorProperties properties;

    /** Creates the replay service.
     * @param mapper source mapper
     * @param operations operation dispatcher
     * @param properties connector settings
     */
    public ManualReplayService(JsonMessageMapper mapper, OperationService operations, ConnectorProperties properties) {
        this.mapper = mapper;
        this.operations = operations;
        this.properties = properties;
    }

    /**
     * Replays a confirmed failed record; this method is intentionally not connected to a Kafka listener.
     *
     * @param envelope validated recovery metadata
     * @param originalValue original source JSON bytes
     * @return TrustDeck operation result
     */
    public Object replay(RecoveryEnvelope envelope, byte[] originalValue) {
        validate(envelope);
        byte[] preservedValue = decodeValue(envelope);
        if (!Arrays.equals(preservedValue, originalValue)) {
            throw new IllegalArgumentException("Replay payload does not match the preserved source value.");
        }
        return replayValidated(envelope, preservedValue);
    }

    /** Replays from the byte-preserving envelope after a process restart.
     * @param envelope recovery envelope
     * @return TrustDeck operation result
     */
    public Object replay(RecoveryEnvelope envelope) {
        validate(envelope);
        return replayValidated(envelope, decodeValue(envelope));
    }

    /** Executes one already validated replay using the envelope's preserved source bytes. */
    private Object replayValidated(RecoveryEnvelope envelope, byte[] originalValue) {
        MappedMessage mapped = mapper.map(originalValue);
        if (envelope.resolvedTarget() == null || !envelope.resolvedTarget().equals(mapped.domain())
                || (envelope.targetProject() != null
                && !envelope.targetProject().equals(properties.getTrustdeck().getProject()))
                || (envelope.targetEntityType() != null
                && !envelope.targetEntityType().equals(mapped.entityType()))) {
            throw new IllegalArgumentException("Recovery target does not match the replayed mapping.");
        }
        String correlation = envelope.sourceTopic() + "/" + envelope.partition() + "/" + envelope.offset();
        if (properties.getOperation().equals("CREATE_ENTITY_AND_PSEUDONYM")) {
            operations.restoreCompletedUuid(correlation, UUID.fromString(envelope.completedUuid()));
        }
        return operations.execute(mapped, correlation);
    }

    private void validate(RecoveryEnvelope envelope) {
        if (envelope == null || (!"1".equals(envelope.schemaVersion()) && !"2".equals(envelope.schemaVersion()))
                || !properties.getOperation().equals(envelope.operation())) {
            throw new IllegalArgumentException("Recovery envelope version or operation is not replayable.");
        }
        if (!"FAILED_DLT".equals(envelope.status())) {
            throw new IllegalArgumentException("Only confirmed FAILED_DLT records are explicitly replayable.");
        }
        if ("UNKNOWN_OUTCOME".equals(envelope.failureCategory()) || "PENDING_LINKAGE_REVIEW".equals(envelope.status())) {
            throw new IllegalArgumentException("Unknown outcomes and linkage reviews require reconciliation first.");
        }
        if (properties.getOperation().equals("CREATE_ENTITY_AND_PSEUDONYM")
                && (!"ENTITY_CREATED".equals(envelope.completedStage()) || envelope.completedUuid() == null)) {
            throw new IllegalArgumentException("Combined replay requires the confirmed entity stage and UUID.");
        }
        if (envelope.sourceTopic() == null || envelope.sourceTopic().isBlank() || envelope.partition() < 0
                || envelope.offset() < 0 || envelope.valueBase64() == null) {
            throw new IllegalArgumentException("Recovery envelope source identity and preserved value are required.");
        }
        if (envelope.schemaVersion().equals("2") && envelope.targetProject() == null
                && properties.getOperation().contains("ENTITY")) {
            throw new IllegalArgumentException("Schema-2 entity replay requires its target project.");
        }
    }

    /** Decodes the original source value and rejects malformed operator input. */
    private byte[] decodeValue(RecoveryEnvelope envelope) {
        try {
            return Base64.getDecoder().decode(envelope.valueBase64());
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Recovery envelope contains invalid source value encoding.", exception);
        }
    }
}
