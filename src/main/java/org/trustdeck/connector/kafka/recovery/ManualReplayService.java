/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.recovery;

import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.trustdeck.connector.kafka.config.ConnectorProperties;
import org.trustdeck.connector.kafka.mapping.JsonMessageMapper;
import org.trustdeck.connector.kafka.mapping.MappedMessage;
import org.trustdeck.connector.kafka.operation.OperationService;

/**
 * Validates and executes explicitly initiated recovery replays.
 *
 * The service only replays recovery envelopes that match the currently configured
 * connector operation and contain a confirmed failed source record. Preserved
 * source bytes and resolved TrustDeck targets are validated before the mapped
 * operation is executed again.
 *
 * @author Armin Müller
 */
@Service
public class ManualReplayService {

	/** Maps preserved Kafka source bytes back to connector messages. */
	private final JsonMessageMapper mapper;

	/** Executes the configured TrustDeck operation for replayed messages. */
	private final OperationService operationService;

	/** Connector configuration used to validate replay metadata and targets. */
	private final ConnectorProperties properties;

	/**
	 * Creates the manual replay service.
	 *
	 * @param mapper source message mapper
	 * @param operations TrustDeck operation dispatcher
	 * @param properties connector configuration
	 */
	public ManualReplayService(JsonMessageMapper mapper, OperationService operations, ConnectorProperties properties) {
		this.mapper = mapper;
		this.operationService = operations;
		this.properties = properties;
	}

	/**
	 * Replays a confirmed failed record after verifying its externally supplied source value.
	 *
	 * The supplied bytes must exactly match the source value preserved in the
	 * recovery envelope before the replay is allowed to continue.
	 *
	 * @param envelope recovery metadata describing the failed source record
	 * @param originalValue original source JSON bytes supplied for replay
	 * @return TrustDeck operation result
	 * @throws IllegalArgumentException if the envelope is not replayable or the supplied bytes do not match the preserved value
	 */
	public Object replay(RecoveryEnvelope envelope, byte[] originalValue) {
		validate(envelope);

		byte[] preservedValue = decodeValue(envelope);
		if (!Arrays.equals(preservedValue, originalValue)) {
			throw new IllegalArgumentException("Replay payload does not match the preserved source value.");
		}

		return replayValidated(envelope, preservedValue);
	}

	/**
	 * Replays a confirmed failed record directly from the byte-preserving recovery envelope.
	 *
	 * This overload allows replay after a process restart because the original
	 * Kafka value is reconstructed from the envelope itself.
	 *
	 * @param envelope recovery envelope containing replay metadata and source bytes
	 * @return TrustDeck operation result
	 * @throws IllegalArgumentException if the envelope is not replayable or its preserved source value is invalid
	 */
	public Object replay(RecoveryEnvelope envelope) {
		validate(envelope);
		return replayValidated(envelope, decodeValue(envelope));
	}

	/**
	 * Executes a replay after envelope validation and source-value reconstruction.
	 *
	 * The mapped dynamic targets must still match the targets recorded in the
	 * recovery envelope. Combined entity-and-pseudonym operationService also restore the
	 * confirmed completed entity stage before replaying the remaining operation.
	 *
	 * @param envelope validated recovery envelope
	 * @param originalValue preserved original source bytes
	 * @return TrustDeck operation result
	 * @throws IllegalArgumentException if replayed targets do not match the recovery metadata or completed-stage metadata is invalid
	 */
	private Object replayValidated(RecoveryEnvelope envelope, byte[] originalValue) {
		MappedMessage mapped = mapper.map(originalValue);

		// Prevent replay when current mapping resolves to different TrustDeck targets
		if (envelope.resolvedTarget() == null || !envelope.resolvedTarget().equals(mapped.domain())
				|| (envelope.targetProject() != null && !envelope.targetProject().equals(properties.getTrustdeck().getProject()))
				|| (envelope.targetEntityType() != null && !envelope.targetEntityType().equals(mapped.entityType()))) {
			throw new IllegalArgumentException("Recovery target does not match the replayed mapping.");
		}

		String correlation = envelope.sourceTopic() + "/" + envelope.partition() + "/" + envelope.offset();

		// Restore the confirmed entity stage so combined replay resumes after entity creation
		if (properties.getOperation().equals("CREATE_ENTITY_AND_PSEUDONYM")) {
			operationService.restoreCompletedUuid(correlation, UUID.fromString(envelope.completedUuid()));
		}

		return operationService.execute(mapped, correlation);
	}

	/**
	 * Validates whether a recovery envelope is eligible for explicit replay.
	 *
	 * The envelope must use a supported schema version, match the configured
	 * operation, represent a confirmed failed DLT record, contain complete source
	 * identity and preserved data, and satisfy operation-specific replay metadata.
	 *
	 * @param envelope recovery envelope to validate
	 * @throws IllegalArgumentException if the envelope is incomplete, incompatible, or represents an outcome requiring reconciliation
	 */
	private void validate(RecoveryEnvelope envelope) {
		if (envelope == null
				|| (!"1".equals(envelope.schemaVersion()) && !"2".equals(envelope.schemaVersion()))
				|| !properties.getOperation().equals(envelope.operation())) {
			throw new IllegalArgumentException("Recovery envelope version or operation is not replayable.");
		}

		if (!"FAILED_DLT".equals(envelope.status())) {
			throw new IllegalArgumentException("Only confirmed FAILED_DLT records are explicitly replayable.");
		}

		// Ambiguous outcomes must be reconciled manually before any replay is attempted
		if ("UNKNOWN_OUTCOME".equals(envelope.failureCategory())
				|| "PENDING_LINKAGE_REVIEW".equals(envelope.status())) {
			throw new IllegalArgumentException("Unknown outcomes and linkage reviews require reconciliation first.");
		}

		// Combined replay is only safe after confirmed completion of the entity stage
		if (properties.getOperation().equals("CREATE_ENTITY_AND_PSEUDONYM")
				&& (!"ENTITY_CREATED".equals(envelope.completedStage()) || envelope.completedUuid() == null)) {
			throw new IllegalArgumentException("Combined replay requires the confirmed entity stage and UUID.");
		}

		if (envelope.sourceTopic() == null || envelope.sourceTopic().isBlank()
				|| envelope.partition() < 0 || envelope.offset() < 0
				|| envelope.valueBase64() == null) {
			throw new IllegalArgumentException("Recovery envelope source identity and preserved value are required.");
		}

		// Schema version 2 requires the original project context for entity operationService
		if (envelope.schemaVersion().equals("2") && envelope.targetProject() == null && properties.getOperation().contains("ENTITY")) {
			throw new IllegalArgumentException("Schema-2 entity replay requires its target project.");
		}
	}

	/**
	 * Decodes the original Kafka value preserved in the recovery envelope.
	 *
	 * @param envelope recovery envelope containing the Base64-encoded source value
	 * @return decoded original source bytes
	 * @throws IllegalArgumentException if the preserved source value is not valid Base64
	 */
	private byte[] decodeValue(RecoveryEnvelope envelope) {
		try {
			return Base64.getDecoder().decode(envelope.valueBase64());
		} catch (IllegalArgumentException exception) {
			throw new IllegalArgumentException(
					"Recovery envelope contains invalid source value encoding.", exception);
		}
	}
}
