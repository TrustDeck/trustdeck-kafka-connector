/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.recovery;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Exposes the guarded manual replay endpoint for recovery envelopes.
 *
 * The controller is only created when manual replay is explicitly enabled.
 * Incoming recovery envelopes are delegated to {@link ManualReplayService},
 * which performs the actual validation and replay of the preserved source value.
 *
 * @author Armin Müller
 */
@RestController
@RequestMapping("/internal/replay")
@ConditionalOnProperty(name = "connector.replay.enabled", havingValue = "true")
public class ManualReplayController {

	/** Guarded service responsible for validating and executing manual replays. */
	private final ManualReplayService replay;

	/**
	 * Creates the manual replay controller.
	 *
	 * @param replay guarded replay service
	 */
	public ManualReplayController(ManualReplayService replay) {
		this.replay = replay;
	}

	/**
	 * Replays the original source value preserved in a recovery envelope.
	 *
	 * The envelope is passed unchanged to the replay service so that the
	 * preserved source data and replay metadata can be validated before the
	 * configured connector operation is executed again.
	 *
	 * @param envelope recovery envelope containing the preserved source record
	 * @return successful response containing the replayed operation result
	 */
	@PostMapping
	public ResponseEntity<Object> replay(@RequestBody RecoveryEnvelope envelope) {
		return ResponseEntity.ok(replay.replay(envelope));
	}
}
