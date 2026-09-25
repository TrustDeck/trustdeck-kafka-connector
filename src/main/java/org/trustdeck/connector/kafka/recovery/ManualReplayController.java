/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.kafka;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Explicit, disabled-by-default operator entry point for one guarded replay. */
@RestController
@RequestMapping("/internal/replay")
@ConditionalOnProperty(name = "connector.replay.enabled", havingValue = "true")
public class ManualReplayController {

    private final ManualReplayService replay;

    /** Creates the replay controller.
     * @param replay guarded replay service
     */
    public ManualReplayController(ManualReplayService replay) {
        this.replay = replay;
    }

    /** Replays exactly the source value preserved by one recovery envelope.
     * @param envelope recovery envelope
     * @return operation result
     */
    @PostMapping
    public ResponseEntity<Object> replay(@RequestBody RecoveryEnvelope envelope) {
        return ResponseEntity.ok(replay.replay(envelope));
    }
}
