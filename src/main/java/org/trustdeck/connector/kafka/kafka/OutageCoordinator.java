/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.kafka;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.trustdeck.connector.kafka.notification.NotificationService;
import org.trustdeck.connector.kafka.trustdeck.TrustDeckPreflight;

/** Pauses source containers during systemic backend failures and probes for recovery. */
@Component
public class OutageCoordinator {

    private final OutageState state;
    private final KafkaListenerEndpointRegistry registry;
    private final TrustDeckPreflight preflight;
    private final NotificationService notifications;

    /** Creates the outage coordinator.
     * @param state shared outage state
     * @param registry listener containers
     * @param preflight authenticated resource preflight
     * @param notifications optional SMTP notifications
     */
    public OutageCoordinator(OutageState state, KafkaListenerEndpointRegistry registry,
                             TrustDeckPreflight preflight,
                             NotificationService notifications) {
        this.state = state;
        this.registry = registry;
        this.preflight = preflight;
        this.notifications = notifications;
    }

    /** Pauses every source container without acknowledging the current record.
     * @param reason sanitized outage category
     */
    public void pause(String reason) {
        boolean alreadyPaused = state.isPaused();
        state.setPaused(true, reason);
        registry.getListenerContainers().forEach(container -> {
            if (!container.isContainerPaused()) {
                container.pause();
            }
        });
        if (!alreadyPaused) {
            notifications.send("TrustDeck outage", "Consumption paused: " + reason);
        }
    }

    /** Probes a configured read-only resource and resumes paused containers on success. */
    @Scheduled(fixedDelayString = "${connector.recovery.probe-interval:15s}")
    public void probeAndResume() {
        if (!state.isPaused()) {
            return;
        }
        try {
            preflight.verifyStaticResources();
            registry.getListenerContainers().forEach(container -> {
                if (container.isContainerPaused()) {
                    container.resume();
                }
            });
            state.setPaused(false, "");
            notifications.send("TrustDeck recovered", "Authenticated read-only probe succeeded; consumption resumed.");
        } catch (RuntimeException ignored) {
            // Keep the source paused until an authenticated read-only probe succeeds.
        }
    }
}
