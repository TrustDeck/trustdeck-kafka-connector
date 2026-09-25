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
    public synchronized void pause(String reason) {
        pause(reason, null, null);
    }

    /** Pauses consumption and retains the dynamic target that caused the outage.
     * @param reason sanitized outage category
     * @param domain failed dynamic domain
     * @param entityType failed dynamic entity type
     */
    public synchronized void pause(String reason, String domain, String entityType) {
        pause(reason, domain, entityType, null);
    }

    /** Pauses consumption and retains dynamic targets and project identity for probing.
     * @param reason sanitized outage category
     * @param domain failed dynamic domain
     * @param entityType failed dynamic entity type
     * @param project failed dynamic entity project
     */
    public synchronized void pause(String reason, String domain, String entityType, String project) {
        boolean alreadyPaused = state.isPaused();
        state.setPaused(true, reason);
        failedDomain = domain;
        failedEntityType = entityType;
        failedProject = project;
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
    public synchronized void probeAndResume() {
        if (!state.isPaused()) {
            return;
        }
        try {
            preflight.verifyStaticResources();
            if (failedDomain != null) {
                preflight.verifyDomain(failedDomain);
            }
            if (failedEntityType != null) {
                preflight.verifyEntityType(failedProject, failedEntityType);
            }
            registry.getListenerContainers().forEach(container -> {
                if (container.isContainerPaused()) {
                    container.resume();
                }
            });
            state.setPaused(false, "");
            failedDomain = null;
            failedEntityType = null;
            failedProject = null;
            notifications.send("TrustDeck recovered", "Authenticated read-only probe succeeded; consumption resumed.");
        } catch (RuntimeException ignored) {
            // Keep the source paused until an authenticated read-only probe succeeds.
        }
    }

    /** Last dynamic domain that failed, if any. */
    private volatile String failedDomain;

    /** Last dynamic entity type that failed, if any. */
    private volatile String failedEntityType;

    /** Project associated with the failed dynamic entity-type probe. */
    private volatile String failedProject;
}
