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
import org.trustdeck.client.TrustDeckClient;
import org.trustdeck.connector.kafka.config.ConnectorProperties;

/** Pauses source containers during systemic backend failures and probes for recovery. */
@Component
public class OutageCoordinator {

    private final OutageState state;
    private final KafkaListenerEndpointRegistry registry;
    private final TrustDeckClient client;
    private final ConnectorProperties properties;

    /** Creates the outage coordinator.
     * @param state shared outage state
     * @param registry listener containers
     * @param client authenticated client
     * @param properties connector settings
     */
    public OutageCoordinator(OutageState state, KafkaListenerEndpointRegistry registry,
                             TrustDeckClient client, ConnectorProperties properties) {
        this.state = state;
        this.registry = registry;
        this.client = client;
        this.properties = properties;
    }

    /** Pauses every source container without acknowledging the current record.
     * @param reason sanitized outage category
     */
    public void pause(String reason) {
        state.setPaused(true, reason);
        registry.getListenerContainers().forEach(container -> {
            if (!container.isContainerPaused()) {
                container.pause();
            }
        });
    }

    /** Probes a configured read-only resource and resumes paused containers on success. */
    @Scheduled(fixedDelayString = "${connector.recovery.probe-interval:15s}")
    public void probeAndResume() {
        if (!state.isPaused()) {
            return;
        }
        try {
            String project = properties.getTrustdeck().getProject();
            if (project != null && !project.isBlank()) {
                client.projects().get(project);
            } else {
                String domain = properties.getMapping().getDomainConstant();
                if (domain == null || domain.isBlank()) {
                    return;
                }
                client.domains().get(domain);
            }
            registry.getListenerContainers().forEach(container -> {
                if (container.isContainerPaused()) {
                    container.resume();
                }
            });
            state.setPaused(false, "");
        } catch (RuntimeException ignored) {
            // Keep the source paused until an authenticated read-only probe succeeds.
        }
    }
}
