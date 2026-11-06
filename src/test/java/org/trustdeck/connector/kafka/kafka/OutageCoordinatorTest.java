/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.kafka;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.trustdeck.connector.kafka.health.OutageCoordinator;
import org.trustdeck.connector.kafka.health.OutageState;
import org.trustdeck.connector.kafka.notification.NotificationService;
import org.trustdeck.connector.kafka.trustdeck.TrustDeckPreflight;

/** Verifies outage pause and target-aware authenticated recovery probing. */
class OutageCoordinatorTest {

    /** Probes the failed dynamic resources before resuming source containers. */
    @Test
    void probesFailedDynamicTargetsBeforeResume() {
        OutageState state = new OutageState();
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        TrustDeckPreflight preflight = mock(TrustDeckPreflight.class);
        NotificationService notifications = mock(NotificationService.class);
        when(registry.getListenerContainers()).thenReturn(List.of(container));
        when(container.isContainerPaused()).thenReturn(true);
        OutageCoordinator coordinator = new OutageCoordinator(state, registry, preflight, notifications);

        coordinator.pause("BACKEND_UNAVAILABLE", "dynamic-domain", "dynamic-type", "project");
        coordinator.probeAndResume();

        verify(preflight).verifyStaticResources();
        verify(preflight).verifyDomain("dynamic-domain");
        verify(preflight).verifyEntityType("project", "dynamic-type");
        verify(container).resume();
    }
}
