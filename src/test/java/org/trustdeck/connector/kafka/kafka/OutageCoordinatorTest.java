/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.kafka;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
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
        OutageCoordinator coordinator = new OutageCoordinator(state, registry, preflight, notifications, Duration.ZERO);

        coordinator.pause("BACKEND_UNAVAILABLE", "dynamic-domain", "dynamic-type", "project");
        coordinator.probeAndResume();

        verify(preflight).verifyStaticResources();
        verify(preflight).verifyDomain("dynamic-domain");
        verify(preflight).verifyEntityType("project", "dynamic-type");
        verify(container).resume();
    }

    /**
     * A probe that runs before the consumer threads applied the pause must still resume:
     * pause() only requests the pause, and isContainerPaused() stays false until all threads paused.
     */
    @Test
    void resumesPauseThatHasNotTakenEffectYet() {
        OutageState state = new OutageState();
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        when(registry.getListenerContainers()).thenReturn(List.of(container));
        when(container.isPauseRequested()).thenReturn(false, true);
        when(container.isContainerPaused()).thenReturn(false);
        OutageCoordinator coordinator = new OutageCoordinator(state, registry, mock(TrustDeckPreflight.class),
                mock(NotificationService.class), Duration.ZERO);

        coordinator.pause("BACKEND_UNAVAILABLE", "ishmed", null, "project");
        coordinator.probeAndResume();

        verify(container).pause();
        verify(container).resume();
        assertFalse(state.isPaused());
    }

    /** A probe within the minimum pause does nothing, so a slow backend gets time to recover. */
    @Test
    void keepsMinimumPauseBeforeProbing() {
        OutageState state = new OutageState();
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        TrustDeckPreflight preflight = mock(TrustDeckPreflight.class);
        when(registry.getListenerContainers()).thenReturn(List.of(container));
        when(container.isPauseRequested()).thenReturn(true);
        OutageCoordinator coordinator = new OutageCoordinator(state, registry, preflight,
                mock(NotificationService.class), Duration.ofMinutes(1));

        coordinator.pause("BACKEND_UNAVAILABLE", "ishmed", null, "project");
        coordinator.probeAndResume();

        verify(preflight, never()).verifyStaticResources();
        verify(container, never()).resume();
        assertTrue(state.isPaused());
    }
}
