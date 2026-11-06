/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.health;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.trustdeck.connector.kafka.notification.NotificationService;
import org.trustdeck.connector.kafka.trustdeck.TrustDeckPreflight;

/**
 * Coordinates connector behavior during temporary TrustDeck outages.
 *
 * When an outage is detected, source consumption is paused without acknowledging
 * the current record. The coordinator retains any dynamic TrustDeck resources
 * involved in the failure and periodically probes them together with the static
 * configuration. Consumption resumes once all required checks succeed.
 * 
 * @author Armin Müller
 */
@Component
public class OutageCoordinator {

	/** Shared outage state exposed to other connector components. */
    private final OutageState state;
    
    /** Registry containing the active Kafka listener containers. */
    private final KafkaListenerEndpointRegistry registry;
    
    /** Performs authenticated read-only checks against configured TrustDeck resources. */
    private final TrustDeckPreflight preflight;
    
    /** Sends optional notifications when consumption is paused or resumed. */
    private final NotificationService notifications;

    /** Last dynamic domain involved in the outage, if any. */
    private volatile String failedDomain;

    /** Last dynamic entity type involved in the outage, if any. */
    private volatile String failedEntityType;

    /** Project associated with the retained dynamic entity type, if any. */
    private volatile String failedProject;

    /**
	 * Creates the outage coordinator.
	 *
	 * @param state shared outage state
	 * @param registry registry containing the Kafka listener containers
	 * @param preflight authenticated TrustDeck resource preflight
	 * @param notifications notification service for outage and recovery messages
	 */
    public OutageCoordinator(OutageState state, KafkaListenerEndpointRegistry registry, TrustDeckPreflight preflight, NotificationService notifications) {
        this.state = state;
        this.registry = registry;
        this.preflight = preflight;
        this.notifications = notifications;
    }

    /**
	 * Pauses all source listener containers because of a detected outage.
	 *
	 * No dynamic TrustDeck resource is retained for the subsequent recovery probe.
	 * The current record remains unacknowledged so it can be processed again after recovery.
	 *
	 * @param reason sanitized outage category
	 */
    public synchronized void pause(String reason) {
        pause(reason, null, null);
    }

    /**
	 * Pauses all source listener containers and retains the dynamic TrustDeck
	 * resources involved in the failure.
	 *
	 * The retained domain and entity type are verified during subsequent recovery
	 * probes before consumption is resumed.
	 *
	 * @param reason sanitized outage category
	 * @param domain failed dynamic domain, or {@code null} if none
	 * @param entityType failed dynamic entity type, or {@code null} if none
	 */
    public synchronized void pause(String reason, String domain, String entityType) {
        pause(reason, domain, entityType, null);
    }

    /**
	 * Pauses all source listener containers and retains the dynamic TrustDeck
	 * resources required for the recovery probe.
	 *
	 * The outage notification is sent only when transitioning from running to
	 * paused state. Repeated failures while already paused update the retained
	 * resource information without sending another notification.
	 *
	 * @param reason sanitized outage category
	 * @param domain failed dynamic domain, or {@code null} if none
	 * @param entityType failed dynamic entity type, or {@code null} if none
	 * @param project project associated with the failed entity type, or {@code null} if none
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

    /**
	 * Probes the required TrustDeck resources and resumes source consumption after
	 * successful recovery.
	 *
	 * Static resources are always verified first. Any dynamic domain or entity type
	 * retained from the failure is then checked as well. If any probe fails, the
	 * connector remains paused and another attempt is made after the configured
	 * probe interval.
	 */
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
            // Keep the source paused until an authenticated read-only probe succeeds
        }
    }
}
