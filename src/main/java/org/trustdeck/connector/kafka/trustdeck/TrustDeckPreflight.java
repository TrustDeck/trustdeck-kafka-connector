/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package org.trustdeck.connector.kafka.trustdeck;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;
import org.trustdeck.client.TrustDeckClient;
import org.trustdeck.connector.kafka.config.ConnectorProperties;

/** Performs authenticated static and bounded dynamic TrustDeck resource checks. */
@Component
public class TrustDeckPreflight {

    /** Lifetime of successful dynamic resource checks. */
    private static final Duration CACHE_TTL = Duration.ofMinutes(5);

    /** Shared authenticated client. */
    private final TrustDeckClient client;

    /** Connector settings. */
    private final ConnectorProperties properties;

    /** Expiring successful resource checks. */
    private final ConcurrentMap<String, Instant> verified = new ConcurrentHashMap<>();

    /** Whether authenticated preflight is enabled for this instance. */
    private final boolean enabled;

    /** Creates the authenticated preflight checker.
     * @param client TrustDeck client
     * @param properties connector settings
     * @param enabled whether checks are enabled
     */
    public TrustDeckPreflight(TrustDeckClient client, ConnectorProperties properties,
                              @Value("${connector.preflight.enabled:true}") boolean enabled) {
        this.client = client;
        this.properties = properties;
        this.enabled = enabled;
    }

    /** Verifies configured static resources after authentication is available. */
    @PostConstruct
    public void verifyStaticResources() {
        if (!enabled) {
            return;
        }
        String operation = properties.getOperation().toUpperCase(Locale.ROOT);
        if (requiresEntity(operation)) {
            verifyProject(properties.getTrustdeck().getProject());
            if (!blank(properties.getTrustdeck().getEntityType())) {
                verifyEntityType(properties.getTrustdeck().getProject(), properties.getTrustdeck().getEntityType());
            }
        }
        if (requiresDomain(operation) && !blank(properties.getMapping().getDomainConstant())) {
            verifyDomain(properties.getMapping().getDomainConstant());
        }
    }

    /** Verifies a dynamic domain once per bounded cache interval.
     * @param domain domain name
     */
    public void verifyDomain(String domain) {
        if (!enabled) {
            return;
        }
        if (blank(domain)) {
            throw new IllegalArgumentException("A dynamic TrustDeck domain is required.");
        }
        verify("domain:" + domain, () -> client.domains().get(domain));
    }

    /** Verifies a dynamic entity type once per bounded cache interval.
     * @param project project name
     * @param type entity type name
     */
    public void verifyEntityType(String project, String type) {
        if (!enabled) {
            return;
        }
        if (blank(project) || blank(type)) {
            throw new IllegalArgumentException("A TrustDeck project and entity type are required.");
        }
        verify("type:" + project + ":" + type, () -> client.entityTypes(project).get(type));
    }

    /** Verifies a project once per bounded cache interval. */
    private void verifyProject(String project) {
        if (blank(project)) {
            throw new IllegalArgumentException("A TrustDeck project is required.");
        }
        verify("project:" + project, () -> client.projects().get(project));
    }

    /** Executes one authenticated check unless a recent successful result exists. */
    private void verify(String key, Runnable check) {
        Instant now = Instant.now();
        Instant previous = verified.get(key);
        if (previous != null && previous.plus(CACHE_TTL).isAfter(now)) {
            return;
        }
        check.run();
        verified.put(key, now);
    }

    /** Returns whether the operation needs a pseudonym domain. */
    private boolean requiresDomain(String operation) {
        return operation.equals("CREATE_PSEUDONYM") || operation.equals("UPDATE_PSEUDONYM")
                || operation.equals("DELETE_PSEUDONYM") || operation.equals("CREATE_ENTITY_AND_PSEUDONYM");
    }

    /** Returns whether the operation needs a project and entity type. */
    private boolean requiresEntity(String operation) {
        return operation.equals("CREATE_ENTITY") || operation.equals("UPDATE_ENTITY")
                || operation.equals("DELETE_ENTITY") || operation.equals("CREATE_ENTITY_AND_PSEUDONYM");
    }

    /** Tests whether a value is absent. */
    private boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
