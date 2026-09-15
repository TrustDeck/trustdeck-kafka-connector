/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** Verifies operation-aware startup validation without contacting external services. */
class ConfigurationValidationTest {

    /** Pseudonym-only setup accepts a constant domain without entity settings. */
    @Test
    void pseudonymConstantDomainDoesNotNeedEntitySettings() {
        ConnectorProperties properties = base("CREATE_PSEUDONYM");
        properties.getMapping().getCommon().setDomainConstant("synthetic-domain");
        properties.getMapping().getCommon().setIdentifierPath("/identifier");
        properties.getMapping().getCommon().setIdTypePath("/idType");

        assertDoesNotThrow(() -> new ConfigurationValidation(properties));
    }

    /** Identifier-only deletion requires both selector components. */
    @Test
    void deleteRequiresIdentifierTypeWhenPsnIsAbsent() {
        ConnectorProperties properties = base("DELETE_PSEUDONYM");
        properties.getMapping().getCommon().setIdentifierPath("/identifier");

        assertThrows(IllegalArgumentException.class, () -> new ConfigurationValidation(properties));
    }

    /** Invalid endpoint schemes fail before any client bean is created. */
    @Test
    void invalidEndpointIsRejected() {
        ConnectorProperties properties = base("CREATE_PSEUDONYM");
        properties.getTrustdeck().setServiceUrl("file:///tmp/backend");

        assertThrows(IllegalArgumentException.class, () -> new ConfigurationValidation(properties));
    }

    /** Entity operations require their fixed project target. */
    @Test
    void entityOperationRequiresProject() {
        ConnectorProperties properties = base("CREATE_ENTITY");
        properties.getMapping().getCommon().setEntityTypePath("/entityType");

        assertThrows(IllegalArgumentException.class, () -> new ConfigurationValidation(properties));
    }

    /** Rejects empty JSON Pointer segments in explicit entity targets. */
    @Test
    void entityTargetPointerMustBeNonEmpty() {
        ConnectorProperties properties = base("CREATE_ENTITY");
        properties.getTrustdeck().setProject("project");
        properties.getTrustdeck().setEntityType("type");
        ConnectorProperties.FieldMapping field = new ConnectorProperties.FieldMapping();
        field.setSourcePath("/source");
        field.setTargetPath("/data/");
        properties.getMapping().getCommon().setEntityFields(java.util.List.of(field));

        assertThrows(IllegalArgumentException.class, () -> new ConfigurationValidation(properties));
    }

    private ConnectorProperties base(String operation) {
        ConnectorProperties properties = new ConnectorProperties();
        properties.setOperation(operation);
        properties.setSourceTopic("source");
        properties.setGroupId("group");
        properties.getTrustdeck().setServiceUrl("https://backend.example.test");
        properties.getTrustdeck().setTokenEndpoint("https://identity.example.test/token");
        return properties;
    }
}
