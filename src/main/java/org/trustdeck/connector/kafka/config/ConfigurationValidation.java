/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.trustdeck.connector.kafka.config;

import java.util.Locale;
import java.util.Set;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** Performs operation-aware startup validation after property binding. */
@Configuration
@EnableConfigurationProperties(ConnectorProperties.class)
public class ConfigurationValidation {

    /** Supported fixed operation names. */
    private static final Set<String> OPERATIONS = Set.of("CREATE_PSEUDONYM", "UPDATE_PSEUDONYM", "DELETE_PSEUDONYM",
            "CREATE_ENTITY", "UPDATE_ENTITY", "DELETE_ENTITY", "CREATE_ENTITY_AND_PSEUDONYM");

    /**
     * Validates mappings that are required only by the selected operation.
     *
     * @param properties bound connector properties
     */
    public ConfigurationValidation(ConnectorProperties properties) {
        if (blank(properties.getOperation())) {
            throw new IllegalArgumentException("connector.operation is required.");
        }
        if (blank(properties.getSourceTopic()) || blank(properties.getGroupId())) {
            throw new IllegalArgumentException("connector.source-topic and connector.group-id are required.");
        }
        String operation = properties.getOperation().toUpperCase(Locale.ROOT);
        if (!OPERATIONS.contains(operation)) {
            throw new IllegalArgumentException("connector.operation must be one of " + OPERATIONS);
        }
        properties.setOperation(operation);
        if (blank(properties.getRecovery().getDltTopic())) {
            properties.getRecovery().setDltTopic(properties.getSourceTopic() + ".DLT");
        }
        if (blank(properties.getRecovery().getReviewTopic())) {
            properties.getRecovery().setReviewTopic(properties.getSourceTopic() + ".REVIEW");
        }
        if (properties.getSourceTopic().equals(properties.getRecovery().getDltTopic())
                || properties.getSourceTopic().equals(properties.getRecovery().getReviewTopic())) {
            throw new IllegalArgumentException("Source, DLT, and review topics must be distinct.");
        }
        if (!properties.getSourceTopic().equals(properties.getRecovery().getDltTopic())
                && !properties.getSourceTopic().equals(properties.getRecovery().getReviewTopic())
                && properties.getRecovery().getDltTopic() != null
                && properties.getRecovery().getReviewTopic() != null
                && properties.getRecovery().getDltTopic().equals(properties.getRecovery().getReviewTopic())) {
            throw new IllegalArgumentException("DLT and review topics must be distinct.");
        }
        boolean pseudonym = operation.equals("CREATE_PSEUDONYM") || operation.equals("UPDATE_PSEUDONYM")
                || operation.equals("DELETE_PSEUDONYM");
        if (pseudonym && blank(properties.getMapping().getIdentifierPath())
                && !operation.equals("DELETE_PSEUDONYM")) {
            throw new IllegalArgumentException("identifierPath is required for pseudonym operations.");
        }
        if (pseudonym && blank(properties.getMapping().getIdTypePath())
                && !operation.equals("DELETE_PSEUDONYM")) {
            throw new IllegalArgumentException("idTypePath is required for pseudonym create/update operations.");
        }
        if (operation.equals("CREATE_ENTITY") || operation.equals("UPDATE_ENTITY")
                || operation.equals("DELETE_ENTITY") || operation.equals("CREATE_ENTITY_AND_PSEUDONYM")) {
            if (blank(properties.getTrustdeck().getProject())) {
                throw new IllegalArgumentException("trustdeck.project is required for entity operations.");
            }
            if (blank(properties.getTrustdeck().getEntityType()) && blank(properties.getMapping().getEntityTypePath())) {
                throw new IllegalArgumentException("trustdeck.entity-type or mapping.entity-type-path is required for entity operations.");
            }
        }
    }

    /**
     * Tests whether text is absent or blank.
     *
     * @param text value to inspect
     * @return true when blank
     */
    private static boolean blank(String text) {
        return text == null || text.isBlank();
    }
}
