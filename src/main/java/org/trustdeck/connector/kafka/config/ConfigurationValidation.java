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
import java.net.URI;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Validates the connector configuration and applies required normalization and defaults.
 *
 * Validation covers general connector settings, TrustDeck connection settings,
 * recovery topics, statistics configuration, operation-specific mappings,
 * entity field mappings, URLs, and JSON Pointer syntax.
 * 
 * @author Armin Müller
 */
@Configuration
@EnableConfigurationProperties(ConnectorProperties.class)
public class ConfigurationValidation {

    /** Connector operations supported by this application. */
    private static final Set<String> OPERATIONS = Set.of(
    		"CREATE_PSEUDONYM", 
    		"UPDATE_PSEUDONYM", 
    		"DELETE_PSEUDONYM",
            "CREATE_ENTITY", 
            "UPDATE_ENTITY", 
            "DELETE_ENTITY", 
            "CREATE_ENTITY_AND_PSEUDONYM");

    /**
     * Validates the supplied connector configuration and applies normalized or
	 * derived values where required.
	 *
	 * @param properties connector configuration to validate
	 * @throws IllegalArgumentException if a required setting is missing, invalid, or inconsistent with another setting
     */
	public ConfigurationValidation(ConnectorProperties properties) {
		if (blank(properties.getOperation())) {
			throw new IllegalArgumentException("connector.operation is required.");
		}
		
		if (blank(properties.getSourceTopic()) || blank(properties.getGroupId())) {
			throw new IllegalArgumentException("connector.source-topic and connector.group-id are required.");
		}
		
		// Normalize the operation before applying operation-specific validation
		String operation = properties.getOperation().toUpperCase(Locale.ROOT);
		if (!OPERATIONS.contains(operation)) {
			throw new IllegalArgumentException("connector.operation must be one of " + OPERATIONS);
		}
		
		properties.setOperation(operation);
		
		requireHttpUrl(properties.getTrustdeck().getServiceUrl(), "trustdeck.service-url");
		requireHttpUrl(properties.getTrustdeck().getTokenEndpoint(), "trustdeck.token-endpoint");
		
		if (blank(properties.getTrustdeck().getProject())) {
			throw new IllegalArgumentException("trustdeck.project is required for the project-scoped client API.");
		}
		
		if (properties.getConnectTimeout().isZero() || properties.getConnectTimeout().isNegative()
				|| properties.getReadTimeout().isZero() || properties.getReadTimeout().isNegative()) {
			throw new IllegalArgumentException("HTTP timeouts must be positive and finite.");
		}
		
		if (properties.getStatistics().getInterval() == null || properties.getStatistics().getInterval().isZero()
				|| properties.getStatistics().getInterval().isNegative()
				|| blank(properties.getStatistics().getOutputDirectory())
				|| properties.getStatistics().getMaxFileAge() == null
				|| properties.getStatistics().getMaxFileAge().isZero()
				|| properties.getStatistics().getMaxFileAge().isNegative()
				|| properties.getStatistics().getMaxFileSize() == null
				|| properties.getStatistics().getMaxFileSize().toBytes() <= 0) {
			throw new IllegalArgumentException("Statistics interval, maximum file age, and maximum file size must be positive; output directory must be set.");
		}
		
		if (properties.getRecovery().getBatchTimeBudget() == null || properties.getRecovery().getBatchTimeBudget().isZero()
				|| properties.getRecovery().getBatchTimeBudget().isNegative()) {
			throw new IllegalArgumentException("recovery.batch-time-budget must be positive.");
		}

		// Use predictable recovery topic names when none were configured explicitly
		if (blank(properties.getRecovery().getDltTopic())) {
			properties.getRecovery().setDltTopic(properties.getSourceTopic() + ".DLT");
		}
		
		if (blank(properties.getRecovery().getReviewTopic())) {
			properties.getRecovery().setReviewTopic(properties.getSourceTopic() + ".REVIEW");
		}
		
		// Recovery topics must never feed back into the source topic
		if (properties.getSourceTopic().equals(properties.getRecovery().getDltTopic())
				|| properties.getSourceTopic().equals(properties.getRecovery().getReviewTopic())) {
			throw new IllegalArgumentException("Source, DLT, and review topics must be distinct.");
		}
		
		// DLT and review processing must also use separate topics
		if (properties.getRecovery().getDltTopic().equals(properties.getRecovery().getReviewTopic())) {
			throw new IllegalArgumentException("DLT and review topics must be distinct.");
		}
		
		boolean pseudonym = operation.equals("CREATE_PSEUDONYM") || operation.equals("UPDATE_PSEUDONYM") || operation.equals("DELETE_PSEUDONYM");
		if (pseudonym && blank(properties.getMapping().getCommon().getIdentifierPath())
				&& !operation.equals("DELETE_PSEUDONYM")) {
			throw new IllegalArgumentException("identifierPath is required for pseudonym operations.");
		}
		
		if (pseudonym && blank(properties.getMapping().getCommon().getIdTypePath())
				&& !operation.equals("DELETE_PSEUDONYM")) {
			throw new IllegalArgumentException("idTypePath is required for pseudonym create/update operations.");
		}
		
		// Deletion may identify the pseudonym directly or via identifier and ID type
		if (operation.equals("DELETE_PSEUDONYM") && blank(properties.getMapping().getCommon().getPsnPath())
				&& (blank(properties.getMapping().getCommon().getIdentifierPath())
						|| blank(properties.getMapping().getCommon().getIdTypePath()))) {
			throw new IllegalArgumentException("delete pseudonym needs psnPath or identifierPath and idTypePath.");
		}
		
		if (operation.equals("UPDATE_PSEUDONYM")
				&& (blank(properties.getMapping().getUpdatePseudonym().getOldIdentifierPath())
						|| blank(properties.getMapping().getUpdatePseudonym().getOldIdTypePath()))) {
			throw new IllegalArgumentException("update pseudonym requires old identifier and id type mappings.");
		}
		
		if (operation.equals("CREATE_ENTITY") || operation.equals("UPDATE_ENTITY") || operation.equals("DELETE_ENTITY")
				|| operation.equals("CREATE_ENTITY_AND_PSEUDONYM")) {
			if (blank(properties.getTrustdeck().getProject())) {
				throw new IllegalArgumentException("trustdeck.project is required for entity operations.");
			}
			
			// The entity type may be fixed globally or resolved from each incoming record
			if (blank(properties.getTrustdeck().getEntityType())
					&& blank(properties.getMapping().getCommon().getEntityTypePath())) {
				throw new IllegalArgumentException("trustdeck.entity-type or mapping.entity-type-path is required for entity operations.");
			}
			
			for (ConnectorProperties.FieldMapping field : properties.getMapping().getCommon().getEntityFields()) {
				if (field == null || blank(field.getSourcePath()) || blank(field.getTargetPath())) {
					throw new IllegalArgumentException("Entity field mappings require sourcePath and targetPath.");
				}
				
				validatePointer(field.getSourcePath(), "entity field sourcePath");
				validatePointer(field.getTargetPath(), "entity field targetPath");
			}
		}
	}

	/**
	 * Tests whether text is absent or blank.
	 *
	 * @param text value to inspect
	 * @return {@code true} if the value is {@code null} or blank, otherwise {@code false}
	 */
	private static boolean blank(String text) {
		return text == null || text.isBlank();
	}

	/**
	 * Validates that a configuration value contains a usable HTTP or HTTPS URL.
	 * The URL must define either the {@code http} or {@code https} scheme and contain a host.
	 *
	 * @param value URL value to validate
	 * @param name configuration property name used in validation messages
	 * @throws IllegalArgumentException if the value is missing or is not a valid HTTP(S) URL
	 */
	private static void requireHttpUrl(String value, String name) {
		if (blank(value)) {
			throw new IllegalArgumentException(name + " must be an HTTP(S) URL.");
		}
		
		try {
			URI uri = URI.create(value);
			if (!Set.of("http", "https").contains(uri.getScheme()) || uri.getHost() == null) {
				throw new IllegalArgumentException(name + " must be an HTTP(S) URL.");
			}
		} catch (IllegalArgumentException exception) {
			throw new IllegalArgumentException(name + " must be an HTTP(S) URL.", exception);
		}
	}

	/**
	 * Validates the JSON Pointer syntax used by a source or target mapping.
	 *
	 * Empty path segments, trailing separators, and invalid tilde escape
	 * sequences are rejected. Valid JSON Pointer escapes are {@code ~0} and {@code ~1}.
	 *
	 * @param value JSON Pointer to validate
	 * @param name mapping name used in validation messages
	 * @throws IllegalArgumentException if the JSON Pointer contains an invalid structure or escape sequence
	 */
	private static void validatePointer(String value, String name) {
		if (value.startsWith("/") && (value.equals("/") || value.contains("//") || value.endsWith("/"))) {
			throw new IllegalArgumentException(name + " is not a valid non-empty JSON Pointer.");
		}
		
		if (value.contains("~") && !value.matches("(?:[^~]|~0|~1)*")) {
			throw new IllegalArgumentException(name + " contains an invalid JSON Pointer escape.");
		}
	}
}
