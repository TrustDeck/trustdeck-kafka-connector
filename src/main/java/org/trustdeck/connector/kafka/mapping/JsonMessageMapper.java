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

package org.trustdeck.connector.kafka.mapping;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.zone.ZoneRulesException;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;
import org.trustdeck.connector.kafka.config.ConnectorProperties;

/**
 * Maps raw Kafka message values to validated connector messages.
 *
 * The mapper parses UTF-8 JSON input, resolves configured source
 * mappings, converts timestamps and UUIDs, builds entity data, and validates
 * operation-specific requirements before returning a {@link MappedMessage}.
 *
 * @author Armin Müller
 */
@Component
public class JsonMessageMapper {

	/** Maximum JSON nesting depth accepted by the connector. */
	private static final int MAX_DEPTH = 64;

	/** JSON parser with bounded nesting depth and string length. */
	private final ObjectMapper mapper = JsonMapper.builder(new JsonFactory().setStreamReadConstraints(
			StreamReadConstraints.builder().maxNestingDepth(MAX_DEPTH).maxStringLength(1_048_576).build())).build();

	/** Connector configuration containing source mapping definitions. */
	private final ConnectorProperties properties;

	/**
	 * Creates the message mapper.
	 *
	 * @param properties connector configuration containing mapping settings
	 */
	public JsonMessageMapper(ConnectorProperties properties) {
		this.properties = properties;
	}

	/**
	 * Maps one raw Kafka value according to the configured connector operation.
	 *
	 * The source value must contain exactly one UTF-8 encoded JSON object. Common
	 * and operation-specific mappings are resolved, converted, and validated
	 * before the resulting connector message is returned.
	 *
	 * @param bytes raw Kafka value bytes
	 * @return mapped and validated connector message
	 * @throws MappingException if the source value or a mapped value is invalid
	 */
	public MappedMessage map(byte[] bytes) {
		if (bytes == null || bytes.length == 0) {
			throw new MappingException("Kafka value must not be null or empty.");
		}

		JsonNode root;

		// Decode strictly as UTF-8 and require exactly one JSON value
		try {
			CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
					.onMalformedInput(CodingErrorAction.REPORT)
					.onUnmappableCharacter(CodingErrorAction.REPORT)
					.decode(ByteBuffer.wrap(bytes));

			try (JsonParser parser = mapper.getFactory().createParser(decoded.toString())) {
				root = mapper.readTree(parser);

				if (root == null || parser.nextToken() != null) {
					throw new MappingException("Kafka value must contain exactly one JSON value.");
				}
			}
		} catch (CharacterCodingException exception) {
			throw new MappingException("Kafka value is not valid UTF-8 JSON.", exception);
		} catch (java.io.IOException exception) {
			throw new MappingException("Kafka value is not valid UTF-8 JSON.", exception);
		}

		if (!root.isObject()) {
			throw new MappingException("Kafka value root must be a JSON object.");
		}

		ConnectorProperties.MappingProperties mapping = properties.getMapping();
		String operation = properties.getOperation();
		var common = mapping.getCommon();
		var update = mapping.getUpdatePseudonym();

		// Resolve common pseudonym, validity, provenance, and entity mappings
		String domain = value(root, common.getDomainConstant(), common.getDomainPath(), false);
		String identifier = value(root, null, common.getIdentifierPath(), false);
		String idType = value(root, null, common.getIdTypePath(), false);
		String psn = value(root, null, common.getPsnPath(), false);
		String validityTime = value(root, null, common.getValidityTimePath(), false);
		LocalDateTime validFrom = datetime(root, common.getValidFromPath());
		LocalDateTime validTo = datetime(root, common.getValidToPath());
		String username = value(root, null, common.getUsernamePath(), false);
		String entityType = value(root, properties.getTrustdeck().getEntityType(), common.getEntityTypePath(), false);
		UUID entityId = parseUuid(value(root, null, common.getEntityIdPath(), false));
		ObjectNode entityData = entityData(root, common.getEntityFields());

		// Resolve fields used specifically by pseudonym update operations
		String oldIdentifier = value(root, null, update.getOldIdentifierPath(), false);
		String oldIdType = value(root, null, update.getOldIdTypePath(), false);
		String oldPsn = value(root, null, update.getOldPsnPath(), false);
		String newIdentifier = value(root, null, update.getNewIdentifierPath(), false);
		String newIdType = value(root, null, update.getNewIdTypePath(), false);
		String newPsn = value(root, null, update.getNewPsnPath(), false);
		String oldDomain = value(root, null, update.getOldDomainPath(), false);
		String newDomain = value(root, null, update.getNewDomainPath(), false);

		// Validate values required by the selected operation
		if (operation.equals("CREATE_PSEUDONYM") || operation.equals("UPDATE_PSEUDONYM")) {
			require(identifier, "identifierPath");
			require(idType, "idTypePath");
		}

		if (operation.equals("DELETE_PSEUDONYM") && identifier == null && psn == null) {
			throw new MappingException("DELETE_PSEUDONYM requires identifier/idType or psn mapping.");
		}

		if ((operation.equals("CREATE_PSEUDONYM") || operation.equals("UPDATE_PSEUDONYM")
				|| operation.equals("DELETE_PSEUDONYM") || operation.equals("CREATE_ENTITY_AND_PSEUDONYM"))
				&& domain == null) {
			throw new MappingException("A domain constant or domain path is required.");
		}

		if ((operation.equals("UPDATE_ENTITY") || operation.equals("DELETE_ENTITY")) && entityId == null) {
			throw new MappingException("Entity update/delete requires a mapped TrustDeck UUID.");
		}

		return new MappedMessage(domain, identifier, idType, psn, validityTime, validFrom, validTo, username, entityType, 
				entityId, entityData, oldIdentifier, oldIdType, oldPsn, newIdentifier, newIdType, newPsn, oldDomain, newDomain);
	}

	/**
	 * Resolves a string value from a configured constant or source path.
	 *
	 * A non-blank constant takes precedence over the source path. Missing or null
	 * source values resolve to {@code null}, while mapped non-string values are rejected.
	 *
	 * @param root source JSON object
	 * @param constant optional constant value
	 * @param path optional source path
	 * @param required whether a configured path is required
	 * @return resolved non-blank string value, or {@code null} if absent
	 * @throws MappingException if a required path is missing or the mapped value is not a JSON string
	 */
	private String value(JsonNode root, String constant, String path, boolean required) {
		if (constant != null && !constant.isBlank()) {
			return constant;
		}

		if (path == null || path.isBlank()) {
			if (required) {
				throw new MappingException("Required mapping path is missing.");
			}

			return null;
		}

		String pointer = path.startsWith("/") ? path : "/" + path;
		JsonNode node = root.at(pointer);

		if (node.isMissingNode() || node.isNull()) {
			return null;
		}

		if (!node.isValueNode() || !node.isTextual()) {
			throw new MappingException("Mapped scalar field must be a JSON string: " + pointer);
		}

		String text = node.textValue();
		return text == null || text.isBlank() ? null : text;
	}

	/**
	 * Resolves and converts a mapped timestamp to a UTC {@link LocalDateTime}.
	 *
	 * Offset-aware ISO timestamps are converted directly to UTC. Offset-free
	 * timestamps use the configured format and source timezone and are rejected
	 * when the local time is ambiguous or nonexistent.
	 *
	 * @param root source JSON object
	 * @param path configured timestamp source path
	 * @return timestamp converted to UTC, or {@code null} if no value is mapped
	 * @throws MappingException if the timestamp, format, or timezone is invalid
	 */
	private LocalDateTime datetime(JsonNode root, String path) {
		String text = value(root, null, path, false);
		if (text == null) {
			return null;
		}

		try {
			// Prefer timestamps that already contain an explicit UTC offset.
			try {
				return OffsetDateTime.parse(text, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
						.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime();
			} catch (DateTimeParseException ignored) {
				// Offset-free timestamps require an explicit format and source timezone
				String pattern = properties.getMapping().getCommon().getDatetimeFormat();
				if (pattern == null || pattern.isBlank()) {
					throw new MappingException("Offset-free timestamp requires mapping.datetimeFormat.");
				}

				DateTimeFormatter formatter = new DateTimeFormatterBuilder()
						.appendPattern(pattern).toFormatter().withResolverStyle(ResolverStyle.STRICT);

				LocalDateTime local = LocalDateTime.parse(text, formatter);
				ZoneId zone = ZoneId.of(properties.getMapping().getCommon().getSourceTimezone());
				List<ZoneOffset> offsets = zone.getRules().getValidOffsets(local);

				// Reject daylight-saving overlaps and gaps instead of choosing an offset implicitly
				if (offsets.size() != 1) {
					throw new MappingException("Timestamp falls in an ambiguous or nonexistent local time.");
				}

				return ZonedDateTime.ofLocal(local, zone, offsets.getFirst()).withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
			}
		} catch (DateTimeParseException | ZoneRulesException exception) {
			throw new MappingException("Mapped timestamp is invalid.", exception);
		}
	}

	/**
	 * Builds entity data from the configured source-to-target field mappings.
	 *
	 * Original JSON value types are preserved. Missing optional mappings are
	 * skipped, while missing required mappings cause processing to fail.
	 *
	 * @param root source JSON object
	 * @param fields configured entity field mappings
	 * @return entity data assembled from the mapped fields
	 * @throws MappingException if a required source value is absent or a mapped value has an unsupported type
	 */
	private ObjectNode entityData(JsonNode root, List<ConnectorProperties.FieldMapping> fields) {
		ObjectNode result = mapper.createObjectNode();

		if (fields == null) {
			return result;
		}

		for (ConnectorProperties.FieldMapping field : fields) {
			// Constants override source values while preserving the same target handling
			JsonNode source = field.getConstant() == null
					? root.at(pointer(field.getSourcePath()))
					: mapper.valueToTree(field.getConstant());

			if (source.isMissingNode() || source.isNull()) {
				if (field.isRequired()) {
					throw new MappingException("Required entity mapping is absent: " + field.getSourcePath());
				}

				continue;
			}

			if (!source.isValueNode() && !source.isContainerNode()) {
				throw new MappingException("Mapped entity value has an unsupported type.");
			}

			setPointer(result, field.getTargetPath(), source);
		}

		return result;
	}

	/**
	 * Converts a mapped string value to a UUID.
	 *
	 * @param text mapped UUID text
	 * @return parsed UUID, or {@code null} if no value is mapped
	 * @throws MappingException if the mapped value is not a valid UUID
	 */
	private UUID parseUuid(String text) {
		if (text == null) {
			return null;
		}

		try {
			return UUID.fromString(text);
		} catch (IllegalArgumentException exception) {
			throw new MappingException("Mapped entity ID is not a UUID.", exception);
		}
	}

	/**
	 * Requires a mapped string value to be present and non-blank.
	 *
	 * @param value mapped value to validate
	 * @param name mapping name used in the validation message
	 * @throws MappingException if the value is {@code null} or blank
	 */
	private void require(String value, String name) {
		if (value == null || value.isBlank()) {
			throw new MappingException(name + " must resolve to a nonblank value.");
		}
	}

	/**
	 * Converts a configured path to JSON Pointer form.
	 *
	 * Bare top-level field names are prefixed with {@code /}; existing JSON
	 * Pointers are returned unchanged.
	 *
	 * @param path configured source or target path
	 * @return normalized JSON Pointer
	 * @throws MappingException if the configured path is null or blank
	 */
	private String pointer(String path) {
		if (path == null || path.isBlank()) {
			throw new MappingException("Mapping path must not be blank.");
		}

		return path.startsWith("/") ? path : "/" + path;
	}

	/**
	 * Writes a mapped value to a shallow or nested object JSON Pointer.
	 *
	 * Missing intermediate objects are created as required. JSON Pointer escape
	 * sequences in path segments are decoded before the target fields are written.
	 *
	 * @param target target entity data object
	 * @param path configured target JSON Pointer
	 * @param value mapped JSON value
	 * @throws MappingException if the target path is null or blank
	 */
	private void setPointer(ObjectNode target, String path, JsonNode value) {
		String[] parts = pointer(path).substring(1).split("/");
		ObjectNode current = target;

		// Traverse the target path and create missing intermediate objects
		for (int i = 0; i < parts.length - 1; i++) {
			String part = parts[i].replace("~1", "/").replace("~0", "~");
			JsonNode child = current.get(part);
			current = child != null && child.isObject() ? (ObjectNode) child : current.putObject(part);
		}

		// Deep-copy the source value so the output tree owns its mapped node
		current.set(parts[parts.length - 1].replace("~1", "/").replace("~0", "~"), value.deepCopy());
	}
}
