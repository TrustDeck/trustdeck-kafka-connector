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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
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

/** Parses strict JSON objects and applies only explicitly configured mappings. */
@Component
public class JsonMessageMapper {

    /** Maximum JSON nesting accepted by the connector. */
    private static final int MAX_DEPTH = 64;

    /** JSON parser with bounded nesting and token length. */
    private final ObjectMapper mapper = JsonMapper.builder(new JsonFactory().setStreamReadConstraints(
            StreamReadConstraints.builder().maxNestingDepth(MAX_DEPTH).maxStringLength(1_048_576).build())).build();

    /** Connector mapping settings. */
    private final ConnectorProperties properties;

    /**
     * Creates the mapper.
     *
     * @param properties connector settings
     */
    public JsonMessageMapper(ConnectorProperties properties) {
        this.properties = properties;
    }

    /**
     * Maps one Kafka value according to the selected operation.
     *
     * @param bytes raw Kafka value bytes
     * @return mapped message
     * @throws MappingException when bytes or configured values are invalid
     */
    public MappedMessage map(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new MappingException("Kafka value must not be null or empty.");
        }
        JsonNode root;
        try {
            CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
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
        String oldIdentifier = value(root, null, update.getOldIdentifierPath(), false);
        String oldIdType = value(root, null, update.getOldIdTypePath(), false);
        String oldPsn = value(root, null, update.getOldPsnPath(), false);
        String newIdentifier = value(root, null, update.getNewIdentifierPath(), false);
        String newIdType = value(root, null, update.getNewIdTypePath(), false);
        String newPsn = value(root, null, update.getNewPsnPath(), false);
        String oldDomain = value(root, null, update.getOldDomainPath(), false);
        String newDomain = value(root, null, update.getNewDomainPath(), false);

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
        return new MappedMessage(domain, identifier, idType, psn, validityTime, validFrom, validTo, username,
                entityType, entityId, entityData, oldIdentifier, oldIdType, oldPsn,
                newIdentifier, newIdType, newPsn, oldDomain, newDomain);
    }

    /**
     * Resolves a configured constant before a source path.
     *
     * @param root source object
     * @param constant optional constant
     * @param path optional source path
     * @param required whether absence is an error
     * @return string value or null
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
     * Converts an explicitly mapped timestamp.
     *
     * @param root source object
     * @param path source path
     * @return UTC wall-clock value or null
     */
    private LocalDateTime datetime(JsonNode root, String path) {
        String text = value(root, null, path, false);
        if (text == null) {
            return null;
        }
        try {
            try {
                return java.time.OffsetDateTime.parse(text, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                        .withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime();
            } catch (DateTimeParseException ignored) {
                String pattern = properties.getMapping().getCommon().getDatetimeFormat();
                if (pattern == null || pattern.isBlank()) {
                    throw new MappingException("Offset-free timestamp requires mapping.datetimeFormat.");
                }
                DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern(pattern)
                        .toFormatter().withResolverStyle(ResolverStyle.STRICT);
                LocalDateTime local = LocalDateTime.parse(text, formatter);
                ZoneId zone = ZoneId.of(properties.getMapping().getCommon().getSourceTimezone());
                List<ZoneOffset> offsets = zone.getRules().getValidOffsets(local);
                if (offsets.size() != 1) {
                    throw new MappingException("Timestamp falls in an ambiguous or nonexistent local time.");
                }
                return ZonedDateTime.ofLocal(local, zone, offsets.getFirst()).withZoneSameInstant(ZoneOffset.UTC)
                        .toLocalDateTime();
            }
        } catch (DateTimeParseException | java.time.zone.ZoneRulesException exception) {
            throw new MappingException("Mapped timestamp is invalid.", exception);
        }
    }

    /**
     * Builds entity data from explicit field mappings while preserving JSON types.
     *
     * @param root source object
     * @param fields field mappings
     * @return mapped object
     */
    private ObjectNode entityData(JsonNode root, List<ConnectorProperties.FieldMapping> fields) {
        ObjectNode result = mapper.createObjectNode();
        if (fields == null) {
            return result;
        }
        for (ConnectorProperties.FieldMapping field : fields) {
            JsonNode source = field.getConstant() == null ? root.at(pointer(field.getSourcePath()))
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
     * Converts a UUID mapping.
     *
     * @param text UUID text
     * @return UUID or null
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
     * Requires a nonblank mapped value.
     *
     * @param value value
     * @param name mapping name
     */
    private void require(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new MappingException(name + " must resolve to a nonblank value.");
        }
    }

    /**
     * Converts bare descriptors to JSON Pointer form.
     *
     * @param path configured path
     * @return pointer
     */
    private String pointer(String path) {
        if (path == null || path.isBlank()) {
            throw new MappingException("Mapping path must not be blank.");
        }
        return path.startsWith("/") ? path : "/" + path;
    }

    /**
     * Writes a mapped value at a shallow or nested object pointer.
     *
     * @param target output object
     * @param path target pointer
     * @param value source value
     */
    private void setPointer(ObjectNode target, String path, JsonNode value) {
        String[] parts = pointer(path).substring(1).split("/");
        ObjectNode current = target;
        for (int i = 0; i < parts.length - 1; i++) {
            String part = parts[i].replace("~1", "/").replace("~0", "~");
            JsonNode child = current.get(part);
            current = child != null && child.isObject() ? (ObjectNode) child : current.putObject(part);
        }
        current.set(parts[parts.length - 1].replace("~1", "/").replace("~0", "~"), value.deepCopy());
    }
}
