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

package org.trustdeck.connector.kafka.operation;

import java.util.List;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.trustdeck.client.TrustDeckClient;
import org.trustdeck.client.model.BatchResult;
import org.trustdeck.client.model.IdentifierItem;
import org.trustdeck.client.model.Pseudonym;
import org.trustdeck.connector.kafka.config.ConnectorProperties;
import org.trustdeck.connector.kafka.mapping.MappedMessage;

/**
 * Executes bounded pseudonym creation batches against TrustDeck.
 *
 * The service validates batch compatibility and serialized request size before
 * submission. Returned pseudonyms are correlated back to their source records
 * using complete identifying fields. Partial responses are completed safely by
 * replaying only unresolved source records individually.
 *
 * @author Armin Müller
 */
@Service
public class PseudonymBatchService {

	/** Shared TrustDeck client used for batch and fallback requests. */
	private final TrustDeckClient client;

	/** Connector configuration defining batch size and wire-size limits. */
	private final ConnectorProperties properties;

	/** JSON serializer used to enforce the configured batch wire-size limit. */
	private final ObjectMapper mapper;

	/**
	 * Creates the pseudonym batch service.
	 *
	 * @param client shared TrustDeck client
	 * @param properties connector configuration containing batch limits
	 * @param mapper JSON mapper used to determine serialized request size
	 */
	public PseudonymBatchService(TrustDeckClient client, ConnectorProperties properties, ObjectMapper mapper) {
		this.client = client;
		this.properties = properties;
		this.mapper = mapper;
	}

	/**
	 * Creates pseudonyms for one compatible batch of mapped source messages.
	 *
	 * All messages must target the same domain, contain an imported pseudonym
	 * value, and remain within the configured record and serialized byte limits.
	 * Returned results are correlated by identifier, identifier type, and
	 * pseudonym value. For partial responses, unresolved records are retried
	 * individually after all returned successes were correlated safely.
	 *
	 * @param messages compatible mapped source messages
	 * @return successfully created pseudonyms
	 * @throws IllegalArgumentException if the batch is incompatible, exceeds a configured limit, or cannot be serialized
	 * @throws IllegalStateException if the batch response cannot be correlated safely to the submitted source records
	 */
	public List<Pseudonym> execute(List<MappedMessage> messages) {
		if (messages.isEmpty()) {
			return List.of();
		}

		String domain = messages.getFirst().domain();

		// Only homogeneous imported-value batches can be correlated safely
		if (messages.size() > properties.getBatchSize() || messages.stream()
				.anyMatch(message -> !domain.equals(message.domain()) || message.psn() == null)) {
			throw new IllegalArgumentException("Pseudonym batch contains incompatible records.");
		}

		// Build the TrustDeck request while preserving the source message order
		List<Pseudonym> request = messages.stream()
				.map(message -> Pseudonym.builder()
						.identifierItem(IdentifierItem.builder().identifier(message.identifier()).idType(message.idType()).build())
						.psn(message.psn())
						.validityTime(message.validityTime())
						.validFrom(message.validFrom())
						.validTo(message.validTo())
						.build())
				.toList();

		// Enforce the configured wire-size limit on the serialized batch payload
		try {
			if (mapper.writeValueAsBytes(request).length > properties.getBatchBytes()) {
				throw new IllegalArgumentException("Pseudonym batch exceeds the configured byte limit.");
			}
		} catch (JsonProcessingException exception) {
			throw new IllegalArgumentException("Pseudonym batch cannot be serialized.", exception);
		}

		BatchResult<Pseudonym> result = client.project(properties.getTrustdeck().getProject())
				.pseudonyms(domain).createBatch(request, false);

		if (result == null || result.items() == null) {
			throw new IllegalStateException("Pseudonym batch response is missing its result items.");
		}

		// Keep all source occurrences for each correlation key so duplicate identifiers or 
		// pseudonym values can still be matched deterministically
		Map<String, Queue<Integer>> occurrences = new HashMap<>();
		for (int index = 0; index < messages.size(); index++) {
			occurrences.computeIfAbsent(key(messages.get(index)), ignored -> new ArrayDeque<>()).add(index);
		}

		// Correlate every returned pseudonym to exactly one submitted occurrence
		List<Pseudonym> correlated = new ArrayList<>(messages.size());
		for (Pseudonym returned : result.items()) {
			if (returned == null || returned.getIdentifierItem() == null) {
				throw new IllegalStateException("Pseudonym batch response correlation is ambiguous.");
			}

			Queue<Integer> indexes = occurrences.get(key(returned));
			if (indexes == null || indexes.isEmpty()) {
				throw new IllegalStateException("Pseudonym batch response correlation is ambiguous.");
			}

			indexes.remove();
			correlated.add(returned);
		}

		// A complete response must contain one safely correlated result for every input
		if (!result.partial() && occurrences.values().stream().anyMatch(queue -> !queue.isEmpty())) {
			throw new IllegalStateException("Pseudonym batch response omitted an input record.");
		}

		if (result.partial()) {
			if (correlated.isEmpty()) {
				throw new IllegalStateException("Partial pseudonym response has no safely correlated successes.");
			}

			// Retry only source records that were not represented in the partial response
			for (Queue<Integer> unresolved : occurrences.values()) {
				while (!unresolved.isEmpty()) {
					int index = unresolved.remove();
					correlated.add(client.project(properties.getTrustdeck().getProject()).pseudonyms(domain).create(request.get(index), false));
				}
			}
		}

		return correlated;
	}

	/**
	 * Builds the correlation key used to match a returned pseudonym to its source occurrence.
	 *
	 * Identifier, identifier type, and pseudonym value are combined because all
	 * three fields are available in the submitted imported-value batch.
	 *
	 * @param pseudonym returned pseudonym
	 * @return correlation key
	 */
	private String key(Pseudonym pseudonym) {
		return pseudonym.getIdentifierItem().getIdentifier() + "\u0000"
				+ pseudonym.getIdentifierItem().getIdType() + "\u0000"
				+ pseudonym.getPsn();
	}

	/**
	 * Builds the correlation key used to identify a submitted source occurrence.
	 *
	 * @param message mapped source message
	 * @return correlation key
	 */
	private String key(MappedMessage message) {
		return message.identifier() + "\u0000" + message.idType() + "\u0000" + message.psn();
	}
}
