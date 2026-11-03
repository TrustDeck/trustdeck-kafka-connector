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

package org.trustdeck.connector.kafka.listener;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.trustdeck.connector.kafka.health.OutageCoordinator;

/**
 * Last-resort handler for exceptions that escape the batch listener.
 *
 * The listener handles systemic failures itself by pausing and calling
 * {@code nack}, so this handler only runs for unexpected failures such as
 * programming errors or interruption during shutdown. It replaces Spring's
 * default batch handling, which re-invokes the whole batch up to ten times
 * without delay (repeating TrustDeck calls for records that were already
 * acknowledged) and then <em>discards</em> the batch with only a log entry.
 *
 * Instead, this handler never retries and never commits. It rewinds every
 * still-assigned partition of the failed batch to its last committed offset, so
 * acknowledged records are not repeated and unacknowledged records are
 * redelivered. Partitions revoked by a concurrent rebalance are skipped instead
 * of failing the seek. While the container is running, consumption is also
 * paused through the outage coordinator so the failure is reported once and
 * retried at the probe interval instead of in a tight loop.
 *
 * @author Armin Müller
 */
@Slf4j
public class ListenerErrorHandler implements CommonErrorHandler {

	/** Outage category used when an unexpected listener failure pauses consumption. */
	public static final String UNEXPECTED_LISTENER_FAILURE = "UNEXPECTED_LISTENER_FAILURE";

	/** Lazily resolved coordinator, avoiding a dependency cycle with the listener registry. */
	private final ObjectProvider<OutageCoordinator> outage;

	/**
	 * Creates the handler.
	 *
	 * @param outage provider of the outage coordinator used to pause consumption
	 */
	public ListenerErrorHandler(ObjectProvider<OutageCoordinator> outage) {
		this.outage = outage;
	}

	/**
	 * Offsets are committed only by explicit listener acknowledgments.
	 *
	 * @return always {@code false}
	 */
	@Override
	public boolean isAckAfterHandle() {
		return false;
	}

	/**
	 * Unprocessed records are redelivered through seeks.
	 *
	 * @return always {@code true}
	 */
	@Override
	public boolean seeksAfterHandling() {
		return true;
	}

	/**
	 * Rewinds the failed batch to the committed offsets and pauses consumption.
	 *
	 * @param thrownException exception thrown by the listener
	 * @param data records of the failed batch
	 * @param consumer consumer owned by the calling container thread
	 * @param container container that invoked the listener
	 * @param invokeListener callback that would re-invoke the listener; intentionally unused
	 */
	@Override
	public void handleBatch(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
					MessageListenerContainer container, Runnable invokeListener) {
		log.error("Listener failed unexpectedly; rewinding {} record(s) to committed offsets: {}",
				data.count(), FailureClassifier.describe(thrownException), thrownException);

		rewind(data, consumer);

		if (container.isRunning() && !Thread.currentThread().isInterrupted()) {
			OutageCoordinator coordinator = outage.getIfAvailable();
			if (coordinator != null) {
				coordinator.pause(UNEXPECTED_LISTENER_FAILURE);
			}
		}
	}

	/**
	 * Seeks each still-assigned partition of the batch to its committed offset,
	 * or to the first batch offset when nothing was committed yet.
	 *
	 * @param data records of the failed batch
	 * @param consumer consumer to seek
	 */
	private void rewind(ConsumerRecords<?, ?> data, Consumer<?, ?> consumer) {
		Set<TopicPartition> partitions = new HashSet<>(data.partitions());
		partitions.retainAll(consumer.assignment());
		if (partitions.isEmpty()) {
			return;
		}

		Map<TopicPartition, OffsetAndMetadata> committed;
		try {
			committed = consumer.committed(partitions);
		} catch (RuntimeException exception) {
			// Fall back to the batch start; repeating acknowledged records is preferable to skipping records
			log.warn("Could not read committed offsets; rewinding to batch start: {}", FailureClassifier.describe(exception));
			committed = new HashMap<>();
		}

		for (TopicPartition partition : partitions) {
			OffsetAndMetadata offset = committed.get(partition);
			long target = offset != null ? offset.offset() : data.records(partition).getFirst().offset();

			try {
				consumer.seek(partition, target);
			} catch (IllegalStateException revoked) {
				// The partition was revoked meanwhile; its new owner starts from the committed offset anyway
				log.debug("Skipping seek for revoked partition {}", partition);
			}
		}
	}
}
