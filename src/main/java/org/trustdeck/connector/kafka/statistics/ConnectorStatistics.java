/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.statistics;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;
import org.trustdeck.client.TrustDeckClient;
import org.trustdeck.client.model.ProjectStatistics;
import org.trustdeck.client.model.SystemStatistics;
import org.trustdeck.connector.kafka.config.ConnectorProperties;

/**
 * Collects connector runtime metrics together with periodic TrustDeck backend statistics.
 *
 * Statistics collection is optional and runs on a dedicated daemon thread. Listener
 * processing only updates in-memory counters, while backend statistics are sampled
 * asynchronously. When the application stops, the complete run is written as one
 * JSON file to the configured output directory.
 *
 * @author Armin Müller
 */
@Component
@Slf4j
public class ConnectorStatistics {

	/** Shared TrustDeck client used to retrieve backend statistics. */
	private final TrustDeckClient client;

	/** Connector configuration containing statistics settings. */
	private final ConnectorProperties properties;

	/** JSON serializer used to persist completed statistics runs. */
	private final ObjectMapper mapper;

	/** Total number of source records observed by the listener. */
	private final AtomicLong records = new AtomicLong();

	/** Total number of listener batches processed. */
	private final AtomicLong batches = new AtomicLong();

	/** Accumulated listener batch processing duration in nanoseconds. */
	private final AtomicLong batchNanos = new AtomicLong();

	/** Collected backend and connector statistics snapshots. */
	private final List<Snapshot> snapshots = new ArrayList<>();

	/** Dedicated scheduler used for asynchronous statistics sampling. */
	private ScheduledExecutorService executor;

	/** Timestamp at which statistics collection started. */
	private Instant startedAt;

	/**
	 * Creates the statistics collector.
	 *
	 * @param client shared TrustDeck client
	 * @param properties connector configuration
	 * @param mapper JSON mapper used to persist statistics
	 */
	public ConnectorStatistics(TrustDeckClient client, ConnectorProperties properties, ObjectMapper mapper) {
		this.client = client;
		this.properties = properties;
		this.mapper = mapper;
	}

	/**
	 * Starts periodic statistics collection when explicitly enabled.
	 *
	 * Sampling runs on a dedicated daemon thread using the configured interval so
	 * backend statistics requests do not block Kafka listener processing.
	 */
	@PostConstruct
	public void start() {
		if (!properties.getStatistics().isEnabled()) {
			return;
		}

		startedAt = Instant.now();
		Duration interval = properties.getStatistics().getInterval();

		// Keep backend statistics requests isolated from Kafka consumer threads.
		executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
			Thread thread = new Thread(runnable, "connector-statistics");
			thread.setDaemon(true);
			return thread;
		});

		executor.scheduleWithFixedDelay(this::sampleSafely, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
	}

	/**
	 * Records metrics for one listener batch without performing external I/O.
	 *
	 * The counters are updated atomically so listener threads can record batches
	 * concurrently with asynchronous statistics sampling.
	 *
	 * @param recordCount number of records contained in the batch
	 * @param elapsedNanos batch processing duration in nanoseconds
	 */
	public void recordBatch(int recordCount, long elapsedNanos) {
		if (!properties.getStatistics().isEnabled()) {
			return;
		}

		records.addAndGet(recordCount);
		batches.incrementAndGet();
		batchNanos.addAndGet(elapsedNanos);
	}

	/**
	 * Stops periodic sampling and persists the completed statistics run.
	 *
	 * No file is written when statistics collection was disabled or never started.
	 */
	@PreDestroy
	public void stop() {
		if (!properties.getStatistics().isEnabled() || startedAt == null) {
			return;
		}

		if (executor != null) {
			executor.shutdownNow();
		}

		writeRun();
	}

	/**
	 * Collects one backend and connector statistics snapshot without propagating failures.
	 *
	 * Project statistics are collected when a project is configured, followed by
	 * system statistics. Sampling failures are logged and retained as error snapshots
	 * so later samples can continue normally.
	 */
	private void sampleSafely() {
		try {
			ProjectStatistics project = null;
			String projectName = properties.getTrustdeck().getProject();

			if (projectName != null && !projectName.isBlank()) {
				project = client.project(projectName).getStatistics();
			}

			SystemStatistics system = client.system().getStatistics();

			// Synchronize snapshot writes because shutdown may copy the list concurrently
			synchronized (snapshots) {
				snapshots.add(new Snapshot(Instant.now(), records.get(), batches.get(), averageBatchMillis(),
						project, system, null));
			}
		} catch (Exception exception) {
			log.warn("Unable to collect TrustDeck connector statistics sample", exception);

			// Preserve sampling failures in the run instead of interrupting future collection
			synchronized (snapshots) {
				snapshots.add(new Snapshot(Instant.now(), records.get(), batches.get(), averageBatchMillis(),
						null, null, exception.getClass().getSimpleName() + ": " + exception.getMessage()));
			}
		}
	}

	/**
	 * Calculates the average listener batch processing duration.
	 *
	 * @return average batch duration in milliseconds, or {@code 0.0} if no batch has been recorded
	 */
	private double averageBatchMillis() {
		long count = batches.get();
		return count == 0 ? 0.0 : batchNanos.get() / 1_000_000.0 / count;
	}

	/**
	 * Persists the completed connector statistics run as a JSON file.
	 *
	 * The output directory is created when necessary. The run is first written to
	 * a temporary file and then moved to its final name so partially written files
	 * are not exposed. Atomic replacement is used when supported by the filesystem.
	 */
	private void writeRun() {
		try {
			Path directory = Path.of(properties.getStatistics().getOutputDirectory());
			Files.createDirectories(directory);

			List<Snapshot> samples;

			// Create a stable snapshot before serializing the completed run
			synchronized (snapshots) {
				samples = List.copyOf(snapshots);
			}

			Run run = new Run(startedAt, Instant.now(), records.get(), batches.get(), averageBatchMillis(), samples);

			String filename = "connector-statistics-" + startedAt.toString().replace(':', '-') + ".json";
			Path target = directory.resolve(filename);
			Path temporary = Files.createTempFile(directory, ".connector-statistics-", ".tmp");

			mapper.writerWithDefaultPrettyPrinter().writeValue(temporary.toFile(), run);

			try {
				Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
			} catch (AtomicMoveNotSupportedException exception) {
				// Fall back to a regular replacement when the filesystem cannot move atomically
				Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
			}

			log.info("Wrote connector statistics run to " + target);
		} catch (IOException exception) {
			log.error("Unable to write connector statistics run", exception);
		}
	}

	/**
	 * Represents one periodic connector and TrustDeck backend statistics sample.
	 *
	 * A failed backend sample contains the connector counters and an error description
	 * while the backend statistics values remain {@code null}.
	 *
	 * @param sampledAt time at which the sample was collected
	 * @param records total source records received at sampling time
	 * @param batches total listener batches received at sampling time
	 * @param averageBatchMillis average listener batch processing duration in milliseconds
	 * @param project project statistics, if available
	 * @param system system statistics, if available
	 * @param error sampling error description, if collection failed
	 */
	public record Snapshot(Instant sampledAt, long records, long batches, double averageBatchMillis,
			ProjectStatistics project, SystemStatistics system, String error) {
	}

	/**
	 * Represents the complete statistics collected during one connector process lifetime.
	 *
	 * The run combines final listener counters with all periodic backend snapshots
	 * collected between statistics startup and application shutdown.
	 *
	 * @param startedAt time at which statistics collection started
	 * @param finishedAt time at which the completed run was created
	 * @param records total source records received
	 * @param batches total listener batches received
	 * @param averageBatchMillis average listener batch processing duration in milliseconds
	 * @param samples periodic backend and connector statistics samples
	 */
	public record Run(Instant startedAt, Instant finishedAt, long records, long batches,
			double averageBatchMillis, List<Snapshot> samples) {
	}
}
