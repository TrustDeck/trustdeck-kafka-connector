/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.statistics;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.unit.DataSize;
import org.trustdeck.client.TrustDeckClient;
import org.trustdeck.client.model.ProjectStatistics;
import org.trustdeck.client.model.SystemStatistics;
import org.trustdeck.connector.kafka.config.ConnectorProperties;
import org.trustdeck.connector.kafka.listener.FailureClassifier;

/**
 * Collects connector runtime metrics together with periodic TrustDeck backend statistics.
 *
 * Listener processing only updates atomic counters. Each backend sample is streamed to
 * the active hidden segment file, which is finalized after its age or size limit and
 * made visible before the next segment is opened.
 *
 * @author Armin Müller
 */
@Component
@Slf4j
public class ConnectorStatistics {

	/** Maximum time allowed for the executor to finish during shutdown. */
	private static final Duration SHUTDOWN_EXECUTOR_WAIT = Duration.ofSeconds(25);

	/** Maximum time allowed to acquire the writer lock during shutdown. */
	private static final Duration SHUTDOWN_WRITER_WAIT = Duration.ofSeconds(5);

	/** Shared TrustDeck client used to retrieve backend statistics. */
	private final TrustDeckClient client;

	/** Connector configuration containing statistics settings. */
	private final ConnectorProperties properties;

	/** JSON serializer and streaming generator codec. */
	private final ObjectMapper mapper;

	/** Clock used for segment timestamps and elapsed-age checks. */
	private final Clock clock;

	/** Total number of source records observed by the listener. */
	private final AtomicLong records = new AtomicLong();

	/** Total number of listener batches processed. */
	private final AtomicLong batches = new AtomicLong();

	/** Accumulated listener batch processing duration in nanoseconds. */
	private final AtomicLong batchNanos = new AtomicLong();

	/** Serializes generator writes, rotation, and shutdown finalization. */
	private final ReentrantLock writerLock = new ReentrantLock();

	/** Dedicated scheduler used for asynchronous statistics sampling. */
	private ScheduledExecutorService executor;

	/** Currently open segment, or {@code null} between rotation and the next sample. */
	private Segment segment;

	/** Whether statistics collection successfully initialized a segment. */
	private volatile boolean initialized;

	/** Whether persistence has failed and must remain disabled for this process. */
	private volatile boolean persistenceDisabled;

	/** Prevents sampling tasks from writing after shutdown finalization begins. */
	private volatile boolean stopping;

	/**
	 * Creates the statistics collector using the system UTC clock.
	 *
	 * @param client shared TrustDeck client
	 * @param properties connector configuration
	 * @param mapper JSON mapper used to persist statistics
	 */
	@Autowired
	public ConnectorStatistics(TrustDeckClient client, ConnectorProperties properties, ObjectMapper mapper) {
		this(client, properties, mapper, Clock.systemUTC());
	}

	/**
	 * Creates the statistics collector with an injected clock.
	 *
	 * @param client shared TrustDeck client
	 * @param properties connector configuration
	 * @param mapper JSON mapper used to persist statistics
	 * @param clock clock used for timestamps and rotation checks
	 */
	ConnectorStatistics(TrustDeckClient client, ConnectorProperties properties, ObjectMapper mapper, Clock clock) {
		this.client = client;
		this.properties = properties;
		this.mapper = mapper;
		this.clock = clock;
	}

	/**
	 * Starts periodic statistics collection when explicitly enabled.
	 *
	 * The first segment is opened before the sampler starts. Initialization failures
	 * disable optional statistics without affecting Kafka processing.
	 */
	@PostConstruct
	public void start() {
		if (!properties.getStatistics().isEnabled()) {
			return;
		}

		Duration interval = properties.getStatistics().getInterval();
		long intervalMillis = Math.max(1L, interval.toMillis());
		writerLock.lock();
		try {
			if (initialized || stopping) {
				return;
			}

			try {
				openSegmentLocked();
				initialized = true;
			} catch (IOException | RuntimeException exception) {
				disablePersistenceLocked("Unable to initialize connector statistics persistence", exception);
				return;
			}

			executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
			Thread thread = new Thread(runnable, "connector-statistics");
			thread.setDaemon(true);
			return thread;
		});
		executor.scheduleWithFixedDelay(this::sampleSafely, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
		} finally {
			writerLock.unlock();
		}
	}

	/**
	 * Records metrics for one listener batch without performing external I/O.
	 *
	 * The counters are updated atomically so listener threads can record batches
	 * concurrently with asynchronous statistics sampling and segment rotation.
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
	 * Stops sampling and finalizes the current segment within bounded shutdown time.
	 *
	 * An in-progress backend request is first allowed to finish, then interrupted
	 * when the executor wait expires. A task that finishes later observes the stopping
	 * state and cannot write to a finalized generator.
	 */
	@PreDestroy
	public void stop() {
		if (!properties.getStatistics().isEnabled() || !initialized) {
			return;
		}

		ScheduledExecutorService currentExecutor = executor;
		if (currentExecutor != null) {
			currentExecutor.shutdown();
			awaitExecutor(currentExecutor);
		}

		stopping = true;
		boolean locked = false;
		try {
			locked = writerLock.tryLock(SHUTDOWN_WRITER_WAIT.toMillis(), TimeUnit.MILLISECONDS);
			if (!locked) {
				log.error("Unable to acquire the connector statistics writer before shutdown; the hidden segment remains incomplete");
				return;
			}

			if (segment != null) {
				finalizeSegmentLocked("graceful application shutdown");
			}
		} catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			log.error("Interrupted while finalizing connector statistics during shutdown; the hidden segment remains incomplete");
		} finally {
			if (locked) {
				writerLock.unlock();
			}
		}
	}

	/**
	 * Waits for the sampling executor and interrupts a task that exceeds the bound.
	 *
	 * @param currentExecutor executor being stopped
	 */
	private void awaitExecutor(ScheduledExecutorService currentExecutor) {
		try {
			if (!currentExecutor.awaitTermination(SHUTDOWN_EXECUTOR_WAIT.toMillis(), TimeUnit.MILLISECONDS)) {
				currentExecutor.shutdownNow();
				currentExecutor.awaitTermination(SHUTDOWN_WRITER_WAIT.toMillis(), TimeUnit.MILLISECONDS);
			}
		} catch (InterruptedException exception) {
			currentExecutor.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Collects one backend sample and sends either its values or an error snapshot to the writer.
	 *
	 * TrustDeck calls happen before taking the writer lock so listener and file operations
	 * are not held up by backend latency.
	 */
	void sampleSafely() {
		if (stopping || persistenceDisabled) {
			return;
		}

		ProjectStatistics project = null;
		SystemStatistics system = null;
		String error = null;

		try {
			String projectName = properties.getTrustdeck().getProject();
			if (projectName != null && !projectName.isBlank()) {
				project = client.project(projectName).getStatistics();
			}
			system = client.system().getStatistics();
		} catch (Exception exception) {
			log.warn("Unable to collect TrustDeck connector statistics sample: {}", FailureClassifier.describe(exception));
			project = null;
			system = null;
			error = exception.getClass().getSimpleName() + ": " + exception.getMessage();
		}

		appendSample(project, system, error);
	}

	/**
	 * Appends one sample to the open segment and rotates it when a limit is reached.
	 *
	 * @param project project statistics, or {@code null} when unavailable
	 * @param system system statistics, or {@code null} when unavailable
	 * @param error backend sampling error, or {@code null} for a successful sample
	 */
	private void appendSample(ProjectStatistics project, SystemStatistics system, String error) {
		writerLock.lock();
		try {
			if (stopping || persistenceDisabled) {
				return;
			}
			if (segment == null) {
				try {
					openSegmentLocked();
				} catch (IOException | RuntimeException exception) {
					disablePersistenceLocked("Unable to initialize a rotated connector statistics segment", exception);
					return;
				}
			}

			Segment current = segment;
			Snapshot snapshot = new Snapshot(clock.instant(), segmentRecords(current), segmentBatches(current),
					segmentAverageBatchMillis(current), project, system, error);
			try {
				current.generator().writeObject(snapshot);
				current.generator().flush();
				if (rotationRequired(current)) {
					finalizeSegmentLocked(rotationReason(current));
				}
			} catch (IOException | RuntimeException exception) {
				disablePersistenceLocked("Unable to persist connector statistics sample", exception);
			}
		} finally {
			writerLock.unlock();
		}
	}

	/**
	 * Opens a hidden temporary segment and writes its JSON header.
	 *
	 * @throws IOException if the directory, temporary file, or generator cannot be created
	 */
	private void openSegmentLocked() throws IOException {
		Path directory = Path.of(properties.getStatistics().getOutputDirectory());
		Files.createDirectories(directory);

		Instant segmentStartedAt = clock.instant();
		Path temporary = Files.createTempFile(directory, ".connector-statistics-", ".tmp");
		Path target = uniqueTarget(directory, segmentStartedAt);
		OutputStream output = Files.newOutputStream(temporary);
		JsonGenerator generator = null;
		try {
			generator = mapper.getFactory().createGenerator(output);
			generator.setCodec(mapper);
			generator.useDefaultPrettyPrinter();
			generator.writeStartObject();
			generator.writeObjectField("startedAt", segmentStartedAt);
			generator.writeArrayFieldStart("samples");
			generator.flush();
			segment = new Segment(segmentStartedAt, records.get(), batches.get(), batchNanos.get(), temporary, target,
					generator);
		} catch (IOException | RuntimeException exception) {
			if (generator != null) {
				try {
					generator.close();
				} catch (IOException closeException) {
					exception.addSuppressed(closeException);
				}
			} else {
				try {
					output.close();
				} catch (IOException closeException) {
					exception.addSuppressed(closeException);
				}
			}
			throw exception;
		}
	}

	/**
	 * Selects a final filename containing the full-precision segment start timestamp.
	 *
	 * @param directory statistics output directory
	 * @param segmentStartedAt segment start time
	 * @return unused final segment path
	 */
	private Path uniqueTarget(Path directory, Instant segmentStartedAt) {
		String timestamp = segmentStartedAt.toString().replace(':', '-');
		Path target = directory.resolve("connector-statistics-" + timestamp + ".json");
		int suffix = 1;
		while (Files.exists(target)) {
			target = directory.resolve("connector-statistics-" + timestamp + "-" + suffix++ + ".json");
		}
		return target;
	}

	/**
	 * Determines whether the current segment reached either configured rotation limit.
	 *
	 * @param current current open segment
	 * @return whether age or flushed byte size reached its limit
	 * @throws IOException if the temporary file size cannot be read
	 */
	private boolean rotationRequired(Segment current) throws IOException {
		Duration age = Duration.between(current.startedAt(), clock.instant());
		DataSize maxSize = properties.getStatistics().getMaxFileSize();
		return !age.isNegative() && age.compareTo(properties.getStatistics().getMaxFileAge()) >= 0
				|| Files.size(current.temporary()) >= maxSize.toBytes();
	}

	/**
	 * Returns the configured reason for the first rotation condition reached.
	 *
	 * @param current current open segment
	 * @return human-readable rotation reason
	 * @throws IOException if the temporary file size cannot be read
	 */
	private String rotationReason(Segment current) throws IOException {
		Duration age = Duration.between(current.startedAt(), clock.instant());
		if (!age.isNegative() && age.compareTo(properties.getStatistics().getMaxFileAge()) >= 0) {
			return "maximum age";
		}
		if (Files.size(current.temporary()) >= properties.getStatistics().getMaxFileSize().toBytes()) {
			return "maximum size";
		}
		return "configured rotation limit";
	}

	/**
	 * Finalizes the current segment and atomically publishes its completed JSON file.
	 *
	 * @param reason reason written to the operational log
	 */
	private void finalizeSegmentLocked(String reason) {
		Segment current = segment;
		segment = null;
		if (current == null) {
			return;
		}

		boolean closed = false;
		try {
			JsonGenerator generator = current.generator();
			generator.writeEndArray();
			generator.writeObjectField("finishedAt", clock.instant());
			generator.writeNumberField("records", segmentRecords(current));
			generator.writeNumberField("batches", segmentBatches(current));
			generator.writeNumberField("averageBatchMillis", segmentAverageBatchMillis(current));
			generator.writeEndObject();
			generator.close();
			closed = true;
			publish(current);
			log.info("Finalized connector statistics segment {} (reason: {})", current.target(), reason);
		} catch (IOException | RuntimeException exception) {
			if (!closed) {
				try {
					current.generator().close();
				} catch (IOException closeException) {
					exception.addSuppressed(closeException);
				}
			}
			disablePersistenceLocked("Unable to finalize connector statistics segment", exception);
		}
	}

	/**
	 * Publishes a closed temporary segment using an atomic move where supported.
	 *
	 * @param current closed segment to publish
	 * @throws IOException if the move cannot be completed
	 */
	private void publish(Segment current) throws IOException {
		try {
			Files.move(current.temporary(), current.target(), StandardCopyOption.ATOMIC_MOVE,
					StandardCopyOption.REPLACE_EXISTING);
		} catch (AtomicMoveNotSupportedException exception) {
			Files.move(current.temporary(), current.target(), StandardCopyOption.REPLACE_EXISTING);
		}
	}

	/**
	 * Disables persistence after one failure and closes the unusable open generator.
	 *
	 * @param message safe operational failure description
	 * @param exception persistence exception
	 */
	private void disablePersistenceLocked(String message, Exception exception) {
		if (persistenceDisabled) {
			return;
		}
		persistenceDisabled = true;
		Segment current = segment;
		segment = null;
		if (current != null) {
			try {
				current.generator().close();
			} catch (IOException closeException) {
				exception.addSuppressed(closeException);
			}
		}
		log.error("{}: {}", message, exception.getClass().getSimpleName());
	}

	/**
	 * Calculates the segment-relative record count.
	 *
	 * @param current current segment
	 * @return records observed since the segment began
	 */
	private long segmentRecords(Segment current) {
		return records.get() - current.recordBaseline();
	}

	/**
	 * Calculates the segment-relative batch count.
	 *
	 * @param current current segment
	 * @return batches observed since the segment began
	 */
	private long segmentBatches(Segment current) {
		return batches.get() - current.batchBaseline();
	}

	/**
	 * Calculates the segment-relative average listener batch duration.
	 *
	 * @param current current segment
	 * @return average batch duration in milliseconds, or {@code 0.0} without batches
	 */
	private double segmentAverageBatchMillis(Segment current) {
		long segmentBatches = segmentBatches(current);
		return segmentBatches == 0 ? 0.0
				: (batchNanos.get() - current.batchNanosBaseline()) / 1_000_000.0 / segmentBatches;
	}

	/**
	 * Represents one open streamed statistics segment.
	 *
	 * @param startedAt segment start time
	 * @param recordBaseline process record counter at segment start
	 * @param batchBaseline process batch counter at segment start
	 * @param batchNanosBaseline process batch duration counter at segment start
	 * @param temporary hidden temporary file
	 * @param target completed public file
	 * @param generator open JSON generator
	 */
	private record Segment(Instant startedAt, long recordBaseline, long batchBaseline, long batchNanosBaseline,
			Path temporary, Path target, JsonGenerator generator) {
	}

	/**
	 * Represents one periodic connector and TrustDeck backend statistics sample.
	 *
	 * A failed backend sample contains segment-relative connector counters and an error
	 * description while the backend statistics values remain {@code null}.
	 *
	 * @param sampledAt time at which the sample was collected
	 * @param records source records received since the segment started
	 * @param batches listener batches received since the segment started
	 * @param averageBatchMillis average listener batch processing duration in milliseconds
	 * @param project project statistics, if available
	 * @param system system statistics, if available
	 * @param error sampling error description, if collection failed
	 */
	public record Snapshot(Instant sampledAt, long records, long batches, double averageBatchMillis,
			ProjectStatistics project, SystemStatistics system, String error) {
	}
}
