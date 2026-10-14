/*
 * TrustDeck Kafka Connector
 * Copyright 2026 Armin Müller
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.trustdeck.connector.kafka.statistics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.unit.DataSize;
import org.trustdeck.client.ProjectScope;
import org.trustdeck.client.TrustDeckClient;
import org.trustdeck.client.model.ProjectStatistics;
import org.trustdeck.client.model.SystemStatistics;
import org.trustdeck.client.service.SystemOperations;
import org.trustdeck.connector.kafka.config.ConnectorProperties;

/**
 * Verifies streamed statistics persistence, rotation, failure handling, and shutdown.
 *
 * @author Armin Müller
 */
class ConnectorStatisticsTest {

	/** Temporary root for each test's statistics output. */
	@TempDir
	Path temporaryDirectory;

	/**
	 * Statistics remain completely inactive when disabled.
	 */
	@Test
	void disabledStatisticsCreateNoExecutorOrFiles() throws Exception {
		ConnectorProperties properties = properties(false, Duration.ofDays(1), DataSize.ofMegabytes(256));
		Path output = temporaryDirectory.resolve("disabled");
		properties.getStatistics().setOutputDirectory(output.toString());
		ConnectorStatistics statistics = statistics(properties, Clock.fixed(Instant.EPOCH, ZoneId.of("UTC")));

		statistics.start();
		statistics.recordBatch(4, 1_000_000);
		statistics.stop();

		assertFalse(Files.exists(output));
		assertNull(ReflectionTestUtils.getField(statistics, "executor"));
	}

	/**
	 * A started segment is hidden until graceful shutdown publishes valid JSON.
	 */
	@Test
	void gracefulShutdownPublishesTheActiveTemporarySegment() throws Exception {
		ConnectorProperties properties = properties(true, Duration.ofDays(1), DataSize.ofMegabytes(256));
		ConnectorStatistics statistics = statistics(properties, Clock.systemUTC());

		statistics.start();
		assertEquals(1, temporaryFiles().size());
		statistics.stop();

		assertEquals(1, completedFiles().size());
		assertEquals(0, temporaryFiles().size());
		assertTrue(read(completedFiles().get(0)).isObject());
	}

	/**
	 * Segment schema and counters retain the existing field names and value types.
	 */
	@Test
	void completedSchemaContainsRelativeCountersAndSamples() throws Exception {
		MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
		ConnectorProperties properties = properties(true, Duration.ofDays(1), DataSize.ofMegabytes(256));
		ConnectorStatistics statistics = statistics(properties, clock);
		statistics.start();
		statistics.recordBatch(3, 2_000_000);
		statistics.sampleSafely();
		statistics.recordBatch(2, 4_000_000);
		statistics.stop();

		JsonNode run = read(completedFiles().get(0));
		assertTrue(run.has("startedAt"));
		assertTrue(run.has("finishedAt"));
		assertTrue(run.get("records").isIntegralNumber());
		assertTrue(run.get("batches").isIntegralNumber());
		assertTrue(run.get("averageBatchMillis").isFloatingPointNumber());
		assertTrue(run.get("samples").isArray());
		assertEquals(5, run.get("records").longValue());
		assertEquals(2, run.get("batches").longValue());
		assertEquals(3.0, run.get("averageBatchMillis").doubleValue());
		assertEquals(1, run.get("samples").size());
		assertNotNull(run.get("samples").get(0).get("project"));
		assertNotNull(run.get("samples").get(0).get("system"));
	}

	/**
	 * Multiple backend samples are appended in order without rewriting prior samples.
	 */
	@Test
	void successfulSamplesRemainOrdered() throws Exception {
		MutableClock clock = new MutableClock(Instant.EPOCH);
		ConnectorProperties properties = properties(true, Duration.ofDays(1), DataSize.ofMegabytes(256));
		ConnectorStatistics statistics = statistics(properties, clock);
		statistics.start();
		statistics.sampleSafely();
		clock.advance(Duration.ofSeconds(1));
		statistics.sampleSafely();
		statistics.stop();

		JsonNode samples = read(completedFiles().get(0)).get("samples");
		assertEquals(2, samples.size());
		assertNotNull(samples.get(0).get("sampledAt"), samples.toString());
		assertEquals("1970-01-01T00:00:00Z", samples.get(0).get("sampledAt").textValue());
		assertEquals("1970-01-01T00:00:01Z", samples.get(1).get("sampledAt").textValue());
	}

	/**
	 * A backend failure becomes one error snapshot and later sampling continues.
	 */
	@Test
	void backendFailureDoesNotStopLaterSamples() throws Exception {
		MutableClock clock = new MutableClock(Instant.EPOCH);
		ConnectorProperties properties = properties(true, Duration.ofDays(1), DataSize.ofMegabytes(256));
		TrustDeckClient client = mock(TrustDeckClient.class);
		ProjectScope project = mock(ProjectScope.class);
		SystemOperations system = mock(SystemOperations.class);
		
		when(client.project("project")).thenReturn(project);
		when(client.system()).thenReturn(system);
		when(project.getStatistics()).thenThrow(new IllegalStateException("backend payload")).thenReturn(new ProjectStatistics());
		when(system.getStatistics()).thenReturn(new SystemStatistics());
		
		ConnectorStatistics statistics = new ConnectorStatistics(client, properties, mapper(), clock);
		statistics.start();
		statistics.sampleSafely();
		statistics.sampleSafely();
		statistics.stop();

		JsonNode samples = read(completedFiles().get(0)).get("samples");
		assertEquals(2, samples.size());
		assertTrue(samples.get(0).get("error").textValue().contains("IllegalStateException"));
		assertTrue(samples.get(0).get("project").isNull());
		assertTrue(samples.get(0).get("system").isNull());
		assertTrue(samples.get(1).get("error").isNull());
	}

	/**
	 * Advancing the injected clock rotates by age and the following sample opens a new segment.
	 */
	@Test
	void ageRotationPublishesOldSegmentAndLazilyStartsNext() throws Exception {
		MutableClock clock = new MutableClock(Instant.EPOCH);
		ConnectorProperties properties = properties(true, Duration.ofHours(1), DataSize.ofMegabytes(256));
		ConnectorStatistics statistics = statistics(properties, clock);
		
		statistics.start();
		statistics.sampleSafely();
		clock.advance(Duration.ofHours(1).plusNanos(1));
		statistics.sampleSafely();
		assertEquals(1, completedFiles().size());
		statistics.sampleSafely();
		statistics.stop();

		assertEquals(2, completedFiles().size());
		assertEquals(2, read(completedFiles().get(0)).get("samples").size());
		assertEquals(1, read(completedFiles().get(1)).get("samples").size());
	}

	/**
	 * A small size limit rotates after a complete sample and never splits its JSON.
	 */
	@Test
	void sizeRotationKeepsEachSnapshotWhole() throws Exception {
		MutableClock clock = new MutableClock(Instant.EPOCH);
		ConnectorProperties properties = properties(true, Duration.ofDays(1), DataSize.ofBytes(1));
		ConnectorStatistics statistics = statistics(properties, clock);
		
		statistics.start();
		statistics.sampleSafely();
		assertEquals(1, completedFiles().size());
		statistics.sampleSafely();
		statistics.stop();

		assertEquals(2, completedFiles().size());
		assertEquals(1, read(completedFiles().get(0)).get("samples").size());
		assertEquals(1, read(completedFiles().get(1)).get("samples").size());
	}

	/**
	 * Segment baselines change on rotation without resetting process counters.
	 */
	@Test
	void countersAreRelativeToEachSegment() throws Exception {
		MutableClock clock = new MutableClock(Instant.EPOCH);
		ConnectorProperties properties = properties(true, Duration.ofHours(1), DataSize.ofMegabytes(256));
		ConnectorStatistics statistics = statistics(properties, clock);
		
		statistics.start();
		statistics.recordBatch(5, 5_000_000);
		statistics.sampleSafely();
		clock.advance(Duration.ofHours(1));
		statistics.sampleSafely();
		statistics.sampleSafely();
		statistics.recordBatch(4, 4_000_000);
		statistics.sampleSafely();
		statistics.stop();

		List<Path> files = completedFiles();
		assertEquals(2, files.size());
		assertEquals(5, read(files.get(0)).get("records").longValue());
		assertEquals(4, read(files.get(1)).get("records").longValue());
	}

	/**
	 * A zero-record segment is finalized as valid JSON with zero counters.
	 */
	@Test
	void zeroRecordSegmentIsValid() throws Exception {
		ConnectorProperties properties = properties(true, Duration.ofDays(1), DataSize.ofMegabytes(256));
		ConnectorStatistics statistics = statistics(properties, Clock.systemUTC());
		statistics.start();
		statistics.sampleSafely();
		statistics.stop();

		JsonNode run = read(completedFiles().get(0));
		assertEquals(0, run.get("records").longValue());
		assertEquals(0, run.get("batches").longValue());
		assertEquals(0.0, run.get("averageBatchMillis").doubleValue());
		assertTrue(run.get("samples").isArray());
	}

	/**
	 * A sampling task finishing after shutdown cannot append to the finalized file.
	 */
	@Test
	void shutdownPreventsLateSampleWrites() throws Exception {
		ConnectorProperties properties = properties(true, Duration.ofDays(1), DataSize.ofMegabytes(256));
		TrustDeckClient client = mock(TrustDeckClient.class);
		SystemOperations system = mock(SystemOperations.class);
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		
		when(client.system()).thenReturn(system);
		when(system.getStatistics()).thenAnswer(invocation -> {
			entered.countDown();
			release.await(5, TimeUnit.SECONDS);
			return new SystemStatistics();
		});
		
		properties.getTrustdeck().setProject(null);
		ConnectorStatistics statistics = new ConnectorStatistics(client, properties, mapper(), Clock.systemUTC());
		statistics.start();
		Thread sampler = new Thread(statistics::sampleSafely);
		sampler.start();
		assertTrue(entered.await(5, TimeUnit.SECONDS));
		statistics.stop();
		release.countDown();
		sampler.join(5_000);

		assertFalse(sampler.isAlive());
		assertEquals(0, read(completedFiles().get(0)).get("samples").size());
	}

	/**
	 * A directory initialization failure disables persistence but not counter updates.
	 */
	@Test
	void initializationFailureDoesNotPublishInvalidFile() throws Exception {
		Path output = temporaryDirectory.resolve("not-a-directory");
		Files.writeString(output, "occupied");
		ConnectorProperties properties = properties(true, Duration.ofDays(1), DataSize.ofMegabytes(256));
		properties.getStatistics().setOutputDirectory(output.toString());
		ConnectorStatistics statistics = statistics(properties, Clock.systemUTC());

		statistics.start();
		statistics.recordBatch(2, 1_000_000);
		statistics.sampleSafely();
		statistics.stop();

		assertTrue(Files.isRegularFile(output));
		try (Stream<Path> paths = Files.list(temporaryDirectory)) {
			assertEquals(0, paths.filter(path -> path.getFileName().toString().endsWith(".json")).count());
		}
	}

	/**
	 * A write failure disables persistence once and never publishes an invalid file.
	 */
	@Test
	void writeFailureDisablesFurtherPersistence() throws Exception {
		ConnectorProperties properties = properties(true, Duration.ofDays(1), DataSize.ofMegabytes(256));
		TrustDeckClient client = mock(TrustDeckClient.class);
		ProjectScope project = mock(ProjectScope.class);
		SystemOperations system = mock(SystemOperations.class);
		
		when(client.project("project")).thenReturn(project);
		when(client.system()).thenReturn(system);
		when(project.getStatistics()).thenReturn(new ProjectStatistics());
		when(system.getStatistics()).thenReturn(new SystemStatistics());
		
		ObjectMapper failingMapper = mapper();
		SimpleModule module = new SimpleModule();
		module.addSerializer(ProjectStatistics.class, new JsonSerializer<>() {
			@Override
			public void serialize(ProjectStatistics value, JsonGenerator generator, SerializerProvider serializers)
					throws IOException {
				throw new IOException("synthetic write failure");
			}
		});
		
		failingMapper.registerModule(module);
		ConnectorStatistics statistics = new ConnectorStatistics(client, properties, failingMapper, Clock.systemUTC());

		statistics.start();
		statistics.sampleSafely();
		statistics.sampleSafely();
		statistics.stop();

		assertEquals(0, completedFiles().size());
		verify(project, times(1)).getStatistics();
	}

	/**
	 * Creates connector properties with statistics enabled or disabled and explicit
	 * rotation limits for the temporary test output directory.
	 *
	 * @param enabled whether statistics collection is enabled
	 * @param age maximum age of a statistics segment
	 * @param size maximum configured size of a statistics segment
	 * @return configured connector properties
	 */
	private ConnectorProperties properties(boolean enabled, Duration age, DataSize size) {
		ConnectorProperties properties = new ConnectorProperties();
		properties.getStatistics().setEnabled(enabled);
		properties.getStatistics().setInterval(Duration.ofDays(1));
		properties.getStatistics().setOutputDirectory(temporaryDirectory.resolve("statistics").toString());
		properties.getStatistics().setMaxFileAge(age);
		properties.getStatistics().setMaxFileSize(size);
		properties.getTrustdeck().setProject("project");
		
		return properties;
	}

	/**
	 * Creates a statistics collector with mocked project and system statistics
	 * services using the supplied connector properties and clock.
	 *
	 * @param properties connector statistics configuration
	 * @param clock clock used for deterministic timestamps and rotation checks
	 * @return statistics collector backed by mocked TrustDeck services
	 */
	private ConnectorStatistics statistics(ConnectorProperties properties, Clock clock) {
		TrustDeckClient client = mock(TrustDeckClient.class);
		ProjectScope project = mock(ProjectScope.class);
		SystemOperations system = mock(SystemOperations.class);
		
		when(client.project("project")).thenReturn(project);
		when(client.system()).thenReturn(system);
		when(project.getStatistics()).thenReturn(new ProjectStatistics());
		when(system.getStatistics()).thenReturn(new SystemStatistics());
		
		return new ConnectorStatistics(client, properties, mapper(), clock);
	}

	/**
	 * Creates the same Java-time-capable mapper used by the streaming writer.
	 */
	private ObjectMapper mapper() {
		return new ObjectMapper().registerModule(new JavaTimeModule())
				.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
	}

	/**
	 * Reads one completed statistics segment as JSON.
	 *
	 * @param path completed segment file
	 * @return parsed JSON tree
	 * @throws IOException if the file cannot be read or parsed
	 */
	private JsonNode read(Path path) throws IOException {
		return mapper().readTree(path.toFile());
	}

	/**
	 * Lists completed statistics segment files in deterministic filename order.
	 *
	 * @return sorted completed segment paths
	 * @throws IOException if the statistics directory cannot be read
	 */
	private List<Path> completedFiles() throws IOException {
		try (Stream<Path> paths = Files.list(temporaryDirectory.resolve("statistics"))) {
			return paths.filter(path -> path.getFileName().toString().endsWith(".json")).sorted().toList();
		}
	}

	/**
	 * Lists active hidden temporary segment files in deterministic filename order.
	 *
	 * @return sorted temporary segment paths
	 * @throws IOException if the statistics directory cannot be read
	 */
	private List<Path> temporaryFiles() throws IOException {
		try (Stream<Path> paths = Files.list(temporaryDirectory.resolve("statistics"))) {
			return paths.filter(path -> path.getFileName().toString().endsWith(".tmp")).sorted().toList();
		}
	}

	/**
	 * Mutable clock for deterministic age and timestamp assertions.
	 */
	private static final class MutableClock extends Clock {

		/** Current instant returned by this clock. */
		private Instant instant;

		/**
		 * Creates a mutable UTC clock initialized at the supplied instant.
		 *
		 * @param instant initial clock instant
		 */
		private MutableClock(Instant instant) {
			this.instant = instant;
		}

		/**
		 * Advances the clock without waiting in real time.
		 *
		 * @param duration amount of simulated time to advance
		 */
		private void advance(Duration duration) {
			instant = instant.plus(duration);
		}

		@Override
		public ZoneId getZone() {
			return ZoneId.of("UTC");
		}

		@Override
		public Clock withZone(ZoneId zone) {
			return this;
		}

		@Override
		public Instant instant() {
			return instant;
		}
	}
}
