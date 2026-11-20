# TrustDeck Kafka Connector

Version 1.0.0

The TrustDeck Kafka Connector consumes one Kafka topic, applies one configured operation to every record, and calls TrustDeck through the [TrustDeck Java client](https://central.sonatype.com/artifact/org.trustdeck/client). It does not create projects, entity types, domains, schemas, permissions, consumer groups, or recovery-topic consumers; these must exist beforehand.

## Requirements

- Java 21 and Maven 3.8.7 or newer (the Docker build uses Maven 3.9.11)
- Docker with Compose
- TrustDeck backend v2.5.0 or newer, with an existing project, entity type, and domain as needed by the operation
- Kafka with ACLs for the source topic and the recovery topics
- A confidential Keycloak client with service accounts and client-credentials access

TrustDeck only accepts access tokens whose `aud` claim contains `trustdeck`. In the Keycloak admin console, open **Clients → your client → Client scopes → your-client-dedicated**, choose **Configure a new mapper → Audience**, set **Included Client Audience** to `trustdeck`, and enable **Add to access token**. Without this mapper, every TrustDeck request fails with 401. The JWT subject must be the UUID of the TrustDeck service-account user, which needs the permissions listed under [Permissions](#permissions). Keycloak realm-admin roles alone do not grant TrustDeck permissions.

## Build

The connector depends on `org.trustdeck:client:2.1.0` from Maven Central; no extra repository or credentials are needed.

```sh
mvn clean verify
docker build --file docker/Dockerfile -t trustdeck-kafka-connector:1.0.0 .
```

Never put credentials in the POM, the image, build arguments, or the Git history.

Behind an HTTP proxy, export `HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY` (or the lowercase variants) in the shell that runs `docker compose build`. Compose passes them as build arguments; `apt-get` uses them directly, and Maven receives a temporary `settings.xml` generated from them (`docker/maven-proxy.sh`). They are not stored in the image. For a private Maven repository, pass a complete `settings.xml` instead, which takes precedence:

```sh
docker build -f docker/Dockerfile --secret id=maven_settings,src=$HOME/.m2/settings.xml -t trustdeck-kafka-connector:1.0.0 .
```

## Configuration

Copy `.env.example` to `.env` and fill it in. The classpath `application.yaml` requires `TRUSTDECK_OPERATION`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, the backend and token URLs, and the service-account credentials. It contains no sample mappings. `KAFKA_USERNAME` and `KAFKA_PASSWORD` are escaped into a JAAS string when `KAFKA_SASL_JAAS_CONFIG` is absent. Truststores are mounted read-only; TLS hostname and certificate validation stay enabled.

The `config/instance` directory holds deployment-specific overrides: Compose mounts its optional `application.yaml` at `/app/config/application.yaml` and its `truststores` directory read-only, and `SPRING_CONFIG_ADDITIONAL_LOCATION` imports that directory. It is not packaged into the application and contains no credentials.

To run without Docker, export the environment first, since Maven does not read `.env`:

```sh
set -a
source ./.env
set +a
mvn spring-boot:run
```

`server.shutdown: graceful` is set because the connector acknowledges records manually and publishes recovery records; graceful shutdown lets in-flight operations finish within the 45-second Compose stop grace period.

## Operations

Each instance runs exactly one `TRUSTDECK_OPERATION`:

`CREATE_PSEUDONYM`, `UPDATE_PSEUDONYM`, `DELETE_PSEUDONYM`, `CREATE_ENTITY`, `UPDATE_ENTITY`, `DELETE_ENTITY`, `CREATE_ENTITY_AND_PSEUDONYM`

Updates require explicit old selectors and use the standard endpoint, unless mapped identifier, domain, or value changes require `updateComplete`; nothing is moved or regenerated implicitly. Deletes use one exact pseudonym selector and never the domain-wide batch delete. Entity updates and deletes require a mapped TrustDeck UUID. Combined creation keeps a completed entity stage for bounded replay and never rolls it back.

The table lists the minimum operation-specific settings; the common Kafka, URL, OAuth, and recovery settings are always required.

| Operation | Required settings | Optional or additional settings |
|---|---|---|
| `CREATE_PSEUDONYM` | `TRUSTDECK_DOMAIN` or `KAFKA_MESSAGE_SOURCE_SYSTEM_DESCRIPTOR`; identifier and id-type descriptors | Pseudonym value, validity, and timestamp settings |
| `UPDATE_PSEUDONYM` | Domain as above; identifier and id-type descriptors; old identifier and old id-type descriptors | New identifier/type/value, old/new domain, validity, and provenance mappings |
| `DELETE_PSEUDONYM` | Pseudonym descriptor, or identifier plus id-type descriptors | Domain as above |
| `CREATE_ENTITY` | `TRUSTDECK_PROJECT`; fixed or mapped entity type | `connector.mapping.common.entity-fields` in the instance YAML supplies entity data |
| `UPDATE_ENTITY` | Entity settings above; entity ID descriptor | `connector.mapping.common.entity-fields` supplies replacement data; per-field `required` and `constant` rules |
| `DELETE_ENTITY` | Entity settings above; entity ID descriptor | No entity field mappings are needed |
| `CREATE_ENTITY_AND_PSEUDONYM` | Entity settings above; domain | `connector.mapping.common.entity-fields` supplies entity data; pseudonym value, validity, and provenance mappings |

## Mappings

The descriptor variables are the `KAFKA_MESSAGE_*_DESCRIPTOR` entries in `.env.example`. Common mappings bind to `connector.mapping.common`, update selectors and replacements to `connector.mapping.update-pseudonym`. Entity field mappings are YAML list entries under `connector.mapping.common.entity-fields`, not environment variables.

Mapping values are JSON Pointers; a bare top-level name is accepted as shorthand. Only the mappings required by the selected operation are needed; others may be omitted. A nonblank `TRUSTDECK_DOMAIN` takes precedence over the domain message mapping. `TRUSTDECK_PROJECT` and `TRUSTDECK_ENTITY_TYPE` are ignored for pseudonym-only operations. Entity operations require a fixed project and either a fixed or mapped entity type. Entity data preserves numbers, booleans, objects, arrays, and strings, including leading zeros.

A record is rejected if it is a tombstone, a root array or primitive, invalid UTF-8 or JSON, has a trailing JSON value, is oversized, or a required path is missing, null, blank, or of the wrong type.

`src/main/resources/example-pseudonymization.yaml` is an opt-in synthetic example that is not loaded during normal startup:

```sh
mvn spring-boot:run -Dspring-boot.run.arguments=--spring.config.additional-location=classpath:/example-pseudonymization.yaml
```

It maps `/c_source_domain` to the domain, `/c_value` to the identifier, `/c_type_of_pseudonym` to the id type, `/c_pseudonym` to the imported value, and `/t_timestamp` to `validFrom`. Timestamps use the offset-free format `uuuu-MM-dd HH:mm:ss.SSS` in `Europe/Berlin`; `2025-10-09 21:08:10.175` is sent to the backend as the UTC wall-clock time `2025-10-09T19:08:10.175`.

## Permissions

Grant the service-account user only what the selected operation needs:

- `project:read` and `type:read`, plus the enabled entity CRUD permissions, for entity operations
- `domain:read` on each configured domain
- `pseudonym:create` for pseudonym creation, and `pseudonym:create-batch` when batching is enabled
- `pseudonym:read` and `pseudonym:delete` for exact lookup and deletion; `pseudonym:read` is also needed to explain rejected imports
- `pseudonym:update` and `pseudonym:update-complete` when the corresponding update paths are enabled
- `entity:list-pseudonyms` for combined creation
- `pseudonym:read` and `pseudonym:link` only where linked-pseudonym verification is configured

Do not grant domain, project, or type creation, global administration, `entity:resolve-linkage`, `pseudonym:read-batch`, or `pseudonym:delete-batch`. Restrict DLT and review topic ACLs and retention separately.

## Delivery and Recovery

Auto-commit is disabled. A source offset is acknowledged only after the TrustDeck call succeeds or a DLT or review record has been confirmed by the broker. The DLT defaults to `<source>.DLT` and the linkage review topic to `<source>.REVIEW`; all three topic names must differ. Recovery records contain the base64-encoded original key and value, source coordinates, selected safe metadata, operation, target, sanitized failure category, attempts, and completed stage. They never contain tokens, passwords, cookies, arbitrary headers, or raw logs. Restrict topic ACLs and retention, since business payloads may be sensitive.

HTTP calls use finite client timeouts. Mapping failures go directly to the DLT. Transient failures are retried a bounded number of times. A definitive client error (4xx except 401, 403, 408, and 429) is not retried and goes to the DLT as `REJECTED_BY_TRUSTDECK`. A `RecordLinkageConflictException` from the client goes to review as `PENDING_LINKAGE_REVIEW`.

For imported pseudonyms (`CREATE_PSEUDONYM` with a mapped pseudonym), TrustDeck answers an exact duplicate with 200 and every other rejection with a body-less 422. A supplied pseudonym is never replaced: one that is already taken is rejected, also in domains with a random algorithm, and the same pair can be imported into several domains. The connector explains a 422 by looking up the pseudonym and the identifier in the target domain:

- an identical existing mapping counts as success;
- a contradicting mapping goes to review with status `PENDING_CONFLICT_REVIEW` and category `PSEUDONYM_CONFLICT_IDENTIFIER_HAS_OTHER_PSEUDONYM` or `PSEUDONYM_CONFLICT_PSEUDONYM_ASSIGNED_ELSEWHERE`;
- anything else stays `REJECTED_BY_TRUSTDECK`.

Each poll is split into batches of consecutive records with the same partition, domain, and validity settings, up to `TRUSTDECK_BATCH_SIZE`. One consumer per partition (`KAFKA_CONCURRENCY` equal to the partition count) keeps batches as large as `KAFKA_MAX_POLL_RECORDS`. A batch request reports duplicates and conflicts only by omitting them (206) or by rejecting the whole batch when nothing was created (422), so every record not confirmed by a batch is processed individually.

TrustDeck answers 403 for a domain that does not exist, so a missing or unauthorized domain pauses consumption; the log names the domain, and consumption resumes once the domain exists and is readable. During an outage the connector pauses consumption instead of draining records to the DLT; the readiness indicator reports the pause while liveness stays up. Docker restart policies do not restart an unhealthy but running container, so an external watchdog must not undo intentional pauses with a restart loop.

There is no exactly-once guarantee for HTTP effects. If the connector stops after a successful TrustDeck call or recovery publication but before the offset commit, the effect or publication may happen again after a restart. Use the source topic, partition, and offset for downstream deduplication, and never replay a recovery record blindly through the normal listener.

### Notifications

Optional mail notifications use Spring Boot's standard `spring.mail.*` settings, plus the connector-specific `SMTP_FROM`, `SMTP_TO`, `SUBJECT_PREFIX`, and `SMTP_COOLDOWN`. `SMTP_TO` is empty by default, which disables mail entirely. An unauthenticated relay needs only host and port; authenticated SMTP additionally needs username, password, and `SMTP_AUTH=true`. A mail failure never acknowledges data.

### Manual Replay

Manual replay is an explicit operator procedure. Set `CONNECTOR_REPLAY_ENABLED=true`, restrict `/internal/replay` to the operator network, and POST the complete recovery envelope JSON to it. The endpoint uses the envelope's preserved `valueBase64`; replacement bytes are not accepted.

Before replaying, stop or isolate the normal source consumer, keep a copy of the envelope, and confirm that its `status` is `FAILED_DLT`, its `operation` and `resolvedTarget` match the instance, and its `failureCategory` is not `UNKNOWN_OUTCOME`. Do not replay `PENDING_LINKAGE_REVIEW` until the linkage has been reconciled.

For `CREATE_ENTITY_AND_PSEUDONYM`, replay is only valid when the envelope has `completedStage: ENTITY_CREATED` and a `completedUuid`; the endpoint restores that UUID and runs the second stage, or rejects the envelope. Plain republishing cannot carry this metadata and must not be used for combined operations.

A non-HTTP replay tool must use a dedicated process with the same operation and mapping configuration, a distinct consumer group, and restricted ACLs, and must never point at the production source topic.

## Running with Docker

`connector.sh` wraps the Compose calls. `standalone` uses only the base Compose file; `trustdeck` adds an overlay that joins the existing external `trustdeck` network, so `.env` can address `trustdeck-backend` and `trustdeck-keycloak` by container name.

| Command | Effect |
|---|---|
| `start` | Checks `.env` and the instance files, prepares the statistics directory, builds the image, recreates the container, and waits for readiness |
| `stop`, `restart`, `logs`, `build` | As named |
| `status` | Container status plus the last pause and recovery events, including the cause and, for TrustDeck failures, the domain |
| `progress [since-ms] [seconds]` | Throughput, estimated remaining time from the consumer lag, and DLT and review counts |

`progress` reads the consumer lag with a local Kafka CLI (`kafka-consumer-groups.sh` or `kafka-consumer-groups`) and otherwise uses a disposable `apache/kafka` container that is removed on exit. The DLT and review counts need `kcat`, `jq`, and `keytool` on the host.

```sh
./connector.sh trustdeck start
./connector.sh trustdeck logs
```

The equivalent plain Compose call is:

```sh
docker compose --env-file .env -p trustdeck-kafka-connector -f docker/compose.yaml -f docker/compose.trustdeck-network.yaml up -d
```

The base Compose file needs only an `.env`, the read-only instance YAML and truststore mounts, and a free health port. Health is exposed only at `/actuator/health/readiness` and `/actuator/health/liveness`. Use HTTPS URLs for the backend and Keycloak.

### Several Instances

Set `CONNECTOR_PROJECT` to run several instances side by side (default: `trustdeck-kafka-connector`); the container is named after the project. Instances started from the same checkout also need their own environment file, passed as `CONNECTOR_ENV_FILE` (default `.env`, relative to the current directory). Copy `.env.example` to, for example, `connector-b.env`; names ending in `.env` are excluded from Git and the image build context. Each file needs its own `HEALTH_HOST_PORT` (default 18080), `STATISTICS_HOST_DIR`, `KAFKA_GROUP_ID` (unless the instances should share the topic's partitions), and source and mapping settings. Truststores and `config/instance/application.yaml` are shared by all instances of a checkout.

```sh
CONNECTOR_PROJECT=trustdeck-connector-b ./connector.sh standalone start
CONNECTOR_PROJECT=trustdeck-connector-c CONNECTOR_ENV_FILE=connector-c.env ./connector.sh trustdeck start
```

With plain Compose, pass the same file both as `--env-file` and as `CONNECTOR_ENV_FILE`:

```sh
CONNECTOR_ENV_FILE=$PWD/connector-c.env docker compose --env-file connector-c.env -p trustdeck-connector-c -f docker/compose.yaml up -d
```

### Kafka Tuning

New consumer groups start at `earliest`; existing committed offsets take precedence. Inspect and reset offsets only while the consumer is stopped. Defaults are concurrency 2, 100 poll records, batch limits of 100 records and 256 KiB, a 5-second connect timeout, a 30-second read timeout, and three attempts. Keep `max.poll.interval.ms` above the worst-case sequential HTTP and retry time, or lower the poll size. Instances sharing a group divide the partitions between them; instances with different mappings or operations need distinct groups, or they will process records twice or miss them.

## Statistics

Kafka record and batch counters are maintained continuously. When enabled, TrustDeck project and system statistics are sampled every `CONNECTOR_STATISTICS_INTERVAL` and streamed into a hidden temporary file in the output directory. The file is finalized and published atomically after 24 hours, after reaching 256 MiB, or on graceful shutdown; both limits are configurable. The size limit is a soft maximum, since a snapshot is never split between files. After an abrupt termination the current temporary file may be incomplete and is not published.

For a large import, a one-minute interval with daily rotation works well:

```dotenv
CONNECTOR_STATISTICS_ENABLED=true
CONNECTOR_STATISTICS_INTERVAL=1m
CONNECTOR_STATISTICS_OUTPUT_DIRECTORY=statistics
CONNECTOR_STATISTICS_MAX_FILE_AGE=1d
CONNECTOR_STATISTICS_MAX_FILE_SIZE=256MB
```

Each file contains `startedAt`, `finishedAt`, `records`, `batches`, `averageBatchMillis`, and `samples`; counters are relative to that file. The connector requests the lightweight variant of the TrustDeck statistics endpoints: table row counts are PostgreSQL estimates (`recordCountExact: false`) with a running insert counter per table (`insertedRecordCount`), and project entity and pseudonym counts are `null`. This avoids scanning large tables on every sample; insert rates follow from the difference between two samples.

In Docker, files are written to `/app/statistics`, mounted from `STATISTICS_HOST_DIR` (default `statistics/` in the repository root). The directory must be writable by UID 10001; `connector.sh` creates it with the right owner.

## Logging

The console log uses the TrustDeck backend format (`[LEVEL]<tab>yyyy-MM-dd HH:mm:ss,SSS Class:line message`, see `logback-spring.xml`). Third-party libraries log warnings only; set `LOGGING_LEVEL_ROOT=INFO` for Kafka client details, and `LOG_COLORS=never` for log collectors without ANSI support. A statistics sample that keeps failing is logged once at WARN and then at DEBUG.

## Testing

`mvn clean verify` runs the unit tests, the HTTP contract tests for all seven operations, the Testcontainers-based Kafka integration tests (Docker required), and a strict Javadoc check. The integration tests use disposable resources only.

Before going live, verify with synthetic resources: the service-account permissions listed above, DLT and review ACLs and retention, the Keycloak client credentials, the HTTPS truststore, readiness and liveness, and one real end-to-end operation.

## Limitations

- HTTP effects are at-least-once, not exactly-once (see [Delivery and Recovery](#delivery-and-recovery)).
- TrustDeck audit entries name the connector's service account, not the original Kafka producer.
- Record-linkage conflicts are sent to review and not resolved automatically.
- The backend uses timezone-free `LocalDateTime` values; source timestamps are converted to UTC wall-clock time.
- Identifier-only lookups are ambiguous in domains that allow several pseudonyms per identifier.
- Batch responses with status 206 cannot always be correlated for generated, repeated, or reduced results; the connector then reports an unknown outcome.

## License

This project is licensed under the Apache License 2.0; see [LICENSE](LICENSE). Direct dependencies: Spring Boot, Spring Kafka, Jackson, and the TrustDeck client (Apache-2.0); Lombok and Testcontainers (MIT); JUnit (EPL-2.0). To review all transitive licenses, run:

```sh
mvn org.codehaus.mojo:license-maven-plugin:2.7.0:add-third-party -Dlicense.includedScopes=compile,runtime
```

and inspect `target/generated-sources/license/THIRD-PARTY.txt`.
