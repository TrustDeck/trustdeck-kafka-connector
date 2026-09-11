# TrustDeck Kafka Connector

This Apache-2.0 application consumes one Kafka topic with one configured operation and calls TrustDeck through its Java client. It does not create projects, entity types, domains, schemas, permissions, consumer groups, or recovery-topic consumers.

## Run

Prerequisites are Java 21, Maven 3.9+, Docker/Compose, Kafka ACLs, and an existing TrustDeck project/type/domain. Use a confidential Keycloak client with service accounts and client-credentials access. Grant the exact resource permissions listed in `PLAN.md`; the JWT subject must be the TrustDeck service-account user's UUID. Keycloak realm-admin roles alone do not grant TrustDeck permissions.

The released client `1.0.1` is not in Maven Central. This repository uses the approved, unpublished client commit `5ca64d7` as `1.0.2-SNAPSHOT`:

```sh
git clone https://github.com/TrustDeck/trustdeck-client.git
git -C trustdeck-client checkout --detach 5ca64d7
mvn -f trustdeck-client/pom.xml clean install
./mvnw clean verify
```

For authenticated GitHub Packages instead, configure Maven `settings.xml` with a read token and change the client version to a published coordinate. Never put credentials in the POM, image, build args, or history. Docker clones and builds the pinned client commit in a disposable builder stage; it does not use the host Maven cache:

```sh
DOCKER_BUILDKIT=1 docker build --build-arg TRUSTDECK_CLIENT_REF=5ca64d7 -t trustdeck-kafka-connector:1.0.0 .
```

Maven does not source `.env`. In WSL:

```sh
vim .env
set -a
source ./.env
set +a
./mvnw spring-boot:run
```

The generic classpath `application.yaml` requires `TRUSTDECK_OPERATION`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, backend/token URLs, and service-account credentials. It has no sample mapping defaults. `KAFKA_USERNAME` and `KAFKA_PASSWORD` are escaped into a JAAS string when `KAFKA_SASL_JAAS_CONFIG` is absent. Truststore paths are mounted read-only; TLS hostname and certificate validation remain enabled.

## Mappings

Values are JSON Pointers, with a bare top-level name accepted as shorthand. Constant domain configuration wins over the message path. Pseudonym operations require only the fields relevant to that operation. Entity operations require a fixed project and either a fixed entity type or mapped entity-type path. Entity data uses explicit field mappings and preserves numbers, booleans, objects, arrays, and strings including leading zeros. A root array, tombstone, primitive, invalid UTF-8/JSON, trailing JSON value, oversized value, missing required path, null, blank, or wrong type is rejected.

`examples/sample-pseudonym.yaml` is synthetic/example-only. It maps `/c_source_domain` to the domain, `/c_value` to identifier, `/c_type_of_pseudonym` to id type, `/c_pseudonym` to imported value, and `/t_timestamp` to `validFrom`. Its offset-free format is `uuuu-MM-dd HH:mm:ss.SSS` in `Europe/Berlin`; `2025-10-09 21:08:10.175` becomes UTC wall-clock `2025-10-09T19:08:10.175` on the backend wire. Offset-bearing values are normalized by instant. Missing validity values remain absent for backend defaults. Ambiguous and nonexistent DST local times are rejected.

Supported operations are `CREATE_PSEUDONYM`, `UPDATE_PSEUDONYM`, `DELETE_PSEUDONYM`, `CREATE_ENTITY`, `UPDATE_ENTITY`, `DELETE_ENTITY`, and `CREATE_ENTITY_AND_PSEUDONYM`. Updates require explicit old selectors and use the standard endpoint unless mapped identifier/domain/value changes require `updateComplete`; nothing moves or regenerates implicitly. Deletes use one exact pseudonym selector and never the domain-wide delete batch. Entity update/delete require a mapped TrustDeck UUID. Combined creation preserves the completed entity stage for bounded replay and never rolls it back.

### Operation Configuration

Each instance has one fixed `TRUSTDECK_OPERATION`. The following settings are the minimum operation-specific mappings; common Kafka, URL, OAuth, and recovery settings remain required.

| Operation | Required settings | Optional or additional settings |
|---|---|---|
| `CREATE_PSEUDONYM` | `TRUSTDECK_DOMAIN` or `KAFKA_MESSAGE_SOURCE_SYSTEM_DESCRIPTOR`; identifier and id-type descriptors | Pseudonym value, validity, and timestamp settings |
| `UPDATE_PSEUDONYM` | Domain as above; identifier and id-type descriptors; old identifier and old id-type descriptors | New identifier/type/value, old/new domain, validity, and provenance mappings |
| `DELETE_PSEUDONYM` | Pseudonym descriptor, or identifier plus id-type descriptors | Domain as above |
| `CREATE_ENTITY` | `TRUSTDECK_PROJECT`; fixed or mapped entity type | `mapping.entity-fields` in instance YAML supplies entity data |
| `UPDATE_ENTITY` | Entity settings above; entity ID descriptor | `mapping.entity-fields` supplies replacement data; per-field `required` and `constant` rules |
| `DELETE_ENTITY` | Entity settings above; entity ID descriptor | No entity field mappings are needed |
| `CREATE_ENTITY_AND_PSEUDONYM` | Entity settings above; domain | `mapping.entity-fields` supplies entity data; pseudonym value, validity, and provenance mappings |

The descriptor environment variables are the `KAFKA_MESSAGE_*_DESCRIPTOR` entries in `.env.example`; the corresponding YAML properties are under `connector.mapping`. `TRUSTDECK_PROJECT` and `TRUSTDECK_ENTITY_TYPE` are ignored for pseudonym-only operations. A nonblank `TRUSTDECK_DOMAIN` overrides the domain message mapping. Entity field mappings are YAML list entries, not environment variables.

## Delivery and Recovery

Auto-commit is disabled. A source offset is acknowledged only after the TrustDeck call succeeds or a DLT/review envelope is broker-confirmed. DLT defaults to `<source>.DLT`; linkage review defaults to `<source>.REVIEW`; all three names must differ. DLT/review records retain base64 original key/value bytes, source coordinates, selected safe metadata, operation, target, sanitized category, attempts, and completed stage. They never contain tokens, passwords, cookies, arbitrary headers, or raw logs. Restrict topic ACLs and retention because business payloads may be sensitive.

HTTP calls use finite client timeouts. Record mapping failures go directly to DLT. Transient operation failures use bounded retries; a true client `RecordLinkageConflictException` goes to review as `PENDING_LINKAGE_REVIEW`, while an ordinary 409 goes to DLT. Review publication is terminal only after confirmation. There is no exactly-once HTTP claim. The crash window is after a successful TrustDeck call or confirmed recovery publication but before the source offset commit; a restart can perform the HTTP effect or publication again. Use source topic/partition/offset for downstream deduplication. Do not blindly replay an envelope to the normal listener.

### Manual Replay

There is no public replay endpoint or command in this application. Manual replay is an operator procedure using the internal `ManualReplayService` or an equivalent controlled tool. First stop or isolate the normal source consumer, preserve the envelope, and confirm that its `status` is `FAILED_DLT`, its `operation` and `resolvedTarget` match the instance being used, and its `failureCategory` is not `UNKNOWN_OUTCOME`. Do not replay `PENDING_LINKAGE_REVIEW` until linkage has been reconciled. Decode `valueBase64` to the original Kafka value bytes; do not reconstruct JSON or use the envelope JSON as the source record.

Republish those original bytes to a dedicated replay topic with the same operation and mapping configuration, a distinct consumer group, and restricted ACLs. Monitor the replay result and retain the recovery record separately; never point a replay instance at the production source topic. For `CREATE_ENTITY_AND_PSEUDONYM`, replay is valid only when the envelope has `completedStage: ENTITY_CREATED` and `completedUuid`; reconcile that UUID and run stage two, or investigate first. Ordinary republishing cannot carry this metadata by itself, so a tool that does not invoke the replay service must implement the same guarded stage-two procedure.

Production outage handling must pause consumption rather than drain records to DLT; the readiness health indicator reports a pause while liveness remains up. SMTP is optional and disabled with an empty `SMTP_TO`; mail failure never acknowledges data. The current minimal listener retains uncommitted data on failed publication. Docker restart policies do not restart an unhealthy-but-running container, so use an external watchdog carefully and do not erase intentional pauses with a restart loop.

## Kafka and Deployment

Start new groups at `earliest`; existing committed offsets take precedence. Inspect and reset offsets only with the consumer stopped, and never reset the production group during development. Conservative defaults are concurrency 2, 100 poll records, 100-record/256 KiB batch limits, 5-second connect timeout, 30-second read timeout, and three retries. Tune `max.poll.interval.ms` above the worst sequential HTTP and retry budget, or lower poll size. Instances sharing a group divide partitions; independent mappings or operations require distinct groups or they will double-process or omit work.

```sh
docker compose -p trustdeck-connector-a up -d
docker compose -p trustdeck-connector-b -f compose.yaml -f compose.trustdeck-network.yaml up -d
```

The base Compose file requires only an `.env`, read-only instance YAML/truststore mounts, and a free health port. The override joins an already existing external `trustdeck` network. It has no fixed container name or mandatory host port. Use HTTPS backend and Keycloak URLs, a valid `SMTP_FROM`, and finite SMTP settings. Health is exposed only at `/actuator/health/readiness` and `/actuator/health/liveness`.

## Verification and Limitations

Run `./mvnw clean verify`, `git diff --check`, `docker compose config`, and the Docker build above. Generate the dependency/license inventory with `./mvnw org.codehaus.mojo:license-maven-plugin:2.7.0:add-third-party -Dlicense.includedScopes=compile,runtime`; inspect `target/generated-sources/license/THIRD-PARTY.txt` before distribution, including the pinned TrustDeck client and all transitives. Disposable Kafka/Testcontainers integration tests must use unique resources and never existing infrastructure or the production backlog. A live backend test is optional and requires separately confirmed synthetic development resources.

REST audit attribution is the connector service account, not mapped `t_user`. The backend's timezone-free `LocalDateTime` and mixed local/UTC comparisons cannot be fully repaired here. Identifier-only reads are ambiguous in multiple-pseudonym domains. Batch 206 responses can lack safe correlation for generated, repeated, or reduced results; the connector must fall back to singles or report unknown outcomes. These limitations are intentional and described in `PLAN.md`.

Dependency/license sources: Spring Boot and Spring Kafka are Apache-2.0 (`https://spring.io/projects/spring-boot`, `https://spring.io/projects/spring-kafka`); Jackson is Apache-2.0 (`https://github.com/FasterXML/jackson`); Lombok is MIT (`https://github.com/projectlombok/lombok`); JUnit is EPL-2.0 (`https://github.com/junit-team/junit5`); Testcontainers is MIT (`https://github.com/testcontainers/testcontainers`). The TrustDeck client and this application are Apache-2.0. These are summaries of direct dependencies, not a complete notice. The generated report is the distribution review source for resolved transitives; do not describe EPL components as Apache relicensed.
