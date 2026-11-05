#!/usr/bin/env bash
# TrustDeck Kafka Connector
# Copyright 2026 Armin Müller
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

# Absolute path of the directory this script is in
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$ROOT_DIR/.env"
BASE_COMPOSE="$ROOT_DIR/docker/compose.yaml"
NETWORK_COMPOSE="$ROOT_DIR/docker/compose.trustdeck-network.yaml"

# Compose project name; set CONNECTOR_PROJECT to run several instances side by side
PROJECT="${CONNECTOR_PROJECT:-trustdeck-kafka-connector}"
SERVICE="trustdeck-kafka-connector"

# Earlier deployments ran "docker compose" from docker/ without a project name, so Compose
# named the project "docker". That name collides with other stacks started from a docker/ directory.
LEGACY_PROJECT="docker"

# UID of the "connector" user inside the image (see docker/Dockerfile)
CONTAINER_UID=10001

# Helper for printing how to use
usage() {
  cat <<EOF
Usage: $0 <standalone|trustdeck> <start|stop|restart|status|logs|build|progress> [options]

Setups:
  standalone  - Connector only; TrustDeck and Keycloak are reached through the URLs in .env
  trustdeck   - Connector joins the external Docker network 'trustdeck', so .env can use
                container names such as http://trustdeck-backend:8080 (TrustDeck on the same host)

Actions:
  start    - Build the image, (re)create the container, and wait until it is ready
  stop     - Stop and remove the container (committed Kafka offsets are kept)
  restart  - stop, then start
  status   - Show container state, readiness, and the reason of the last pause
  logs     - Follow the container log
  build    - Build the image only
  progress [since-ms] [seconds]
           - Measure throughput over [seconds] (default 60), estimate the remaining time, and
             count DLT/review entries since [since-ms] (default: container start).
             Needs kcat, jq, and keytool on the host for the recovery-topic counts.

Environment:
  CONNECTOR_PROJECT  Compose project name (default: trustdeck-kafka-connector)
  HTTP(S)_PROXY, NO_PROXY  Passed to the image build when set

Examples:
  $0 trustdeck start
  $0 trustdeck logs
  $0 trustdeck progress "" 600
  CONNECTOR_PROJECT=connector-b $0 standalone start
EOF
  exit 1
}

# Decide whether Docker needs sudo. "sudo -E" keeps proxy variables for the image build.
# If your user is not in the docker group, run "sudo -v" before this script so sudo can prompt once.
if docker info >/dev/null 2>&1; then
  SUDO_DOCKER=()
elif command -v sudo >/dev/null 2>&1; then
  SUDO_DOCKER=(sudo -E)
else
  echo "Cannot talk to Docker and sudo is not available."
  echo "   - Is the Docker daemon running?"
  echo "   - Do you need to be in the 'docker' group or use sudo?"
  exit 1
fi

docker_cmd() {
  "${SUDO_DOCKER[@]}" docker "$@"
}

# Runs docker compose with the project name, .env, and the compose files of the selected setup
compose() {
  docker_cmd compose --project-name "$PROJECT" --env-file "$ENV_FILE" "${COMPOSE_FILES[@]}" "$@"
}

# Reads one variable from .env without exporting the whole file
env_value() {
  local name="$1"
  local default="${2:-}"
  local value
  value="$(sed -n "s/^${name}=//p" "$ENV_FILE" | tail -n 1 | tr -d '\r' | sed -e 's/^"\(.*\)"$/\1/' -e "s/^'\(.*\)'$/\1/")"
  echo "${value:-$default}"
}

# Fails early on missing files that Compose would otherwise replace with empty directories
check_files() {
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "Environment file not found: $ENV_FILE"
    echo "Copy .env.example to .env and fill it in."
    exit 1
  fi

  if [[ ! -f "$ROOT_DIR/config/instance/application.yaml" ]]; then
    echo "Instance configuration not found: $ROOT_DIR/config/instance/application.yaml"
    exit 1
  fi

  if [[ ! -d "$ROOT_DIR/config/instance/truststores" ]]; then
    echo "Truststore directory not found: $ROOT_DIR/config/instance/truststores"
    exit 1
  fi
}

# The trustdeck network is created by the TrustDeck stack; without it TrustDeck is not running here
check_trustdeck_network() {
  if ! docker_cmd network inspect trustdeck >/dev/null 2>&1; then
    echo "Docker network 'trustdeck' does not exist. Start the TrustDeck stack first,"
    echo "or use the 'standalone' setup if TrustDeck runs elsewhere."
    exit 1
  fi
}

# Creates the statistics directory and makes it writable for the container user.
# Without this, Docker would create it owned by root and the connector could not write to it.
ensure_statistics_dir() {
  local configured
  configured="$(env_value STATISTICS_HOST_DIR ../statistics)"

  # Compose resolves relative paths against the directory of the first compose file (docker/)
  local dir
  if [[ "$configured" == /* ]]; then
    dir="$configured"
  else
    dir="$ROOT_DIR/docker/$configured"
  fi

  if [[ ! -d "$dir" ]]; then
    echo "Creating statistics directory: $dir"
    mkdir -p "$dir" 2>/dev/null || sudo mkdir -p "$dir"
  fi

  if [[ "$(stat -c %u "$dir")" != "$CONTAINER_UID" ]]; then
    echo "Handing the statistics directory to the container user (UID $CONTAINER_UID): $dir"
    if ! { chown "$CONTAINER_UID:$CONTAINER_UID" "$dir" 2>/dev/null || sudo chown "$CONTAINER_UID:$CONTAINER_UID" "$dir"; }; then
      echo "Could not change the owner; making the directory writable for everyone instead."
      chmod 0777 "$dir" 2>/dev/null || sudo chmod 0777 "$dir"
    fi
  fi
}

# Stops a container from the old "docker" project so it does not consume in parallel with the new one
stop_legacy_container() {
  [[ "$PROJECT" == "$LEGACY_PROJECT" ]] && return 0

  local legacy=()
  mapfile -t legacy < <(docker_cmd ps -aq \
    --filter "label=com.docker.compose.project=$LEGACY_PROJECT" \
    --filter "label=com.docker.compose.service=$SERVICE")

  if [[ ${#legacy[@]} -gt 0 ]]; then
    echo "Removing connector container from the previous project name '$LEGACY_PROJECT' ..."
    docker_cmd stop -t 45 "${legacy[@]}" >/dev/null
    docker_cmd rm "${legacy[@]}" >/dev/null
  fi
}

# Waits for the Docker health check, which calls the readiness endpoint
wait_for_ready() {
  local retries="${1:-36}"
  local delay="${2:-5}"
  local container
  container="$(compose ps -q "$SERVICE")"

  if [[ -z "$container" ]]; then
    echo "The connector container is not running."
    return 1
  fi

  echo "Waiting for the connector to become ready (the first health check runs after about 30s) ..."

  for i in $(seq 1 "$retries"); do
    local status
    status="$(docker_cmd inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container" 2>/dev/null || echo "unknown")"

    case "$status" in
      healthy)
        echo "Connector is ready."
        return 0
        ;;
      unhealthy|exited|dead)
        echo "Connector is $status: either it could not start (configuration), or it paused consumption"
        echo "because TrustDeck, Keycloak, or a recovery topic is unavailable. Last log lines:"
        compose logs --tail=25 --no-log-prefix "$SERVICE" || true
        return 1
        ;;
    esac

    echo "  [$i/$retries] status: $status (retrying in ${delay}s ...)"
    sleep "$delay"
  done

  echo "Timed out waiting for the connector. Check: $0 $MODE logs"
  return 1
}

build() {
  echo "Building the connector image ..."
  compose build
}

start() {
  check_files
  [[ "$MODE" == "trustdeck" ]] && check_trustdeck_network
  ensure_statistics_dir
  build
  stop_legacy_container

  echo "Starting the connector (project '$PROJECT', setup '$MODE') ..."
  compose up -d

  wait_for_ready
  echo "Follow the log with: $0 $MODE logs"
}

stop() {
  echo "Stopping the connector (project '$PROJECT') ..."
  compose down
  echo "Connector stopped. Committed Kafka offsets are kept; the next start continues from there."
}

status() {
  compose ps

  local port
  port="$(env_value HEALTH_HOST_PORT 18080)"
  echo
  echo -n "Readiness (http://127.0.0.1:$port/actuator/health/readiness): "
  if curl -fsS "http://127.0.0.1:$port/actuator/health/readiness" 2>/dev/null; then
    echo
    return 0
  fi
  echo "not ready or not reachable"

  # A paused connector names the cause, and for TrustDeck failures the domain, when it pauses
  echo
  echo "Last pause and recovery events:"
  compose logs --no-log-prefix "$SERVICE" 2>/dev/null \
    | grep -E 'pausing consumption|Recovery probe failed|consumption resumed|Consumption paused' | tail -n 5 \
    || echo "  none found in the current container log"
}

logs() {
  compose logs -f --no-log-prefix --since 10m "$SERVICE"
}

# Reads all variables from .env into the environment (progress needs the Kafka credentials)
load_env() {
  set -a
  # shellcheck disable=SC1090
  source <(sed 's/\r$//' "$ENV_FILE")
  set +a
}

# Reads a plain value such as "dlt-topic: name" from config/instance/application.yaml; placeholders are ignored
instance_value() {
  sed -n "s/^[[:space:]]*$1:[[:space:]]*//p" "$ROOT_DIR/config/instance/application.yaml" \
    | tr -d "\"'\r" | grep -v '^\$' | head -n 1 || true
}

# Sums committed offset, log end, and lag over all partitions of the source topic
group_offsets() {
  local props="$1"
  local topic="$2"
  docker_cmd run --rm --user "$(id -u):$(id -g)" -v "$props:/tmp/admin.properties:ro" \
    -v "$ROOT_DIR/config/instance/truststores:/truststores:ro" apache/kafka:3.9.1 \
    /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --command-config /tmp/admin.properties --describe --group "$KAFKA_GROUP_ID" 2>/dev/null \
  | awk -v t="$topic" '$2 == t {c += $4; e += $5; l += $6} END {printf "%d %d %d\n", c, e, l}'
}

progress() {
  local since_ms="${1:-}"
  local interval="${2:-60}"
  load_env

  local topic="${KAFKA_TOPIC:-}"
  if [[ -z "$topic" ]]; then
    echo "KAFKA_TOPIC is not set in .env."
    exit 1
  fi

  # Default window: since the current container started
  if [[ -z "$since_ms" ]]; then
    local container started
    container="$(compose ps -q "$SERVICE")"
    started="$(docker_cmd inspect -f '{{.State.StartedAt}}' "$container" 2>/dev/null || true)"
    since_ms="$(date -u -d "${started:-now}" +%s%3N)"
  fi

  # Temporary Kafka client configuration; contains the password and is removed on exit
  local props
  props="$(umask 077; mktemp /tmp/kafka-admin.XXXXXX.properties)"
  # shellcheck disable=SC2064
  trap "shred -u '$props' 2>/dev/null || rm -f '$props'" EXIT
  cat > "$props" <<EOF
security.protocol=${KAFKA_SECURITY_PROTOCOL:-SASL_SSL}
sasl.mechanism=${KAFKA_SASL_MECHANISM:-PLAIN}
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${KAFKA_USERNAME}" password="${KAFKA_PASSWORD}";
ssl.truststore.location=/truststores/$(basename "$KAFKA_TRUSTSTORE_LOCATION")
ssl.truststore.password=${KAFKA_TRUSTSTORE_PASSWORD}
ssl.truststore.type=${KAFKA_TRUSTSTORE_TYPE:-JKS}
EOF

  echo "=== Progress of group '$KAFKA_GROUP_ID' on '$topic' (measuring for ${interval}s) ..."
  local c1 c2 e2 l2 t1 t2
  read -r c1 _ _ < <(group_offsets "$props" "$topic"); t1=$(date +%s)
  sleep "$interval"
  read -r c2 e2 l2 < <(group_offsets "$props" "$topic"); t2=$(date +%s)
  awk -v c1="$c1" -v c2="$c2" -v e="$e2" -v l="$l2" -v s=$((t2 - t1)) 'BEGIN {
    if (e == 0) { print "No offsets found; is KAFKA_GROUP_ID correct?"; exit }
    r = (c2 - c1) / s
    printf "Processed: %d of %d (%.2f%%), remaining: %d\n", c2, e, 100 * c2 / e, l
    printf "Rate:      %.0f records/s (%.1f million/day)\n", r, r * 86400 / 1e6
    if (r > 0) printf "ETA:       %.1f hours (%.1f days)\n", l / r / 3600, l / r / 86400
    else if (l > 0) print "ETA:       no progress during the measurement (paused? see status)"
    else print "ETA:       done"
  }'

  echo
  echo "=== Recovery topics since $(date -u -d "@$((since_ms / 1000))" '+%F %T') UTC"
  if ! command -v kcat >/dev/null || ! command -v jq >/dev/null; then
    echo "kcat and jq are needed for this part; skipped."
  else
    local ca
    ca="$(umask 077; mktemp /tmp/kafka-ca.XXXXXX.pem)"
    # shellcheck disable=SC2064
    trap "shred -u '$props' 2>/dev/null || rm -f '$props'; rm -f '$ca'" EXIT
    keytool -exportcert -rfc -keystore "$ROOT_DIR/config/instance/truststores/$(basename "$KAFKA_TRUSTSTORE_LOCATION")" \
      -storepass "$KAFKA_TRUSTSTORE_PASSWORD" > "$ca" 2>/dev/null

    local dlt review t
    dlt="${KAFKA_DLT_TOPIC:-$(instance_value dlt-topic)}"
    review="${KAFKA_REVIEW_TOPIC:-$(instance_value review-topic)}"
    for t in "${dlt:-$topic.DLT}" "${review:-$topic.REVIEW}"; do
      echo "--- $t"
      kcat -C -b "$KAFKA_BOOTSTRAP_SERVERS" -X security.protocol="${KAFKA_SECURITY_PROTOCOL:-SASL_SSL}" \
        -X sasl.mechanism="${KAFKA_SASL_MECHANISM:-PLAIN}" -X sasl.username="$KAFKA_USERNAME" \
        -X sasl.password="$KAFKA_PASSWORD" -X ssl.ca.location="$ca" \
        -t "$t" -o "s@$since_ms" -e -q -f '%s\n' 2>/dev/null \
      | jq -r '"\(.status)\t\(.failureCategory)\t\(.httpStatus // "-")"' | sort | uniq -c
    done
  fi

  echo
  echo "=== Connector"
  status | tail -n +2
}

# --- Main script ---
MODE="${1:-}"
ACTION="${2:-}"

if [[ -z "$MODE" || -z "$ACTION" ]]; then
  usage
fi

case "$MODE" in
  standalone) COMPOSE_FILES=(-f "$BASE_COMPOSE") ;;
  trustdeck)  COMPOSE_FILES=(-f "$BASE_COMPOSE" -f "$NETWORK_COMPOSE") ;;
  *) usage ;;
esac

case "$ACTION" in
  start)   start ;;
  stop)    check_files; stop ;;
  restart) check_files; stop; start ;;
  status)  check_files; status ;;
  logs)    check_files; logs ;;
  build)   check_files; build ;;
  progress) check_files; progress "${3:-}" "${4:-}" ;;
  *) usage ;;
esac
