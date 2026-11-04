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
Usage: $0 <standalone|trustdeck> <start|stop|restart|status|logs|build>

Setups:
  standalone  - Connector only; TrustDeck and Keycloak are reached through the URLs in .env
  trustdeck   - Connector joins the external Docker network 'trustdeck', so .env can use
                container names such as http://trustdeck-backend:8080 (TrustDeck on the same host)

Actions:
  start    - Build the image, (re)create the container, and wait until it is ready
  stop     - Stop and remove the container (committed Kafka offsets are kept)
  restart  - stop, then start
  status   - Show container state and readiness
  logs     - Follow the container log
  build    - Build the image only

Environment:
  CONNECTOR_PROJECT  Compose project name (default: trustdeck-kafka-connector)
  HTTP(S)_PROXY, NO_PROXY  Passed to the image build when set

Examples:
  $0 trustdeck start
  $0 trustdeck logs
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
  curl -fsS "http://127.0.0.1:$port/actuator/health/readiness" 2>/dev/null || echo "not ready or not reachable"
  echo
}

logs() {
  compose logs -f --since 10m "$SERVICE"
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
  *) usage ;;
esac
