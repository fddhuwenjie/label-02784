#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DOWN_ON_EXIT="false"
CORE_ONLY="false"
NO_BUILD="false"
HEALTH_PORT="9999"
BASE_TASK_ID="$(date +%s)"

usage() {
  cat <<'EOF'
Usage:
  scripts/e2e_full_test.sh [options]

Options:
  --task-id <num>     Base task id (script uses base, base+1, base+2)
  --health-port <n>   Health server port (default: 9999)
  --core-only         Run core flow only (skip reconnect/degradation tests)
  --no-build          Use `docker-compose up -d` (without --build)
  --down              Run `docker-compose down` on script exit
  -h, --help          Show help

Examples:
  scripts/e2e_full_test.sh
  scripts/e2e_full_test.sh --core-only
  scripts/e2e_full_test.sh --task-id 20001 --down
EOF
}

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

warn() {
  printf '[%s] WARN: %s\n' "$(date '+%H:%M:%S')" "$*" >&2
}

fail() {
  printf '[%s] ERROR: %s\n' "$(date '+%H:%M:%S')" "$*" >&2
  exit 1
}

require_cmd() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || fail "Missing command: $cmd"
}

ensure_docker_daemon() {
  if docker info >/dev/null 2>&1; then
    return 0
  fi

  if [[ "$(uname -s)" == "Darwin" ]] && [[ -d "/Applications/Docker.app" ]]; then
    log "Docker daemon not ready, trying to start Docker Desktop..."
    open -a Docker >/dev/null 2>&1 || true

    local i
    for i in {1..90}; do
      if docker info >/dev/null 2>&1; then
        log "Docker daemon is ready"
        return 0
      fi
      sleep 2
    done
  fi

  local current_context context_host docker_host_env
  current_context="$(docker context show 2>/dev/null || echo "unknown")"
  context_host="$(docker context inspect "$current_context" --format '{{(index .Endpoints "docker").Host}}' 2>/dev/null || echo "unknown")"
  docker_host_env="${DOCKER_HOST:-<unset>}"

  fail "Docker daemon is not reachable.
Fix:
  1) Start Docker Desktop and wait until it shows 'Engine running'
  2) Or if you use Colima: colima start
  3) Verify with: docker info
Debug:
  docker context: ${current_context}
  context host: ${context_host}
  DOCKER_HOST env: ${docker_host_env}"
}

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
elif docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
else
  fail "Neither docker-compose nor docker compose is available"
fi

compose() {
  "${COMPOSE_CMD[@]}" "$@"
}

cleanup() {
  if [[ "$DOWN_ON_EXIT" == "true" ]]; then
    log "Stopping services (--down)"
    compose down
  fi
}

trap cleanup EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --task-id)
      shift
      [[ $# -gt 0 ]] || fail "--task-id requires a value"
      BASE_TASK_ID="$1"
      ;;
    --health-port)
      shift
      [[ $# -gt 0 ]] || fail "--health-port requires a value"
      HEALTH_PORT="$1"
      ;;
    --core-only)
      CORE_ONLY="true"
      ;;
    --no-build)
      NO_BUILD="true"
      ;;
    --down)
      DOWN_ON_EXIT="true"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "Unknown argument: $1"
      ;;
  esac
  shift
done

[[ "$BASE_TASK_ID" =~ ^[0-9]+$ ]] || fail "task id must be numeric"
[[ "$HEALTH_PORT" =~ ^[0-9]+$ ]] || fail "health port must be numeric"

TASK_ID_1="$BASE_TASK_ID"
TASK_ID_2="$((BASE_TASK_ID + 1))"
TASK_ID_3="$((BASE_TASK_ID + 2))"

require_cmd docker
require_cmd curl
ensure_docker_daemon

wait_for_service_state() {
  local service="$1"
  local expected="$2"
  local timeout_sec="${3:-180}"
  local interval=2
  local rounds=$((timeout_sec / interval))
  local cid status

  cid="$(compose ps -q "$service" | tr -d '\r\n')"
  [[ -n "$cid" ]] || fail "No container id found for service: $service"

  for ((i=1; i<=rounds; i++)); do
    status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid" 2>/dev/null || true)"
    if [[ "$status" == "$expected" ]]; then
      log "Service $service is $status"
      return 0
    fi
    sleep "$interval"
  done

  fail "Service $service did not reach state '$expected' in ${timeout_sec}s"
}

wait_for_service_healthy() {
  wait_for_service_state "$1" "healthy" "${2:-240}"
}

wait_for_service_running() {
  wait_for_service_state "$1" "running" "${2:-180}"
}

http_request() {
  curl -sS --max-time 5 -w $'\n%{http_code}' "$@"
}

wait_for_health_ok() {
  local timeout_sec="${1:-240}"
  local interval=2
  local rounds=$((timeout_sec / interval))
  local response body code

  for ((i=1; i<=rounds; i++)); do
    response="$(http_request "http://localhost:${HEALTH_PORT}/healthz" || true)"
    body="${response%$'\n'*}"
    code="${response##*$'\n'}"
    if [[ "$code" == "200" && "$body" == *'"status":"ok"'* ]]; then
      log "healthz is OK"
      return 0
    fi
    sleep "$interval"
  done

  fail "healthz did not become OK in ${timeout_sec}s"
}

wait_for_health_degraded() {
  local timeout_sec="${1:-120}"
  local interval=2
  local rounds=$((timeout_sec / interval))
  local response body code

  for ((i=1; i<=rounds; i++)); do
    response="$(http_request "http://localhost:${HEALTH_PORT}/healthz" || true)"
    body="${response%$'\n'*}"
    code="${response##*$'\n'}"
    if [[ "$code" == "503" || "$body" == *'"status":"degraded"'* ]]; then
      log "healthz is degraded as expected"
      return 0
    fi
    sleep "$interval"
  done

  fail "healthz did not become degraded in ${timeout_sec}s"
}

assert_readyz_ok() {
  local response body code
  response="$(http_request "http://localhost:${HEALTH_PORT}/readyz")"
  body="${response%$'\n'*}"
  code="${response##*$'\n'}"
  [[ "$code" == "200" ]] || fail "readyz returned HTTP $code"
  [[ "$body" == *'"status":"ok"'* ]] || fail "readyz body is not ok: $body"
  log "readyz check passed"
}

assert_metrics() {
  local response body code
  response="$(http_request "http://localhost:${HEALTH_PORT}/metrics")"
  body="${response%$'\n'*}"
  code="${response##*$'\n'}"
  [[ "$code" == "200" ]] || fail "metrics returned HTTP $code"
  [[ "$body" == *"media_prompt_http_requests_total"* ]] || fail "metrics missing http counter"
  [[ "$body" == *"media_prompt_health_checks_total"* ]] || fail "metrics missing health counter"
  log "metrics check passed"
}

assert_mock_llm() {
  local response body code
  response="$(http_request -X POST "http://localhost:${HEALTH_PORT}/mock-llm" -H "Content-Type: application/json" --data '{"prompt":"hello"}' || true)"
  body="${response%$'\n'*}"
  code="${response##*$'\n'}"

  if [[ "$code" == "404" ]]; then
    warn "/mock-llm is disabled (likely non-demo config), skipping this assertion"
    return 0
  fi

  [[ "$code" == "200" ]] || fail "mock-llm returned HTTP $code"
  [[ "$body" == *"[demo] prompt generated by local mock LLM"* ]] || fail "mock-llm response mismatch: $body"
  log "mock-llm check passed"
}

declare_queues() {
  docker exec -i media-prompt-rabbitmq rabbitmqadmin -u guest -p guest \
    declare queue name=media_task_in durable=true >/dev/null
  docker exec -i media-prompt-rabbitmq rabbitmqadmin -u guest -p guest \
    declare queue name=media_task_out durable=true >/dev/null
  log "RabbitMQ queues ensured"
}

seed_script_doc() {
  local task_id="$1"
  docker exec -i media-prompt-mongodb mongo script_db >/dev/null <<EOF
db.scripts.deleteMany({ task_id: NumberLong(${task_id}) });
db.scripts.insertOne({
  task_id: NumberLong(${task_id}),
  title: "E2E Task ${task_id}",
  scenes: [
    {
      scene_index: 1,
      description: "Rainy neon street",
      storyboards: [
        { storyboard_index: 1, content: "Wide shot of the main character entering frame" },
        { storyboard_index: 2, content: "Close-up of the character looking up" }
      ]
    }
  ]
});
EOF
  log "Seeded Mongo script for task_id=${task_id}"
}

publish_task() {
  local task_id="$1"
  docker exec -i media-prompt-rabbitmq rabbitmqadmin -u guest -p guest \
    publish routing_key=media_task_in payload="{\"task_id\":${task_id}}" >/dev/null
  log "Published task message task_id=${task_id}"
}

publish_invalid_message() {
  docker exec -i media-prompt-rabbitmq rabbitmqadmin -u guest -p guest \
    publish routing_key=media_task_in payload="not-a-json" >/dev/null
  log "Published invalid message to verify deserialization error handling"
}

wait_mysql_success() {
  local task_id="$1"
  local expected_total="${2:-12}"
  local timeout_sec="${3:-180}"
  local interval=2
  local rounds=$((timeout_sec / interval))
  local result total success failed

  for ((i=1; i<=rounds; i++)); do
    result="$(docker exec -i media-prompt-mysql mysql -N -B -uroot -p123456 -D media_db -e \
      "SELECT COALESCE(COUNT(*),0), COALESCE(SUM(CASE WHEN status=0 THEN 1 ELSE 0 END),0), COALESCE(SUM(CASE WHEN status=1 THEN 1 ELSE 0 END),0) FROM tb_media_prompt WHERE task_id=${task_id};" \
      2>/dev/null || true)"

    if [[ -n "$result" ]]; then
      IFS=$'\t' read -r total success failed <<<"$result"
      total="${total:-0}"
      success="${success:-0}"
      failed="${failed:-0}"

      if (( total >= expected_total )) && (( failed == 0 )); then
        log "MySQL verified for task_id=${task_id} (total=${total}, success=${success}, failed=${failed})"
        return 0
      fi
    fi

    sleep "$interval"
  done

  fail "MySQL verification timed out for task_id=${task_id}"
}

wait_downstream_message() {
  local task_id="$1"
  local timeout_sec="${2:-120}"
  local interval=2
  local rounds=$((timeout_sec / interval))
  local out

  for ((i=1; i<=rounds; i++)); do
    out="$(docker exec -i media-prompt-rabbitmq rabbitmqadmin -u guest -p guest \
      get queue=media_task_out ackmode=ack_requeue_true count=50 2>/dev/null || true)"

    if grep -q "\"task_id\":${task_id}" <<<"$out"; then
      log "Downstream queue contains completion message for task_id=${task_id}"
      return 0
    fi

    sleep "$interval"
  done

  fail "No downstream completion message found for task_id=${task_id}"
}

run_core_flow() {
  local task_id="$1"
  seed_script_doc "$task_id"
  publish_task "$task_id"
  wait_mysql_success "$task_id" 12 240
  wait_downstream_message "$task_id" 120
}

log "Using compose command: ${COMPOSE_CMD[*]}"
if [[ "$NO_BUILD" == "true" ]]; then
  log "Starting services without build"
  compose up -d
else
  log "Starting services with build"
  compose up --build -d
fi

wait_for_service_healthy mysql 300
wait_for_service_healthy rabbitmq 300
wait_for_service_healthy mongodb 300
wait_for_service_running backend 180
wait_for_health_ok 300
assert_readyz_ok
assert_metrics
assert_mock_llm
declare_queues

log "Running core flow test #1"
run_core_flow "$TASK_ID_1"

log "Running invalid-message recovery test"
publish_invalid_message
sleep 2
run_core_flow "$TASK_ID_2"

if [[ "$CORE_ONLY" != "true" ]]; then
  log "Running RabbitMQ reconnect test"
  docker restart media-prompt-rabbitmq >/dev/null
  wait_for_service_healthy rabbitmq 300
  wait_for_health_ok 300
  run_core_flow "$TASK_ID_3"

  log "Running health degradation test (MySQL stop/start)"
  docker stop media-prompt-mysql >/dev/null
  wait_for_health_degraded 180
  docker start media-prompt-mysql >/dev/null
  wait_for_service_healthy mysql 300
  wait_for_health_ok 300
fi

log "Collecting summary"
docker exec -i media-prompt-mysql mysql -N -B -uroot -p123456 -D media_db -e \
  "SELECT task_id, COUNT(*), SUM(status=0), SUM(status=1) FROM tb_media_prompt WHERE task_id IN (${TASK_ID_1}, ${TASK_ID_2}, ${TASK_ID_3}) GROUP BY task_id ORDER BY task_id;" || true

log "All requested tests passed"
if [[ "$DOWN_ON_EXIT" == "true" ]]; then
  log "Services will be stopped now by trap"
else
  log "Services are still running. Use '${COMPOSE_CMD[*]} down' to stop."
fi
