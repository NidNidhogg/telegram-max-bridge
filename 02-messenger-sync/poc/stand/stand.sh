#!/bin/sh
# shellcheck disable=SC2086,SC2016,SC2046  # $C is intentionally word-split; quoted scripts run in the container
# Isolated container stand for D1: synthetic tokens, mock Telegram/MAX API on an internal
# network, its own Compose project, image tag and volumes. Never touches the working project.
set -eu
cd "$(dirname "$0")/../../.."
WORK_PROJECT=${WORK_PROJECT:-telegram-max-bridge}
PROJECT=${STAND_PROJECT:-$WORK_PROJECT-stand}
refuse() { echo "stand refused: $*; nothing was stopped or removed" >&2; exit 2; }
case "$PROJECT" in
    "$WORK_PROJECT"|telegram-max-bridge) refuse "stand project '$PROJECT' is a working project name" ;;
esac
# The stand removes its project with down -v, so it must only ever see resources it created itself.
for kind in 'ps -aq' 'volume ls -q'; do
    all=$(docker $kind --filter "label=com.docker.compose.project=$PROJECT" | wc -l)
    own=$(docker $kind --filter "label=com.docker.compose.project=$PROJECT" --filter label=telegram-max-bridge.stand=1 | wc -l)
    [ "$all" = "$own" ] || refuse "project '$PROJECT' has resources not created by the stand"
done
export BRIDGE_IMAGE=${STAND_IMAGE:-telegram-max-bridge:stand}
export HEALTH_STALE_AFTER=6 HEALTH_QUEUE_AGE=8 HEALTH_INTERVAL=1s HEALTH_START_PERIOD=5s
C="docker compose -p $PROJECT -f compose.yaml -f 02-messenger-sync/poc/stand/compose.stand.yaml"
M="make --no-print-directory PROJECT=$PROJECT"
BROKEN=$(mktemp)

step() { echo; echo "== $*"; }
ctl() { $C run --rm --no-deps -T ctl "$@"; }
cid() { $C ps -aq bridge; }
cleanup() {
    rm -f "$BROKEN"
    [ "${KEEP_STAND:-0}" = 1 ] || $C --profile stand down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
health_is() {
    i=0
    while [ $i -lt 90 ]; do
        s=$(docker inspect -f '{{.State.Health.Status}}' "$(cid)")
        if [ "$s" = "$1" ]; then echo "docker health: $s"; return 0; fi
        i=$((i + 1)); sleep 1
    done
    echo "expected docker health $1, got $s" >&2; exit 1
}
stop_bridge() {
    started=$(date +%s)
    $C stop bridge
    took=$(( $(date +%s) - started ))
    code=$(docker inspect -f '{{.State.ExitCode}}' "$(cid)")
    $C logs --no-log-prefix bridge | tail -n 3
    [ "$code" = 0 ] || { echo "bridge exit code $code (SIGKILL?)" >&2; exit 1; }
    [ "$took" -ge 10 ] || { echo "stop took ${took}s; expected an in-flight operation to be awaited" >&2; exit 1; }
    $C logs --no-log-prefix bridge | tail -n 1 | grep -q 'Остановлено. Очередь сохранена.'
    echo "stop OK: SIGTERM handled, waited ${took}s for the in-flight request, exit code 0"
}

$C --profile stand down -v --remove-orphans >/dev/null 2>&1 || true
step 'build stand image and start bridge, monitor, mock API'
$C build bridge
$C up -d --wait --wait-timeout 120 mockapi bridge monitor

step 'isolation and hardening'
for service in bridge monitor; do
    id=$($C ps -q $service)
    docker inspect -f '{{.Config.User}} privileged={{.HostConfig.Privileged}} ports={{json .HostConfig.PortBindings}} mounts={{range .Mounts}}{{.Destination}} {{end}}' "$id" | tee /dev/stderr \
        | grep -q '^10001:10001 privileged=false ports={} mounts=/state $'
done
$C exec -T bridge sh -ec 'test "$(id -u)" = 10001; tr "\0" " " </proc/1/cmdline | grep -q docker-init
    ! find / -xdev \( -path /proc -o -path /app/.venv -o -path /state \) -prune -o \( -name ".env" -o -name ".env.*" -o -name ".state" -o -name "*.sqlite3*" -o -name "*.jsonl" \) -print 2>/dev/null | grep .'
echo 'hardening OK: uid 10001, docker-init as PID 1, no ports, not privileged, no docker.sock, no secrets/state in image'

step 'text, photo, reply in both directions; redelivery deduplication'
ctl flows
health_is healthy
ctl health ok

step 'blocked delivery is detected while the process is alive; pending job survives recreation'
ctl pending-start
health_is unhealthy
ctl health queue_too_old
old=$(cid)
$C up -d --force-recreate --no-deps bridge
[ "$old" != "$(cid)" ] || { echo 'bridge container was not recreated' >&2; exit 1; }
ctl pending-check
ctl pending-release
health_is healthy

step 'SIGTERM during photo preparation'
ctl photo-hold
stop_bridge
ctl photo-check
$C start bridge
ctl photo-done

step 'SIGTERM during publication; uncertain is not retried'
ctl publish-hold
stop_bridge
ctl publish-check
$C start bridge
set -- $(ctl uncertain-stays)
health_is unhealthy
ctl health delivery_blocked
$M COMPOSE="$C" stop-bridge
$M COMPOSE="$C" doctor >/dev/null
echo 'make stop-bridge + make doctor OK against the mock API'
$M COMPOSE="$C" cli ARGS="queue confirm $1 --remote-id $2 --reason 'stand: copy found in target group'"
$C start bridge
health_is healthy
ctl confirmed

step 'stalled process (SIGSTOP) is detected while the container is alive'
pid_cmd='import os,sys
for p in filter(str.isdigit, os.listdir("/proc")):
    a=open(f"/proc/{p}/cmdline","rb").read().split(b"\0")
    if len(a)>2 and a[1].endswith(b"bridge.py") and a[2]==b"run": print(p)'
pid=$($C exec -T bridge python -c "$pid_cmd")
$C exec -T bridge sh -c 'kill -STOP "$1"' _ "$pid"
health_is unhealthy
ctl health heartbeat_stale
[ "$(docker inspect -f '{{.State.Running}}' "$(cid)")" = true ]
if $M COMPOSE="$C" health; then echo 'make health reported ok for a stalled bridge' >&2; exit 1; fi
$M COMPOSE="$C" status | grep -q heartbeat_stale
echo 'make health/status OK: same thresholds as the Docker healthcheck'
$C exec -T bridge sh -c 'kill -CONT "$1"' _ "$pid"
health_is healthy

step 'state volume survives make down / make up'
before=$(ctl snapshot)
$M COMPOSE="$C" down
docker volume inspect "${PROJECT}_state" >/dev/null
$M COMPOSE="$C" up
after=$(ctl snapshot)
[ "$before" = "$after" ] || { echo "state changed: $before -> $after" >&2; exit 1; }
echo "volume OK: $after"
ctl reply-after-restart

step 'make update keeps state; failed build does not stop the working version'
before=$(ctl snapshot)
$M COMPOSE="$C" update
[ "$before" = "$(ctl snapshot)" ]
old=$(cid)
printf 'services:\n  bridge:\n    build:\n      dockerfile_inline: |\n        FROM scratch\n        RUN false\n' >"$BROKEN"
if $M COMPOSE="$C -f $BROKEN" update; then
    echo 'broken update unexpectedly succeeded' >&2; exit 1
fi
[ "$old" = "$(cid)" ] && [ "$(docker inspect -f '{{.State.Running}}' "$old")" = true ]
health_is healthy
[ "$before" = "$(ctl snapshot)" ]
echo 'update OK: state kept; failed build left the running container untouched'

step 'monitor service wrote problem and recovery alerts to the shared volume'
ctl alerts

echo
echo 'Container stand passed: synthetic data only, no real API calls'
