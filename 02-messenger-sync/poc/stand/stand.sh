#!/bin/sh
# shellcheck disable=SC2086,SC2016,SC2046  # $C is intentionally word-split; quoted scripts run in the container
# Isolated container stand for D1–D4: synthetic tokens, mock Telegram/MAX API on an internal
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
    all=$(docker $kind --filter "label=com.docker.compose.project=$PROJECT") \
        || refuse "cannot list docker resources of '$PROJECT'"
    own=$(docker $kind --filter "label=com.docker.compose.project=$PROJECT" --filter label=telegram-max-bridge.stand=1) \
        || refuse "cannot list docker resources of '$PROJECT'"
    [ "$all" = "$own" ] || refuse "project '$PROJECT' has resources not created by the stand"
done
export BRIDGE_IMAGE=${STAND_IMAGE:-telegram-max-bridge:stand}
export BRIDGE_TMPFS_SIZE=64m
export HEALTH_STALE_AFTER=6 HEALTH_QUEUE_AGE=8 HEALTH_INTERVAL=1s HEALTH_START_PERIOD=5s
C="docker compose -p $PROJECT -f compose.yaml -f 02-messenger-sync/poc/stand/compose.stand.yaml"
P="$C -f 02-messenger-sync/poc/stand/compose.storage.yaml"
M="make --no-print-directory PROJECT=$PROJECT"
BROKEN=$(mktemp)
LOGS=$(mktemp)

step() { echo; echo "== $*"; }
# Logs of a container are lost when it is recreated: keep them for the final contract check.
keep_logs() { $C logs --no-log-prefix bridge monitor >>"$LOGS" 2>&1 || true; }
ctl() { $C run --rm --no-deps -T ctl "$@"; }
cid() { $C ps -aq bridge; }
cleanup() {
    rm -f "$BROKEN" "$LOGS"
    if [ "${KEEP_STAND:-0}" != 1 ]; then
        $P --profile stand down -v --remove-orphans >/dev/null 2>&1 || true
        $C --profile stand down -v --remove-orphans >/dev/null 2>&1 || true
    fi
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

$P --profile stand down -v --remove-orphans >/dev/null 2>&1 || true
$C --profile stand down -v --remove-orphans >/dev/null 2>&1 || true
step 'build stand image and start bridge, monitor, mock API'
$C build bridge
$C up -d --wait --wait-timeout 120 mockapi bridge monitor

step 'isolation and hardening'
for service in bridge monitor; do
    id=$($C ps -q $service)
    # No `tee /dev/stderr`: reopening it truncates a stand log redirected to a file.
    info=$(docker inspect -f '{{.Config.User}} privileged={{.HostConfig.Privileged}} ports={{json .HostConfig.PortBindings}} mounts={{range .Mounts}}{{.Destination}} {{end}}' "$id")
    echo "$info" >&2
    [ "$info" = '10001:10001 privileged=false ports={} mounts=/state ' ]
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
keep_logs; $C up -d --force-recreate --no-deps bridge
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
keep_logs
$M COMPOSE="$C" down
docker volume inspect "${PROJECT}_state" >/dev/null
$M COMPOSE="$C" up
after=$(ctl snapshot)
[ "$before" = "$after" ] || { echo "state changed: $before -> $after" >&2; exit 1; }
echo "volume OK: $after"
ctl reply-after-restart

step 'make update keeps state; failed build does not stop the working version'
before=$(ctl snapshot)
keep_logs
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

step 'D2: Telegram poll outage (3x 503) with backoff; MAX and heartbeat continue'
ctl d2-tg-poll-outage

step 'D2: MAX connections refused (requests provably not sent); jobs stay pending'
ctl d2-max-connect-outage

step 'D2: 429 with Retry-After seconds / HTTP-date is honoured on both platforms'
ctl d2-rate-limit

step 'D2: photo download failures both ways; MAX attachment.not.ready reuses the upload'
ctl d2-photo-retry

for scenario in 'error_after d2-5xx 641' 'drop_after d2-drop 642'; do
    set -- $scenario
    step "D2: $1 after a possible publication stays uncertain across recreation"
    set -- $(ctl d2-uncertain "$@") "$2" "$3"
    keep_logs; $C up -d --force-recreate --no-deps bridge
    ctl d2-uncertain-stays "$3" "$4"
    ctl health delivery_blocked
    $M COMPOSE="$C" stop-bridge
    $M COMPOSE="$C" cli ARGS="queue confirm $1 --remote-id $2 --reason 'stand: copy found in target group'"
    $C start bridge
done
health_is healthy

step 'D2: recreation while a retry is scheduled keeps the job, reply link and retry time'
ready=$(ctl d2-wait-start)
keep_logs; $C up -d --force-recreate --no-deps bridge
ctl d2-wait-check "$ready"
health_is healthy

# D3 crash points: stand_bridge.py stops itself (SIGSTOP) at the armed point, the script checks
# SQLite and the mock, then SIGKILLs the container and starts a new, unarmed one.
arm() { keep_logs; STAND_FREEZE=$1 $C up -d --force-recreate --no-deps bridge; }
fresh() { keep_logs; $C up -d --force-recreate --no-deps bridge; }
# The restart policy is switched off first: an automatic restart must not race with the checks.
crash() {
    docker update --restart=no "$(cid)" >/dev/null
    docker kill -s KILL "$(cid)" >/dev/null
    echo 'SIGKILL sent to the bridge container'
}
frozen() {
    i=0
    while [ $i -lt 120 ]; do
        pid=$($C exec -T bridge python -c "$pid_cmd" 2>/dev/null) || pid=
        if [ -n "$pid" ] && [ "$($C exec -T bridge cut -d' ' -f3 "/proc/$pid/stat" 2>/dev/null)" = T ]; then
            echo "bridge stopped itself at the crash point (pid $pid)"; return 0
        fi
        i=$((i + 1)); sleep 0.5
    done
    echo 'bridge did not reach the crash point' >&2; exit 1
}

step 'D3: SIGKILL inside the accept transaction; the redelivered batch is accepted once'
arm accept:d3-freeze-accept
set -- $(ctl d3-accept-start)
frozen
ctl d3-accept-frozen "$@"
crash
fresh
ctl d3-accept-check "$@"

step 'D3: own-bot echo, other bots and identical texts'
ctl d3-echo

step 'D3: SIGKILL during photo preparation in both directions'
ctl d3-photo-start
crash
fresh
ctl d3-photo-release
ctl d3-photo-check

step 'D3: SIGKILL right after the sent COMMIT; redelivery does not publish again'
arm sent:tg741
ctl d3-inject tg tg741 'tg741 d3-c1'
frozen
ctl d3-frozen tg 741 sent tg741 1
crash
fresh
ctl d3-kept tg 741 sent tg741 1 tg >/dev/null
ctl d3-sent-reply

step 'D3: long message, SIGKILL after part 0 sent; part 1 continues, part 0 not repeated'
arm sent:tg751
ctl d3-inject tg tg751 long
frozen
ctl d3-frozen tg 751 sent,pending tg751 1
crash
fresh
ctl d3-kept tg 751 sent,sent tg751 1 >/dev/null

step 'D3: SIGKILL after part 1 claimed (sending) before its POST'
arm sending:tg761-tail
ctl d3-inject tg tg761 long tg761-tail
frozen
ctl d3-frozen tg 761 sent,sending tg761 1
crash
fresh
ctl d3-kept tg 761 sent,uncertain tg761 1 tg >/dev/null
ctl d3-other-direction
fresh
set -- $(ctl d3-kept tg 761 sent,uncertain tg761 1)
skip_id=$1

step 'D3: SIGKILL after the API accepted a publication, before the result COMMIT'
arm publish:maxd3u1
ctl d3-inject max d3u1 'maxd3u1 d3-u1'
frozen
ctl d3-frozen max d3u1 sending maxd3u1 1
crash
fresh
ctl d3-kept max d3u1 uncertain maxd3u1 1 max >/dev/null
fresh
set -- $(ctl d3-kept max d3u1 uncertain maxd3u1 1)
confirm_id=$1 confirm_remote=$2

step 'D3: read-only diagnostics do not change the stopped database; operator resolves uncertain'
$M COMPOSE="$C" stop-bridge
# Logical snapshot (user_version, schema, all rows); WAL/SHM files may legitimately appear on read.
before=$(ctl d3-snapshot)
fail() { echo "read-only check: $*" >&2; exit 1; }
$M COMPOSE="$C" status >/dev/null || fail 'make status failed'
if $M COMPOSE="$C" health >/dev/null; then fail 'make health reported ok for a stopped bridge'; fi
out=$($M -s COMPOSE="$C" cli ARGS='status --json') || fail 'status --json failed'
case "$out" in *'"schema_version": 3'*'process_stopped'*) ;; *) fail "unexpected status: $out" ;; esac
if out=$($M -s COMPOSE="$C" cli ARGS='health --json'); then fail 'health --json reported ok'; fi
case "$out" in *'"ok": false'*'process_stopped'*) ;; *) fail "unexpected health: $out" ;; esac
for what in queue quarantine; do
    out=$($M -s COMPOSE="$C" cli ARGS="$what list") || fail "$what list failed"
    case "$out" in '['*']') ;; *) fail "unexpected $what list: $out" ;; esac
done
[ "$before" = "$(ctl d3-snapshot)" ] || fail 'diagnostics changed data, schema or delivery states'
echo "read-only OK: status/health (exit 0/1, JSON) and queue/quarantine list kept logical snapshot $before"
$M COMPOSE="$C" cli ARGS="queue skip $skip_id --reason 'stand: uncertain part never published'"
$M COMPOSE="$C" cli ARGS="queue confirm $confirm_id --remote-id $confirm_remote --reason 'stand: copy found in target group'"
$C start bridge
ctl d3-health-only input_quarantine
ctl d3-confirm-reply

step 'D3: a second bridge on the same volume is refused before polling or publishing'
hits=$(ctl d3-hits tg:getMe)
if out=$($C run --rm --no-deps -T bridge 2>&1); then echo 'second bridge started' >&2; exit 1; fi
echo "$out" | grep -q 'Другой процесс использует прототип' || { echo "$out" >&2; exit 1; }
[ "$hits" = "$(ctl d3-hits tg:getMe)" ] || { echo 'second bridge contacted the API' >&2; exit 1; }
echo 'owner lock OK: second instance refused before any API call; lock file was never removed'
ctl d3-health-only input_quarantine

seq=0
for worker in poll:tg send:max poll:max send:tg; do
    step "D4: stalled $worker is detected while the heartbeat and the other workers run"
    seq=$((seq + 1))
    arm "worker:$worker"
    ctl d4-stalled "$worker" "$seq"
    fresh
    ctl d4-recovered "$worker"
done

step 'D5.2: SIGKILL inside content cleanup; replay and old replies after restart'
$M COMPOSE="$C" stop-bridge
ctl d52-seed
before=$(ctl d3-snapshot)
$M COMPOSE="$C" maintenance
[ "$before" = "$(ctl d3-snapshot)" ] || fail 'maintenance dry-run changed the bridge database'
if $M COMPOSE="$C" maintenance APPLY=1; then fail 'maintenance apply did not respect the running monitor lock'; fi
[ "$before" = "$(ctl d3-snapshot)" ] || fail 'refused maintenance changed the bridge database'
arm maintenance:kill
frozen
ctl d52-unchanged
crash
ctl d52-unchanged
fresh
ctl d52-cleaned
ctl d52-replies

step 'D5.2: SIGTERM during cleanup respects the transaction and shutdown budget'
$M COMPOSE="$C" stop-bridge
ctl d52-seed
arm maintenance:term
frozen
started=$(date +%s)
docker update --restart=no "$(cid)" >/dev/null
docker kill -s TERM "$(cid)" >/dev/null
docker kill -s CONT "$(cid)" >/dev/null
i=0
while [ $i -lt 35 ]; do
    [ "$(docker inspect -f '{{.State.Running}}' "$(cid)")" = false ] && break
    i=$((i + 1)); sleep 1
done
[ "$(docker inspect -f '{{.State.Running}}' "$(cid)")" = false ] || fail 'cleanup SIGTERM exceeded shutdown budget'
[ "$(docker inspect -f '{{.State.ExitCode}}' "$(cid)")" = 0 ] || fail 'cleanup SIGTERM did not exit cleanly'
[ $(($(date +%s) - started)) -le 30 ] || fail 'cleanup SIGTERM exceeded 30 seconds'
ctl d52-atomic
fresh
ctl d52-cleaned

step 'D5.2: repeated intake/delivery/aging/cleanup cycles on real SQLite files'
ctl d52-cycles

step 'D5.1: bounded alert JSON files and outbox recovery after a real SIGKILL'
ctl d5-rotation

step 'D5.1: /tmp is a real 64 MiB tmpfs in bridge and monitor'
for service in bridge monitor; do
    $C exec -T "$service" python - <<'PY'
import errno, os
from pathlib import Path
p = Path('/tmp/d5-owned-fill')
assert not p.exists()
fs = os.statvfs('/tmp')
assert fs.f_blocks * fs.f_frsize == 64 * 1024**2
try:
    with p.open('xb') as stream:
        while True:
            stream.write(b'x' * 1024**2)
except OSError as exc:
    assert exc.errno == errno.ENOSPC, exc.errno
else:
    raise AssertionError('tmpfs did not enforce its limit')
finally:
    p.unlink()
print('tmpfs OK: 64 MiB, enforced ENOSPC, fixture removed')
PY
done

step 'D5.1: real bounded state-filesystem pressure pauses intake and automatically recovers'
# D1-D4 used normal persistent disk volumes. This final scenario gets a separate bounded
# filesystem in the same project; mockapi holds it mounted across bridge/monitor recreation.
keep_logs
$M COMPOSE="$C" down
export STAND_STORAGE_FREE_WARN_BYTES=2097152 STAND_STORAGE_FREE_CRITICAL_BYTES=1048576
$P up -d --wait --wait-timeout 120 mockapi bridge monitor
$P run --rm --no-deps -T ctl flows
set -- $($P run --rm --no-deps -T ctl d5-thresholds)
export STAND_STORAGE_FREE_WARN_BYTES=$1 STAND_STORAGE_FREE_CRITICAL_BYTES=$2
keep_logs; $P up -d --force-recreate --no-deps bridge monitor
$P run --rm --no-deps -T ctl d5-pressure
keep_logs
$M COMPOSE="$P" down
unset STAND_STORAGE_FREE_WARN_BYTES STAND_STORAGE_FREE_CRITICAL_BYTES
$C up -d mockapi bridge monitor
ctl d3-health-only input_quarantine

step 'D4: bridge and monitor logs are JSON Lines without tokens, texts or author names'
keep_logs
ctl d4-logs <"$LOGS"

echo
echo 'Container stand passed: synthetic data only, no real API calls'
