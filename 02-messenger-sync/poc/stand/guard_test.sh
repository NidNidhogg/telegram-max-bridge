#!/bin/sh
# Regression checks with a fake docker that records every call, so nothing real is touched:
# the stand must refuse before any destructive command when its project could be a working
# one or Docker cannot be inspected, and make cli must pass quoted arguments through intact.
set -eu
cd "$(dirname "$0")/../../.."
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
export FAKE_LOG="$tmp/log" FAKE_FOREIGN='' FAKE_LIST_FAIL=''
cat >"$tmp/docker" <<'FAKE'
#!/bin/sh
printf '[%s]' "$@" >>"$FAKE_LOG"; echo >>"$FAKE_LOG"
case "$*" in
    *telegram-max-bridge.stand=1*) exit 0 ;;
    'ps -aq '*|'volume ls -q '*)
        [ -z "$FAKE_LIST_FAIL" ] || exit 1
        [ -z "$FAKE_FOREIGN" ] || echo foreign
        exit 0 ;;
    *' build '*) exit 1 ;;
esac
exit 0
FAKE
chmod +x "$tmp/docker"

fail() { echo "guard FAIL: $*" >&2; cat "$tmp/log" "$tmp/out" >&2; exit 1; }
fake_make() { : >"$tmp/log"; PATH="$tmp:$PATH" make --no-print-directory "$@" >"$tmp/out" 2>&1; }
refused() {
    if fake_make stand "$@"; then fail "make stand $* unexpectedly succeeded"; fi
    ! grep -q '\[down\]' "$tmp/log" || fail "make stand $* ran down"
    grep -q 'stand refused' "$tmp/out" || fail "make stand $* was not refused"
}

refused PROJECT=same STAND_PROJECT=same
refused PROJECT=other STAND_PROJECT=telegram-max-bridge
FAKE_FOREIGN=1; refused PROJECT=work STAND_PROJECT=foreign; FAKE_FOREIGN=''
FAKE_LIST_FAIL=1; refused PROJECT=work STAND_PROJECT=unlistable; FAKE_LIST_FAIL=''

if fake_make stand PROJECT=telegram-max-bridge-stand; then fail 'stand with a failing build succeeded'; fi
grep -q '\[down\]' "$tmp/log" || fail 'expected the stand to clean its own project'
if grep '\[down\]' "$tmp/log" | grep -vq '\[-p\]\[telegram-max-bridge-stand-stand\]'; then
    fail 'down -v outside the derived stand project'
fi
echo 'stand guard OK: colliding, foreign or uninspectable projects are refused before any down -v'

for args in 'queue skip 12 --reason "Отменено администратором"' "queue skip 12 --reason 'Отменено администратором'"; do
    fake_make cli PROJECT=work ARGS="$args" || fail "make cli ARGS=$args"
    grep -q '\[queue\]\[skip\]\[12\]\[--reason\]\[Отменено администратором\]$' "$tmp/log" || fail "make cli ARGS=$args"
done
if fake_make cli PROJECT=work ARGS=; then fail 'make cli without ARGS succeeded'; fi
[ ! -s "$tmp/log" ] || fail 'make cli without ARGS called docker'
echo 'make cli OK: quoted arguments passed intact, empty ARGS refused without docker'

# Compose resolves env_file differently inside YAML anchors and file overrides. Exercise the
# actual parser with synthetic values; config does not start containers or contact the daemon.
if command -v docker >/dev/null 2>&1; then
    mkdir -p "$tmp/02-messenger-sync"
    cp compose.yaml "$tmp/compose.yaml"
    cp 02-messenger-sync/poc/stand/compose.stand.yaml "$tmp/stand.yaml"
    printf 'TG_BOT_TOKEN=synthetic-isolation-probe\nMAX_CA_FILE=synthetic-isolation-probe\nPROBE_PRIVATE_EXTRA=synthetic-isolation-probe\n' >"$tmp/02-messenger-sync/.env"
    docker compose --env-file "$tmp/02-messenger-sync/.env" --profile tools -f "$tmp/compose.yaml" \
        config --format json test smoke >"$tmp/tools.json"
    ! grep -q synthetic-isolation-probe "$tmp/tools.json" || fail 'tools inherited a private env_file'
    HEALTH_STALE_AFTER=6 HEALTH_QUEUE_AGE=8 docker compose --profile tools --profile stand \
        -f "$tmp/compose.yaml" -f "$tmp/stand.yaml" config --format json >"$tmp/stand.json"
    ! grep -q synthetic-isolation-probe "$tmp/stand.json" || fail 'stand inherited a private env_file'
    echo 'compose isolation OK: tools and stand do not inherit the synthetic private env_file'
fi
