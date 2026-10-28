#!/bin/sh
# Regression check for the stand guard: a fake docker records every call, so nothing real is
# touched. The stand must refuse before any destructive command when its project could be a
# working one, and must only ever run down -v against its own project.
set -eu
cd "$(dirname "$0")/../../.."
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cat >"$tmp/docker" <<'FAKE'
#!/bin/sh
echo "$*" >>"$FAKE_LOG"
case "$*" in
    *telegram-max-bridge.stand=1*) exit 0 ;;
    'ps -aq '*|'volume ls -q '*) [ -z "${FAKE_FOREIGN:-}" ] || echo foreign; exit 0 ;;
    *' build '*) exit 1 ;;
esac
exit 0
FAKE
chmod +x "$tmp/docker"

run() {
    : >"$tmp/log"
    if FAKE_LOG="$tmp/log" PATH="$tmp:$PATH" make --no-print-directory stand "$@" >"$tmp/out" 2>&1; then
        echo "guard FAIL: make stand $* unexpectedly succeeded" >&2; exit 1
    fi
}
no_down() {
    ! grep -q ' down' "$tmp/log" || { echo "guard FAIL: $1 ran down:" >&2; cat "$tmp/log" >&2; exit 1; }
    grep -q 'stand refused' "$tmp/out" || { echo "guard FAIL: $1 was not refused" >&2; cat "$tmp/out" >&2; exit 1; }
}

run PROJECT=same STAND_PROJECT=same
no_down 'stand project equal to PROJECT'
run PROJECT=other STAND_PROJECT=telegram-max-bridge
no_down 'stand project equal to the default working project'
FAKE_FOREIGN=1 run PROJECT=work STAND_PROJECT=foreign
no_down 'stand project with resources it did not create'

run PROJECT=telegram-max-bridge-stand
grep -q ' down' "$tmp/log" || { echo 'guard FAIL: expected the stand to clean its own project' >&2; exit 1; }
if grep ' down' "$tmp/log" | grep -vq -- '-p telegram-max-bridge-stand-stand '; then
    echo 'guard FAIL: down -v outside the derived stand project' >&2; cat "$tmp/log" >&2; exit 1
fi
echo 'stand guard OK: colliding or foreign projects are refused before any down -v'
