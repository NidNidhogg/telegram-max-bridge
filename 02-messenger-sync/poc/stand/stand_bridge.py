#!/usr/bin/env python3
"""Run the unmodified bridge CLI against the stand mock API (test stand only)."""
import asyncio
import json
import os
import signal
import sys
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bridge as b  # noqa: E402

BASE = os.environ['STAND_API_BASE']
MAX_BASE = os.environ.get('STAND_MAX_BASE', BASE)
MOCK_HOST = urllib.parse.urlsplit(BASE).hostname
RealApi, RealBridge, real_check = b.Api, b.Bridge, b.check_media_url


def stand_api(cfg):
    return RealApi(cfg, tg_base=BASE + '/tg', max_base=MAX_BASE + '/max', tg_file_base=BASE + '/tgfile')


def stand_bridge(cfg, api, store, ids):
    bridge = RealBridge(cfg, api, store, ids, send_intervals={'tg': 0.2, 'max': 0.2})
    # Shorter operation budgets than production, still above the 25 s request timeout.
    bridge.budgets = {'poll': 30, 'deliver': 40}
    if POINT == 'worker':
        # D4: STAND_FREEZE=worker:<poll|send>:<platform> hangs exactly one worker forever; the other
        # workers and the heartbeat keep running.
        kind, _, platform = MARKER.partition(':')
        name = 'poll' if kind == 'poll' else 'deliver'
        original, hang = getattr(bridge, name), asyncio.Event()

        async def frozen(target, *args):
            if target == platform:
                print(f'stand: worker {kind}:{platform} frozen', file=sys.stderr, flush=True)
                await hang.wait()
            return await original(target, *args)

        setattr(bridge, name, frozen)
    return bridge


def stand_check(url):
    if urllib.parse.urlsplit(url).hostname != MOCK_HOST:
        real_check(url)


b.Api, b.Bridge, b.check_media_url = stand_api, stand_bridge, stand_check

# D3 crash points: STAND_FREEZE=<point>:<marker>. The process stops itself (SIGSTOP) exactly
# at the point; stand.sh observes SQLite and the mock, then kills the container with SIGKILL.
POINT, _, MARKER = os.environ.get('STAND_FREEZE', '').partition(':')
RealStore = b.Store


def freeze():
    print(f'stand: frozen at crash point {POINT}', file=sys.stderr, flush=True)
    os.kill(os.getpid(), signal.SIGSTOP)


def marked(store, jid):
    row = store.db.execute('SELECT payload FROM jobs WHERE id=?', (jid,)).fetchone()
    return MARKER in row[0]


class StandStore(RealStore):
    def accept_one(self, platform, event, chat_id, own_id):
        super().accept_one(platform, event, chat_id, own_id)
        if POINT == 'accept' and MARKER in json.dumps(event, ensure_ascii=False):
            freeze()  # inside the accept transaction, before COMMIT

    def start_sending(self, job):
        claimed = super().start_sending(job)
        if POINT == 'sending' and claimed and MARKER in job['payload']:
            freeze()  # sending committed, POST not started
        return claimed

    def update(self, jid, state, *args, **kwargs):
        if POINT == 'publish' and state == 'sent' and marked(self, jid):
            freeze()  # API accepted the publication, result not committed
        super().update(jid, state, *args, **kwargs)
        if POINT == 'sent' and state == 'sent' and marked(self, jid):
            freeze()  # sent and remote ID committed


if MARKER and POINT != 'worker':
    b.Store = StandStore

if __name__ == '__main__':
    try:
        sys.exit(b.main() or 0)
    except Exception as exc:  # noqa: BLE001
        sys.exit(b.report_failure(exc))
