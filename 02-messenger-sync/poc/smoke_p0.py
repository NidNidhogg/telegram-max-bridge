#!/usr/bin/env python3
"""Repeatable P0 smoke checks: temporary databases, real CLI, no network calls."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import bridge as b


SCRIPT = Path(b.__file__).resolve()
CFG = {'TG_CHAT_ID': '-100', 'MAX_CHAT_ID': '-200'}
IDS = {'tg': '99', 'max': '88'}


def tg(mid, text='synthetic'):
    return {'update_id': mid, 'message': {'message_id': mid, 'chat': {'id': -100},
            'from': {'id': 10, 'first_name': 'Test'}, 'text': text}}


def mx(mid, broken=False):
    body = {'mid': mid, 'text': 'synthetic'}
    if broken:
        body['attachments'] = [{'type': 'image', 'payload': {}}]
    return {'update_type': 'message_created', 'message': {'recipient': {'chat_id': -200},
            'sender': {'user_id': 20, 'first_name': 'Test'}, 'body': body}}


class LocalApi:
    def __init__(self, state):
        self.state = state
        self.sent = 0

    def request(self, service, path, **_kwargs):
        if service == 'tg' and path == 'getUpdates':
            return []
        if service == 'max' and path == '/updates':
            if (self.state / 'synthetic-failure').exists():
                raise b.TransportError('Синтетический сбой MAX')
            return {'updates': [], 'marker': 42}
        self.sent += 1
        if service == 'tg' and path == 'sendMessage':
            return {'message_id': 9000 + self.sent}
        if service == 'max' and path == '/messages':
            return {'message': {'body': {'mid': f'synthetic-{self.sent}'}}}
        raise AssertionError('Unexpected synthetic API call')


def cli(state, *args, expected=0):
    result = subprocess.run([sys.executable, str(SCRIPT), *args, '--state-dir', str(state)],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == expected, (args, result.returncode, result.stderr)
    return result.stdout


def wait_for(predicate, seconds=10):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError('Timed out waiting for synthetic state')


def stop(process):
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            raise AssertionError('Synthetic child did not stop cleanly')


def smoke_ingest(state):
    with b.Store(state / 'bridge.sqlite3') as store:
        for _ in range(2):
            store.accept('max', [mx('before'), mx('bad', broken=True), mx('after')], 123, '-200', '88')
        bridge = b.Bridge(CFG, LocalApi(state), store, IDS)
        bridge.deliver('tg')
        with store.db:
            store.set('next_tg', 0)
        bridge.deliver('tg')
    with b.Store(state / 'bridge.sqlite3', readonly=True) as store:
        assert store.get('cursor_max') == '123'
        assert store.db.execute("SELECT count(*) FROM jobs WHERE state='sent'").fetchone()[0] == 2
        assert store.db.execute("SELECT count(*) FROM quarantine WHERE state='open'").fetchone()[0] == 1
    print('P0.1 OK: good/bad/good, redelivery, publication and reopen')


def smoke_queue(state):
    with b.Store(state / 'bridge.sqlite3') as store:
        store.accept('tg', [tg(1), tg(2), tg(3, 'x' * 9000)], 4, '-100', '99')
        store.update(1, 'uncertain')
        store.update(2, 'failed')
    cli(state, 'queue', 'confirm', '1', '--remote-id', 'manual-copy', '--reason', 'synthetic review')
    cli(state, 'queue', 'retry', '2', '--reason', 'synthetic fix')
    cli(state, 'queue', 'skip', '3', '--remaining', '--reason', 'synthetic omission')
    cli(state, 'queue', 'confirm', '1', '--remote-id', 'manual-copy', '--reason', 'synthetic review')
    with b.Store(state / 'bridge.sqlite3') as store:
        b.Bridge(CFG, LocalApi(state), store, IDS).deliver('max')
        assert [r[0] for r in store.db.execute('SELECT state FROM jobs ORDER BY id')] == [
            'sent', 'sent', 'skipped', 'skipped', 'skipped']
        assert store.db.execute('SELECT count(*) FROM audit').fetchone()[0] == 5
    print('P0.2 OK: real CLI, recovery, idempotent confirm, multipart skip and audit')


def smoke_monitor(state):
    child = watcher = None
    alerts_path = state / 'alerts.jsonl'

    def alerts():
        if not alerts_path.exists():
            return []
        try:
            return [json.loads(line) for line in alerts_path.read_text().splitlines()]
        except json.JSONDecodeError:
            return []  # A concurrent append may not yet have finished.

    def launch_watcher():
        return subprocess.Popen([sys.executable, str(SCRIPT), 'monitor', '--state-dir', str(state),
                                 '--interval', '0.2', '--alerts-file', str(alerts_path)],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    try:
        child = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--child', str(state)],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        wait_for(lambda: b.read_health(state)['ok'])
        assert json.loads(cli(state, 'health', '--json'))['ok']
        watcher = launch_watcher()
        (state / 'synthetic-failure').touch()
        wait_for(lambda: len(alerts()) == 1)
        assert alerts()[0]['kind'] == 'problem'
        assert alerts()[0]['issue']['key'] == 'poll:max'
        time.sleep(0.4)
        assert len(alerts()) == 1
        stop(watcher)
        assert watcher.returncode == 0
        watcher = launch_watcher()
        time.sleep(0.4)
        assert len(alerts()) == 1
        (state / 'synthetic-failure').unlink()
        wait_for(lambda: len(alerts()) == 2)
        assert alerts()[1]['kind'] == 'recovery'
        stop(child)
        assert child.returncode == 0
        wait_for(lambda: len(alerts()) == 3)
        assert alerts()[2]['issue']['code'] == 'process_stopped'
        assert not json.loads(cli(state, 'health', '--json', expected=1))['ok']
        assert alerts_path.stat().st_mode & 0o777 == 0o600
        print('P0.3 OK: running bridge, API failure, monitor restart without duplicates, recovery, SIGTERM and health exit codes')
    finally:
        stop(watcher)
        stop(child)


def main():
    if len(sys.argv) == 3 and sys.argv[1] == '--child':
        state = Path(sys.argv[2])
        b.config = lambda require_chats=False: CFG
        b.doctor = lambda *args, **kwargs: IDS
        b.Api = lambda cfg: LocalApi(state)
        return b.main(['run', '--state-dir', str(state)])
    with tempfile.TemporaryDirectory(prefix='bridge-p0-smoke-') as tmp:
        root = Path(tmp)
        for name, check in [('ingest', smoke_ingest), ('queue', smoke_queue), ('monitor', smoke_monitor)]:
            state = root / name
            state.mkdir(mode=0o700)
            check(state)
    print('All P0 smoke checks passed; no real API calls or messages')


if __name__ == '__main__':
    sys.exit(main() or 0)
