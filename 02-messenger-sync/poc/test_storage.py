import asyncio
import io
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import bridge as b
import storage as s
from test_bridge import mx, tg


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)
        self.policy = s.Settings(warn_bytes=10000, critical_bytes=20000, free_warn_bytes=2, free_critical_bytes=1)

    def tearDown(self):
        self.tmp.cleanup()

    def test_settings_precedence_validation_and_secret_free_errors(self):
        env = self.path / '.env'
        env.write_text('STORAGE_WARN_BYTES=1234\nSTORAGE_CRITICAL_BYTES=5000\nTG_BOT_TOKEN=SECRET\n')
        policy = b.storage_settings({'STORAGE_WARN_BYTES': '2345'}, env)
        self.assertEqual(policy.warn_bytes, 2345)
        for key, value in [('STORAGE_WARN_BYTES', 'SECRET'), ('STORAGE_WARN_BYTES', '-1'),
                           ('STORAGE_WARN_BYTES', '0'), ('STORAGE_WARN_BYTES', '5000'),
                           ('STORAGE_WARN_BYTES', '9' * 5000), ('STORAGE_WARN_BYTES', '²'),
                           ('ALERTS_BACKUP_COUNT', '33'), ('ALERTS_MAX_BYTES', '100'),
                           ('STORAGE_CHECK_INTERVAL', '61'), ('ALERTS_RETENTION_DAYS', '3651')]:
            with self.subTest(key=key, value=value[:20]), self.assertRaises(b.BridgeError) as caught:
                b.storage_settings({key: value}, env)
            self.assertNotIn('SECRET', str(caught.exception))
        self.assertEqual(b.storage_settings({'ALERTS_BACKUP_COUNT': '0'}, env).alerts_backup_count, 0)

    def test_snapshot_measures_only_managed_files_and_real_volume_free_space(self):
        for name, size in [('bridge.sqlite3', 11), ('bridge.sqlite3-wal', 12), ('monitor.sqlite3', 13),
                           ('monitor.sqlite3-shm', 14), ('alerts.jsonl', 15), ('alerts.jsonl.4', 16),
                           ('bridge.sqlite3.v2-secret.bak', 1000), ('unrelated', 999)]:
            (self.path / name).write_bytes(b'x' * size)
        view = s.snapshot(self.path, self.policy)
        self.assertEqual(view['managed_bytes'], 81)
        self.assertGreater(view['free_bytes'], 0)
        self.assertEqual(view['state'], 'ok')
        self.assertNotIn('secret', json.dumps(view))
        self.assertEqual(view['files']['bridge.sqlite3-shm'], 0)

    def test_thresholds_and_warning_is_not_unhealthy(self):
        store = b.Store(self.path / 'bridge.sqlite3')
        store.runtime('running', 'storage-test')
        for platform, chat, own in [('tg', '-100', '99'), ('max', '-200', '88')]:
            store.accept(platform, [], None, chat, own)
        total = s.snapshot(self.path, s.Settings())['managed_bytes']
        warn = replace(self.policy, warn_bytes=total, critical_bytes=total+10000)
        view = s.snapshot(self.path, warn)
        self.assertEqual(view['state'], 'warning')
        self.assertFalse(view['issues'])
        self.assertEqual(view['warnings'][0]['severity'], 'warning')
        critical = s.snapshot(self.path, replace(warn, warn_bytes=1, critical_bytes=total))
        self.assertEqual(critical['state'], 'critical')
        self.assertEqual(critical['issues'][0]['key'], 'storage:size')
        with patch.object(b, 'bridge_running', return_value=True):
            self.assertTrue(b.read_health(self.path, storage_policy=warn)['ok'])
            self.assertFalse(b.read_health(self.path, storage_policy=replace(warn, warn_bytes=1, critical_bytes=total))['ok'])
        store.db.close()

    def test_bad_measurement_is_unknown_not_zero_or_success(self):
        for failure in (PermissionError('SECRET_PATH'), OSError('PRIVATE_HOST')):
            with patch.object(s.os, 'fstatvfs', side_effect=failure):
                view = s.snapshot(self.path, self.policy)
            self.assertEqual(view['state'], 'unknown')
            self.assertIsNone(view['free_bytes'])
            self.assertEqual(view['issues'][0]['code'], 'storage_unavailable')
            self.assertNotIn('SECRET', json.dumps(view))
        view = s.snapshot(self.path / 'absent', self.policy)
        self.assertEqual(view['state'], 'unknown')
        self.assertFalse((self.path / 'absent').exists())

    def test_links_and_nonregular_files_are_not_followed(self):
        target = self.path / 'outside'
        target.write_text('PRIVATE_DATA')
        alert = self.path / 'alerts.jsonl'
        alert.symlink_to(target)
        self.assertEqual(s.snapshot(self.path, self.policy)['state'], 'unknown')
        alert.unlink()
        os.link(target, alert)
        self.assertEqual(s.snapshot(self.path, self.policy)['state'], 'unknown')
        alert.unlink()
        os.mkfifo(alert)
        self.assertEqual(s.snapshot(self.path, self.policy)['state'], 'unknown')
        self.assertEqual(target.read_text(), 'PRIVATE_DATA')

    def test_diagnostics_do_not_modify_logical_sqlite_data(self):
        store = b.Store(self.path / 'bridge.sqlite3')
        store.accept('tg', [tg()], 2, '-100', '99')
        def dump():
            return list(store.db.iterdump())
        before = dump()
        with patch.object(b, 'storage_settings', return_value=self.policy), redirect_stdout(io.StringIO()) as output:
            self.assertEqual(b.main(['health', '--json', '--state-dir', str(self.path)]), 1)
        view = json.loads(output.getvalue())
        self.assertIn('storage', view)
        self.assertEqual(before, dump())
        store.db.close()

    def test_monitor_deduplicates_byte_changes_and_keeps_unknown_problem(self):
        monitor = b.AlertMonitor(self.path / 'monitor.sqlite3')
        def observe(level, size=10):
            entry = {'key': 'storage:size', 'code': 'storage_size', 'severity': level}
            view = {'platforms': {'tg': {}}, 'issues': [], 'warnings': [], 'generated_at': time.time(),
                    'storage': {'state': level, 'managed_bytes': size}}
            if level in ('warning', 'critical'):
                view['warnings' if level == 'warning' else 'issues'].append(entry)
            monitor.observe(view)
        observe('warning')
        for size in range(20, 30):
            observe('warning', size)
        observe('unknown')
        emitted = []
        monitor.flush(emitted.append)
        self.assertEqual([e['kind'] for e in emitted], ['problem'])
        self.assertEqual(emitted[0]['level'], 'warning')
        monitor.db.close()
        monitor = b.AlertMonitor(self.path / 'monitor.sqlite3')
        observe('ok')
        self.assertEqual(monitor.flush(emitted.append), 0)
        observe('ok')
        self.assertEqual(monitor.flush(emitted.append), 1)
        self.assertEqual([e['kind'] for e in emitted], ['problem', 'recovery'])
        monitor.db.close()

    def test_gate_latches_until_two_fresh_samples_with_headroom(self):
        gate = s.Gate(self.path, self.policy)
        def view(level, size, free=10):
            return {'state': level, 'managed_bytes': size, 'free_bytes': free}
        with patch.object(s, 'snapshot', return_value=view('critical', 20000)):
            self.assertFalse(gate.allow_poll(0))
        with patch.object(s, 'snapshot', return_value=view('warning', 17000)):
            self.assertFalse(gate.allow_poll(5))  # Below the limit, but not enough headroom.
        with patch.object(s, 'snapshot', return_value=view('ok', 1)):
            self.assertFalse(gate.allow_poll(10))
            self.assertFalse(gate.allow_poll(10))  # The other poller must not count this as a second sample.
            self.assertTrue(gate.allow_poll(15))
        with patch.object(s, 'snapshot', return_value=view('unknown', None, None)):
            self.assertFalse(gate.allow_poll(20))

    def test_monitor_rejects_unmanaged_sink_before_creating_state(self):
        state = self.path/'missing-state'
        outside = self.path/'unrelated'
        outside.write_text('KEEP')
        with self.assertRaises(b.BridgeError):
            b.main(['monitor', '--once', '--state-dir', str(state), '--alerts-file', str(outside)])
        self.assertFalse(state.exists())
        self.assertEqual(outside.read_text(), 'KEEP')


class RotationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)
        self.policy = s.Settings(alerts_max_bytes=1024, alerts_backup_count=2)
        self.sink = s.AlertFile(self.path / 'alerts.jsonl', self.policy)

    def tearDown(self):
        self.tmp.cleanup()

    def alert(self, n=0, moment=None):
        return {'event_id': str(n), 'detected_at': time.time() if moment is None else moment, 'text': 'Юникод🌿' * 20}

    def test_rotation_is_bounded_by_bytes_and_preserves_complete_json(self):
        for n in range(40):
            self.sink(self.alert(n))
        paths = list(self.path.glob('alerts.jsonl*'))
        self.assertLessEqual(len(paths), 3)
        all_records = []
        for path in paths:
            self.assertLessEqual(path.stat().st_size, 1024)
            all_records.extend(json.loads(line) for line in path.read_text().splitlines())
        self.assertEqual(max(int(e['event_id']) for e in all_records), 39)

    def test_no_archives_and_oversized_record_keep_unacknowledged_outbox(self):
        sink = s.AlertFile(self.sink.path, replace(self.policy, alerts_backup_count=0))
        for n in range(10):
            sink(self.alert(n))
        self.assertEqual(list(self.path.glob('alerts.jsonl*')), [self.sink.path])
        before = self.sink.path.read_bytes()
        with self.assertRaises(OSError):
            sink({'text': 'x'*2000})
        self.assertEqual(self.sink.path.read_bytes(), before)

    def test_expiration_runs_without_new_alerts_and_respects_owned_paths(self):
        now = time.time()
        self.sink(self.alert(0, now-90*86400))
        unrelated = self.path / 'alerts.jsonl.backup'
        unrelated.write_text('KEEP')
        self.sink.maintain(now)
        self.assertFalse(self.sink.path.exists())
        self.assertEqual(unrelated.read_text(), 'KEEP')
        self.sink(self.alert(1, now+100))  # Clock going backwards does not break the sink.
        self.sink.maintain(now)
        self.assertTrue(self.sink.path.exists())

    def test_lower_limits_remove_oversized_legacy_files_and_extra_archives(self):
        raw = (json.dumps(self.alert())+'\n').encode()
        self.sink.path.write_bytes(raw*10)
        (self.path/'alerts.jsonl.3').write_bytes(raw)
        (self.path/'alerts.jsonl.1').write_bytes(raw*10)
        self.sink.maintain()
        self.assertFalse(self.sink.path.exists())
        self.assertFalse((self.path/'alerts.jsonl.3').exists())
        self.assertFalse((self.path/'alerts.jsonl.1').exists())

    def test_invalid_links_cannot_write_delete_or_truncate_outside(self):
        target = self.path / 'PRIVATE_TARGET'
        target.write_text('KEEP')
        for name in ('alerts.jsonl', 'alerts.jsonl.1'):
            link = self.path / name
            link.symlink_to(target)
            with self.assertRaises(OSError):
                self.sink(self.alert())
            self.assertEqual(target.read_text(), 'KEEP')
            link.unlink()

    def test_partial_last_line_is_repaired_and_pending_event_replayed(self):
        monitor = b.AlertMonitor(self.path / 'monitor.sqlite3')
        monitor.observe(b.health_snapshot(None))
        event_id = monitor.db.execute('select event_id from alerts').fetchone()[0]
        self.sink.path.write_bytes(b'{"unfinished":')
        monitor.flush(self.sink)
        records = [json.loads(line) for line in self.sink.path.read_text().splitlines()]
        self.assertEqual([r['event_id'] for r in records], [event_id])
        self.assertEqual(monitor.db.execute('select delivered from alerts').fetchone()[0], 1)
        monitor.db.close()

    def test_fsync_failure_never_acknowledges_and_replay_has_same_event_id(self):
        monitor = b.AlertMonitor(self.path / 'monitor.sqlite3')
        monitor.observe(b.health_snapshot(None))
        event_id = monitor.db.execute('select event_id from alerts').fetchone()[0]
        with patch.object(s.os, 'fsync', side_effect=OSError('PRIVATE_IO')):
            with self.assertRaises(OSError):
                monitor.flush(self.sink)
        self.assertEqual(monitor.db.execute('select delivered from alerts').fetchone()[0], 0)
        monitor.db.close()
        monitor = b.AlertMonitor(self.path / 'monitor.sqlite3')
        monitor.flush(self.sink)
        records = [json.loads(line) for line in self.sink.path.read_text().splitlines()]
        self.assertTrue(all(r['event_id'] == event_id for r in records))
        self.assertEqual(monitor.db.execute('select delivered from alerts').fetchone()[0], 1)
        monitor.db.close()

    @unittest.skipUnless(sys.platform == 'linux', 'real SIGSTOP/SIGKILL exercised in Docker')
    def test_process_killed_after_sink_write_replays_same_id_on_restart(self):
        script = '''
import os,signal,sys
from pathlib import Path
import bridge as b
import storage as s
state=Path(sys.argv[1]); monitor=b.AlertMonitor(state/'monitor.sqlite3')
monitor.observe(b.health_snapshot(None))
sink=s.AlertFile(state/'alerts.jsonl',s.Settings())
def write(alert):
    sink(alert)
    (state/'written').write_text(alert['event_id'])
    os.kill(os.getpid(),signal.SIGSTOP)
monitor.flush(write)
'''
        proc = subprocess.Popen([sys.executable, '-c', script, str(self.path)], cwd=Path(__file__).parent,
                                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic()+10
            while not (self.path/'written').exists() and time.monotonic()<deadline and proc.poll() is None:
                time.sleep(.02)
            self.assertTrue((self.path/'written').exists())
            event_id = (self.path/'written').read_text()
            conn = sqlite3.connect(self.path/'monitor.sqlite3')
            self.assertEqual(conn.execute('select delivered from alerts').fetchone()[0], 0)
            conn.close()
            proc.kill()
            proc.communicate(timeout=5)
            self.assertEqual(proc.returncode, -signal.SIGKILL)
            monitor = b.AlertMonitor(self.path/'monitor.sqlite3')
            monitor.flush(s.AlertFile(self.path/'alerts.jsonl', s.Settings()))
            self.assertEqual(monitor.db.execute('select delivered from alerts').fetchone()[0], 1)
            monitor.db.close()
            records = [json.loads(line) for line in (self.path/'alerts.jsonl').read_text().splitlines()]
            self.assertEqual([r['event_id'] for r in records], [event_id, event_id])
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.communicate(timeout=5)


class StorageRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_pressure_pauses_polling_but_delivers_saved_jobs_then_resumes(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp)
            store = b.Store(state/'bridge.sqlite3')
            store.accept('tg', [tg()], 2, '-100', '99')
            store.accept('max', [mx()], 1, '-200', '88')
            calls = []
            class Api:
                async def request(self, platform, path, **_kwargs):
                    calls.append(path)
                    if path == 'getUpdates':
                        return []
                    if path == '/updates':
                        return {'updates': [], 'marker': '42'}
                    return {'message_id': 77} if platform == 'tg' else {'message': {'body': {'mid': 'copy'}}}
            bridge = b.Bridge({'TG_CHAT_ID': '-100', 'MAX_CHAT_ID': '-200'}, Api(), store,
                              {'tg': '99', 'max': '88'}, send_intervals={'tg': 0, 'max': 0})
            bridge.storage_gate = s.Gate(state, s.Settings(check_interval=.01))
            pressure = {'state': 'critical', 'managed_bytes': 600*1024**2, 'free_bytes': 1024**3}
            stop = asyncio.Event()
            with patch.object(s, 'snapshot', side_effect=lambda *_: pressure), redirect_stdout(io.StringIO()):
                task = asyncio.create_task(b.run_bridge(bridge, stop, poll_interval=.01, delivery_interval=.01,
                                                       heartbeat_interval=.01, shutdown_timeout=.1))
                try:
                    await asyncio.sleep(.08)
                    self.assertEqual([r[0] for r in store.db.execute('select state from jobs')], ['sent', 'sent'])
                    self.assertNotIn('getUpdates', calls)
                    self.assertNotIn('/updates', calls)
                    self.assertEqual(store.get('cursor_tg'), '2')
                    self.assertEqual(store.get('cursor_max'), '1')
                    self.assertEqual(bridge.workers['poll:tg']['phase'], 'storage_wait')
                    pressure = {'state': 'ok', 'managed_bytes': 1, 'free_bytes': 1024**3}
                    await asyncio.sleep(.08)
                    self.assertIn('getUpdates', calls)
                    self.assertIn('/updates', calls)
                    self.assertEqual(store.get('cursor_max'), '42')
                    self.assertEqual(calls.count('sendMessage')+calls.count('/messages'), 2)
                finally:
                    stop.set()
                    await task
                    store.db.close()


if __name__ == '__main__':
    unittest.main()
