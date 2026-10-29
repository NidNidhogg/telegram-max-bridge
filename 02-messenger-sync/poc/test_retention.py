import asyncio
import io
import json
import os
import select
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import retention as r
from test_bridge import FakeApi, b, mx, tg

DAY = 86400


def logical(db):
    return tuple(db.iterdump())


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.store = b.Store(self.state / 'bridge.sqlite3')
        self.addCleanup(self.store.db.close)
        self.db = self.store.db
        self.now = time.time()
        self.policy = r.Settings(budget_ms=200)

    def seed(self, mid=1, state='sent', days=31, platform='tg', text='PRIVATE текст https://secret.invalid'):
        event = tg(mid, text=text) if platform == 'tg' else mx(str(mid), text=text)
        self.store.accept(platform, [event], mid + 1, '-100' if platform == 'tg' else '-200', '99' if platform == 'tg' else '88')
        with self.db:
            self.db.execute('UPDATE jobs SET state=?,remote_id=?,created=?,updated=? WHERE source=? AND mid=?',
                            (state, 'copy-' + str(mid), self.now - (days + 1) * DAY,
                             self.now - days * DAY, platform, str(mid)))
        return self.db.execute('SELECT * FROM jobs WHERE source=? AND mid=? ORDER BY part', (platform, str(mid))).fetchall()

    def maintain(self, **kwargs):
        return r.Maintainer(self.db, 'bridge', self.policy).run(now=self.now, **kwargs)

    def test_settings_validate_without_echoing_values(self):
        self.assertEqual(r.Settings.load({}), r.Settings())
        for key, value in [('RETENTION_CONTENT_ENABLED', '2'), ('RETENTION_CONTENT_DAYS', 'secret'),
                           ('RETENTION_CONTENT_DAYS', '0'), ('RETENTION_HISTORY_DAYS', '29'),
                           ('MAINTENANCE_INTERVAL', '-1'), ('MAINTENANCE_BATCH_SIZE', '1001'),
                           ('MAINTENANCE_BUDGET_MS', '201')]:
            with self.subTest(key=key), self.assertRaises(ValueError) as caught:
                r.Settings.load({key: value})
            self.assertNotIn(value, str(caught.exception))

    def test_exact_boundary_whole_message_and_unknown_dates(self):
        self.seed(1, days=30)
        self.seed(2, days=29.999)
        self.seed(3, text='длинный ' * 1000)
        self.seed(4)
        self.seed(5)
        self.seed(6)
        with self.db:
            self.db.execute("UPDATE jobs SET state='pending' WHERE mid='3' AND part=1")
            self.db.execute("UPDATE jobs SET updated=NULL WHERE mid='4'")
            self.db.execute("UPDATE jobs SET updated=? WHERE mid='5'", (self.now + DAY,))
            self.db.execute("UPDATE jobs SET created='unknown' WHERE mid='6'")
        before = [tuple(row) for row in self.db.execute('SELECT * FROM jobs WHERE mid!=\'1\' ORDER BY id')]
        result = self.maintain()
        self.assertEqual(result['counts']['jobs'], 1)
        self.assertEqual(self.db.execute("SELECT payload FROM jobs WHERE mid='1'").fetchone()[0], r.CLEARED)
        self.assertEqual([tuple(row) for row in self.db.execute('SELECT * FROM jobs WHERE mid!=\'1\' ORDER BY id')], before)

    def test_unresolved_states_and_metadata_survive_idempotent_cleanup(self):
        for mid, state in enumerate(('pending', 'sending', 'failed', 'uncertain', 'sent', 'skipped'), 1):
            self.seed(mid, state=state)
        with self.db:
            self.db.execute('UPDATE jobs SET ready=1234,attempts=7')
        original = [tuple(row) for row in self.db.execute('SELECT id,state,remote_id,ready,attempts,created,updated,parent FROM jobs')]
        protected = [tuple(row) for row in self.db.execute('SELECT * FROM jobs WHERE id<=4')]
        self.assertEqual(self.maintain()['counts']['jobs'], 2)
        for _ in range(3):
            self.assertEqual(self.maintain()['counts']['jobs'], 0)
        self.assertEqual(original, [tuple(row) for row in self.db.execute('SELECT id,state,remote_id,ready,attempts,created,updated,parent FROM jobs')])
        self.assertEqual(protected, [tuple(row) for row in self.db.execute('SELECT * FROM jobs WHERE id<=4')])
        self.assertEqual(self.db.execute('SELECT count(*) FROM inbox').fetchone()[0], 6)
        self.assertEqual(b.queue_view(self.store, 'queue', 'show', 5)['content_cleared'], 1)

    def test_disabling_content_keeps_history_maintenance(self):
        self.seed(days=100)
        with self.db:
            self.store.audit('job', 1, 'confirm', 'uncertain', 'sent', 'PRIVATE reason')
            self.db.execute('UPDATE audit SET created=?', (self.now - 100 * DAY,))
        self.policy = r.Settings(content_enabled=0, budget_ms=200)
        result = self.maintain()
        self.assertEqual(result['counts'], {'jobs': 0, 'quarantine': 0, 'audit': 1})
        self.assertNotEqual(self.db.execute('SELECT payload FROM jobs').fetchone()[0], r.CLEARED)

    def test_quarantine_tombstone_and_unresolved_audit_are_preserved(self):
        bad = mx('bad')
        bad['message']['body']['attachments'] = [{'type': 'image', 'payload': {}}]
        self.store.accept('max', [bad], 2, '-200', '88')
        with self.db:
            self.db.execute("UPDATE quarantine SET state='resolved',created=?,updated=?", (self.now - 101 * DAY, self.now - 100 * DAY))
            self.store.audit('quarantine', 1, 'retry', 'open', 'resolved', 'PRIVATE')
            self.db.execute('UPDATE audit SET created=?', (self.now - 100 * DAY,))
        job = self.seed(1, state='uncertain', days=100, platform='max')[0]
        with self.db:
            self.store.audit('job', job['id'], 'retry', 'failed', 'pending', 'PRIVATE')
            self.db.execute('UPDATE audit SET created=?', (self.now - 100 * DAY,))
        self.assertEqual(self.maintain()['counts'], {'jobs': 0, 'quarantine': 0, 'audit': 0})
        with self.db:
            self.db.execute("UPDATE jobs SET state='sent'")
        self.maintain()  # wrap cursors
        result = self.maintain()
        self.assertEqual(result['counts'], {'jobs': 1, 'quarantine': 1, 'audit': 2})
        before = self.db.execute('SELECT count(*) FROM jobs').fetchone()[0]
        self.store.accept('max', [bad], 3, '-200', '88')
        self.store.accept('max', [mx('bad', text='now valid')], 4, '-200', '88')
        self.assertEqual(self.db.execute('SELECT count(*) FROM jobs').fetchone()[0], before)
        self.assertEqual(self.db.execute('SELECT payload FROM quarantine').fetchone()[0], r.CLEARED)

    def test_batch_cursors_progress_across_completed_tombstones(self):
        for mid in range(1, 8):
            self.seed(mid)
        self.policy = r.Settings(batch_size=2, budget_ms=200)
        for _ in range(5):
            result = self.maintain()
            self.assertLessEqual(result['scanned'], 6)
        self.assertEqual(self.db.execute('SELECT count(*) FROM jobs WHERE payload=?', (r.CLEARED,)).fetchone()[0], 7)

    def test_quarantine_cleanup_fits_default_budget_with_100000_preserved_ids(self):
        old = self.now - 100 * DAY
        with self.db:
            self.db.execute("""WITH RECURSIVE n(i) AS(SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<100000)
                INSERT INTO jobs(source,chat,mid,destination,part,payload,state,created,updated)
                SELECT 'tg','-100',i,'max',0,?,'sent',?,? FROM n""", (r.CLEARED, old - 1, old))
            self.db.executemany("""INSERT INTO quarantine(platform,chat,event_key,payload,error,state,created,updated)
                VALUES('tg','-100',?,'{}','fixed','resolved',?,?)""", [(str(i), old - 1, old) for i in range(30)])
        owner = r.Maintainer(self.db, 'bridge', r.Settings())
        result = owner.run(now=self.now)
        self.assertEqual(result['state'], 'ok')
        self.assertEqual(result['counts']['quarantine'], 30)
        self.assertEqual(self.db.execute('SELECT count(*) FROM jobs').fetchone()[0], 100000)

    def test_dry_run_and_health_keep_logical_snapshots_and_hide_contents(self):
        self.seed()
        alerts = b.AlertMonitor(self.state / 'monitor.sqlite3')
        self.addCleanup(alerts.db.close)
        before = logical(self.db), logical(alerts.db)
        stream = io.StringIO()
        with patch.object(b, 'ENV', self.state / 'absent.env'), redirect_stdout(stream):
            self.assertEqual(b.main(['maintenance', '--state-dir', str(self.state)]), 0)
        self.assertEqual(json.loads(stream.getvalue())['databases']['bridge']['counts']['jobs'], 1)
        self.assertNotIn('PRIVATE', stream.getvalue())
        b.read_health(self.state, storage_policy=b.storage.Settings(free_warn_bytes=2, free_critical_bytes=1))
        self.assertEqual((logical(self.db), logical(alerts.db)), before)

    def test_apply_requires_both_writer_locks_before_mutation(self):
        self.seed()
        before = logical(self.db)
        with b.process_lock(self.state, 'monitor.lock'), patch.object(b, 'ENV', self.state / 'absent.env'):
            with self.assertRaises(b.BridgeError):
                b.main(['maintenance', '--apply', '--state-dir', str(self.state)])
        self.assertEqual(logical(self.db), before)

    def test_missing_dry_run_and_invalid_settings_never_create_state(self):
        missing = self.state / 'missing'
        with redirect_stdout(io.StringIO()), patch.object(b, 'ENV', self.state / 'absent.env'):
            self.assertEqual(b.main(['maintenance', '--state-dir', str(missing)]), 0)
            with patch.dict(os.environ, {'MAINTENANCE_BATCH_SIZE': 'invalid'}), self.assertRaises(b.BridgeError):
                b.main(['maintenance', '--apply', '--state-dir', str(missing)])
        self.assertFalse(missing.exists())

    def test_newer_or_corrupt_database_refused_before_other_database_changes(self):
        self.seed()
        alerts = b.AlertMonitor(self.state / 'monitor.sqlite3')
        alerts.db.execute('PRAGMA user_version=99')
        alerts.db.close()
        before = logical(self.db)
        with patch.object(b, 'ENV', self.state / 'absent.env'), self.assertRaises(b.BridgeError):
            b.main(['maintenance', '--apply', '--state-dir', str(self.state)])
        self.assertEqual(logical(self.db), before)
        (self.state / 'monitor.sqlite3').write_bytes(b'corrupt database')
        with patch.object(b, 'ENV', self.state / 'absent.env'), self.assertRaises(sqlite3.Error):
            b.main(['maintenance', '--apply', '--state-dir', str(self.state)])
        self.assertEqual(logical(self.db), before)

    def test_stopping_and_real_sqlite_full_roll_back_payload(self):
        self.seed()
        payload = self.db.execute('SELECT payload FROM jobs').fetchone()[0]
        self.assertEqual(self.maintain(stopping=lambda: True)['state'], 'deferred')
        self.assertEqual(self.db.execute('SELECT payload FROM jobs').fetchone()[0], payload)
        self.db.executescript('''CREATE TABLE fill(data BLOB);
            CREATE TRIGGER force_full AFTER UPDATE OF payload ON jobs BEGIN
              INSERT INTO fill VALUES(zeroblob(1048576)); END;''')
        pages = self.db.execute('PRAGMA page_count').fetchone()[0]
        self.db.execute(f'PRAGMA max_page_count={pages+1}')
        result = self.maintain()
        self.assertEqual((result['state'], result['reason']), ('error', 'storage_error'))
        self.assertEqual(self.db.execute('SELECT payload FROM jobs').fetchone()[0], payload)
        self.assertEqual(self.db.execute('SELECT count(*) FROM fill').fetchone()[0], 0)
        with sqlite3.connect(self.state / 'bridge.sqlite3') as check:
            self.assertEqual(check.execute('PRAGMA integrity_check').fetchone()[0], 'ok')

    def test_real_busy_is_bounded_and_keeps_cursor(self):
        self.seed()
        before = logical(self.db)
        other = sqlite3.connect(self.state / 'bridge.sqlite3')
        try:
            other.execute('BEGIN IMMEDIATE')
            started = time.monotonic()
            with self.assertRaises(sqlite3.OperationalError):
                self.maintain()
            self.assertLess(time.monotonic() - started, 0.5)
            self.assertEqual(logical(self.db), before)
        finally:
            other.rollback()
            other.close()

    def test_sql_budget_interrupts_a_real_expensive_statement_without_partial_changes(self):
        self.seed()
        self.db.executescript('''CREATE TABLE slow(n INTEGER);
            CREATE TRIGGER slow_cleanup AFTER UPDATE OF payload ON jobs BEGIN
              SELECT sum(a.n+b.n+c.n) FROM slow a,slow b,slow c; END;''')
        with self.db:
            self.db.executemany('INSERT INTO slow VALUES(?)', [(i,) for i in range(200)])
        before = self.db.execute('SELECT payload FROM jobs').fetchone()[0]
        self.policy = r.Settings(budget_ms=5)
        started = time.monotonic()
        result = self.maintain()
        self.assertEqual((result['state'], result['reason']), ('deferred', 'budget'))
        self.assertLess(time.monotonic() - started, 0.5)
        self.assertEqual(result['cursors'], {})
        self.assertEqual(self.db.execute('SELECT payload FROM jobs').fetchone()[0], before)
        self.db.execute('DROP TRIGGER slow_cleanup')
        self.assertEqual(self.maintain()['counts']['jobs'], 1)

    def test_real_sigkill_during_cleanup_rolls_back_and_then_cleans_once(self):
        self.seed()
        script = '''import os,signal,sys,time
from pathlib import Path
import bridge as b,retention as r
s=b.Store(Path(sys.argv[1])/'bridge.sqlite3')
def freeze(result):
 print('in_transaction',flush=True);os.kill(os.getpid(),signal.SIGSTOP)
owner=r.Maintainer(s.db,'bridge',r.Settings(budget_ms=200));owner.save=freeze
owner.run()
'''
        child = subprocess.Popen([sys.executable, '-c', script, str(self.state)], cwd=Path(__file__).parent,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertTrue(select.select([child.stdout], [], [], 5)[0], 'cleanup child did not reach transaction')
            self.assertEqual(child.stdout.readline().strip(), 'in_transaction')
            child.kill()
            child.wait(timeout=5)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
            child.stdout.close()
            child.stderr.close()
        self.assertNotEqual(self.db.execute('SELECT payload FROM jobs').fetchone()[0], r.CLEARED)
        self.assertEqual(self.maintain()['counts']['jobs'], 1)
        self.assertEqual(self.maintain()['counts']['jobs'], 0)

    def test_pages_are_reused_checkpoint_defers_for_real_reader_and_compact_preserves_ids(self):
        self.seed(text='я' * 80000)
        for _ in range(4):
            self.maintain()
        before = r.metrics(self.db)
        self.assertGreater(before['reusable_pages'], 0)
        self.db.execute('PRAGMA wal_autocheckpoint=0')
        r.checkpoint(self.db)
        reader = sqlite3.connect(self.state / 'bridge.sqlite3')
        try:
            reader.execute('BEGIN')
            reader.execute('SELECT * FROM jobs').fetchone()
            self.seed(2)
            self.assertEqual(r.checkpoint(self.db)['state'], 'deferred')
        finally:
            reader.close()
        self.assertEqual(r.checkpoint(self.db)['state'], 'ok')
        reuse_before = r.metrics(self.db)
        self.seed(3, text='я' * 45000)
        reuse_after = r.metrics(self.db)
        self.assertLess(reuse_after['reusable_pages'], reuse_before['reusable_pages'])
        for _ in range(3):
            self.maintain()
        original = list(self.db.execute('SELECT id,mid,remote_id FROM jobs'))
        old_pages = r.metrics(self.db)['pages']
        old_bytes = (self.state / 'bridge.sqlite3').stat().st_size
        with patch.object(b, 'ENV', self.state / 'absent.env'), patch.dict(os.environ, {'STORAGE_FREE_CRITICAL_BYTES': '1'}), redirect_stdout(io.StringIO()):
            self.assertEqual(b.main(['maintenance', '--apply', '--compact', '--state-dir', str(self.state)]), 0)
        self.assertLess(r.metrics(self.db)['pages'], old_pages)
        self.assertLess((self.state / 'bridge.sqlite3').stat().st_size, old_bytes)
        self.assertEqual(list(self.db.execute('SELECT id,mid,remote_id FROM jobs')), original)
        print('D5.2 storage measurements: ' + json.dumps({'after_cleanup': before, 'before_reuse': reuse_before,
              'after_reuse': reuse_after, 'after_compact': r.metrics(self.db), 'physical_before': old_bytes,
              'physical_after': (self.state / 'bridge.sqlite3').stat().st_size}, sort_keys=True))

    def test_compact_reports_deferred_if_a_reader_appears_before_final_checkpoint(self):
        self.seed(text='я' * 10000)
        real_checkpoint = r.checkpoint
        readers = []

        def with_reader(db, *, truncate=False):
            if truncate:
                reader = sqlite3.connect(self.state / 'bridge.sqlite3')
                readers.append(reader)
                reader.execute('BEGIN')
                reader.execute('SELECT * FROM jobs').fetchone()
            return real_checkpoint(db, truncate=truncate)

        try:
            with (patch.object(b, 'ENV', self.state / 'absent.env'),
                  patch.dict(os.environ, {'STORAGE_FREE_CRITICAL_BYTES': '1'}),
                  patch.object(r, 'checkpoint', with_reader), redirect_stdout(io.StringIO()) as output):
                code = b.main(['maintenance', '--apply', '--compact', '--state-dir', str(self.state)])
            self.assertEqual(code, 1)
            result = json.loads(output.getvalue())['databases']['bridge']
            self.assertTrue(result['compacted'])
            self.assertEqual(result['checkpoint']['state'], 'deferred')
            self.assertEqual(result['checkpoint']['mode'], 'TRUNCATE')
        finally:
            for reader in readers:
                reader.close()
        self.assertEqual(real_checkpoint(self.db, truncate=True)['state'], 'ok')


class MonitorRetentionTests(unittest.TestCase):
    def test_only_old_delivered_closed_events_expire_and_legacy_dates_are_conservative(self):
        with tempfile.TemporaryDirectory() as directory:
            owner = b.AlertMonitor(Path(directory) / 'monitor.sqlite3')
            try:
                now = time.time()
                issue = {'key': 'blocked:max', 'code': 'delivery_blocked'}
                for key, delivered, since in [('old', 1, now - 91 * DAY), ('pending', 0, now - 91 * DAY),
                                               ('blocked:max', 1, now - 91 * DAY), ('legacy', 1, None),
                                               ('invalid', 1, 'unknown')]:
                    payload = {'event_id': key, 'kind': 'problem', 'detected_at': now - 100 * DAY, 'issue': {'key': key}}
                    if since is not None:
                        payload['retention_since'] = since
                    owner.db.execute('INSERT INTO alerts(event_id,payload,delivered) VALUES(?,?,?)', (key, json.dumps(payload), delivered))
                owner.db.execute("INSERT INTO monitor_state VALUES('issues',?)", (json.dumps({issue['key']: issue}),))
                owner.db.commit()
                result = r.Maintainer(owner.db, 'monitor', r.Settings()).run(now=now)
                self.assertEqual(result['counts'], {'alerts': 1, 'legacy': 1})
                remaining = {row['event_id']: row for row in owner.db.execute('SELECT * FROM alerts')}
                self.assertEqual(set(remaining), {'pending', 'blocked:max', 'legacy', 'invalid'})
                self.assertEqual(json.loads(remaining['legacy']['payload'])['retention_since'], now)
                self.assertEqual(remaining['pending']['delivered'], 0)
                seen = []
                owner.flush(seen.append, stopping=lambda: True)
                self.assertEqual(seen, [])
                owner.flush(seen.append)
                self.assertEqual(seen[0]['event_id'], 'pending')
                acknowledged = json.loads(owner.db.execute("SELECT payload FROM alerts WHERE event_id='pending'").fetchone()[0])
                self.assertGreater(acknowledged['retention_since'], now - 1)
            finally:
                owner.db.close()


class RetentionRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_old_replies_and_redelivery_both_directions_after_auto_cleanup(self):
        with tempfile.TemporaryDirectory() as directory, b.Store(Path(directory) / 'bridge.sqlite3') as store:
            now = time.time()
            store.accept('tg', [tg(1), tg(2)], 3, '-100', '99')
            store.accept('max', [mx('first')], 3, '-200', '88')
            for jid in (1, 2, 3):
                store.update(jid, 'sent', remote_id=str(1000 + jid) if jid == 3 else 'copy-' + str(jid))
            with store.db:
                store.db.execute('UPDATE jobs SET created=?,updated=?', (now - 32 * DAY, now - 31 * DAY))
            api = FakeApi()

            async def request(platform, path, **kwargs):
                if path == 'getUpdates':
                    return []
                if path == '/updates':
                    return {'updates': [], 'marker': '3'}
                return await FakeApi.request(api, platform, path, **kwargs)

            api.request = request
            bridge = b.Bridge({'TG_CHAT_ID': '-100', 'MAX_CHAT_ID': '-200'}, api, store,
                              {'tg': '99', 'max': '88'}, send_intervals={'tg': 0, 'max': 0})
            bridge.maintainer = r.Maintainer(store.db, 'bridge', r.Settings(interval=1))
            stop = asyncio.Event()
            task = asyncio.create_task(b.run_bridge(bridge, stop, poll_interval=0.02, heartbeat_interval=0.02))
            try:
                for _ in range(100):
                    if store.db.execute('SELECT count(*) FROM jobs WHERE payload=?', (r.CLEARED,)).fetchone()[0] == 3:
                        break
                    await asyncio.sleep(0.01)
                self.assertEqual(store.db.execute('SELECT count(*) FROM jobs WHERE payload=?', (r.CLEARED,)).fetchone()[0], 3)
                store.accept('tg', [tg(1), tg(2)], 3, '-100', '99')
                store.accept('max', [mx('first')], 3, '-200', '88')
                self.assertEqual(api.calls, [])
                reply_tg = tg(3)
                reply_tg['message']['reply_to_message'] = tg(1)['message']
                reply_max = mx('second')
                reply_max['message']['link'] = {'type': 'reply', 'message': {'mid': 'first', 'text': 'PRIVATE'}}
                store.accept('tg', [reply_tg], 4, '-100', '99')
                store.accept('max', [reply_max], 4, '-200', '88')
                for _ in range(100):
                    if len(api.calls) == 2:
                        break
                    await asyncio.sleep(0.02)
                self.assertEqual(len(api.calls), 2)
                self.assertEqual(store.db.execute('SELECT count(*) FROM jobs').fetchone()[0], 5)
                self.assertIn('copy-1', json.dumps(api.calls))
                self.assertIn('1003', json.dumps(api.calls))
            finally:
                stop.set()
                await task


if __name__ == '__main__':
    unittest.main()
