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


class ShutdownTests(unittest.TestCase):
    PROBE = '''import asyncio,json,os,signal,sqlite3,sys
from pathlib import Path
import bridge as b
from test_bridge import tg,mx
mode,platform,directory=sys.argv[1:]
state=Path(directory);signalled=False;inside=False;before={};late=[];posts=[];once=False
keys=('runtime_state','heartbeat','run_started','run_stopped','run_id')
def snapshot(store):
 return {'meta':{k:store.get(k) for k in keys},'jobs':[tuple(r) for r in store.db.execute('SELECT * FROM jobs ORDER BY id')]}
class StopStore(b.Store):
 def accept_one(self,source,event,chat_id,own_id):
  global signalled,inside
  super().accept_one(source,event,chat_id,own_id)
  if mode=='accept' and source==platform and not signalled:
   inside=self.db.in_transaction;signalled=True;os.kill(os.getpid(),signal.SIGTERM)
 def runtime(self,status,run_id=None):
  global before,signalled
  if mode=='full' and status=='stopped':
   before=snapshot(self)
   self.db.executescript("""CREATE TABLE stop_full_space(data BLOB);
    CREATE TRIGGER stop_full BEFORE INSERT ON meta WHEN NEW.key='run_stopped'
    BEGIN INSERT INTO stop_full_space VALUES(zeroblob(1048576)); END;""")
   pages=self.db.execute('PRAGMA page_count').fetchone()[0]
   self.db.execute(f'PRAGMA max_page_count={pages+1}')
  super().runtime(status,run_id)
  if mode=='full' and status=='running':
   signalled=True;os.kill(os.getpid(),signal.SIGTERM)
class Api:
 async def request(self,service,path,**kwargs):
  global once
  if signalled:late.append([service,path])
  if path in ('getUpdates','/updates'):
   events=[]
   if mode=='accept' and service==platform and not once:
    once=True;events=[tg(1,'first'),tg(2,'🌿'*2000)] if service=='tg' else [mx('m1','first'),mx('m2','🌿'*2000)]
   return events if service=='tg' else {'updates':events,'marker':'accept-end'}
  posts.append([service,path]);return {'message_id':10000} if service=='tg' else {'message':{'body':{'mid':'copy'}}}
b.RUN['component']='bridge';exit_code=0;failure=None
previous={s:signal.getsignal(s) for s in (signal.SIGTERM,signal.SIGINT)}
with b.process_lock(state):
 with StopStore(state/'bridge.sqlite3') as store:
  if mode=='full':
   store.accept('tg',[tg(1),tg(2)],3,'-100','99');store.update(1,'uncertain')
  bridge=b.Bridge({'TG_CHAT_ID':'-100','MAX_CHAT_ID':'-200'},Api(),store,{'tg':'99','max':'88'},send_intervals={'tg':0,'max':0})
  async def run():
   loop=asyncio.get_running_loop();original=loop.create_future
   def create_future():
    global once,signalled
    if not once:
     once=True;signalled=True;os.kill(os.getpid(),signal.SIGTERM)
    return original()
   if mode=='wait':loop.create_future=create_future
   try:await b.run_bridge(bridge,poll_interval=0.01,delivery_interval=0.01,heartbeat_interval=0.01)
   finally:loop.create_future=original
  try:asyncio.run(run())
  except sqlite3.OperationalError as exc:
   failure=getattr(exc,'sqlite_errorcode',None);exit_code=b.report_failure(exc)
  result={'inside':inside,'late':late,'posts':posts,'error':failure,'before':before,'after':snapshot(store),
          'cursor':store.get('cursor_'+platform),'inbox':store.db.execute('SELECT count(*) FROM inbox').fetchone()[0],
          'states':[r[0] for r in store.db.execute('SELECT state FROM jobs ORDER BY id')],
          'signals_restored':all(signal.getsignal(s)==h for s,h in previous.items())}
with b.process_lock(state):result['lock_released']=True
with b.Store(state/'bridge.sqlite3',readonly=True) as check:result['integrity']=check.db.execute('PRAGMA integrity_check').fetchone()[0]
print('SHUTDOWN_RESULT '+json.dumps(result));sys.exit(exit_code)
'''

    def probe(self, mode, platform):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run([sys.executable, '-c', self.PROBE, mode, platform, directory],
                                    cwd=Path(__file__).parent, capture_output=True, text=True, timeout=10)
            lines = [line for line in result.stdout.splitlines() if line.startswith('SHUTDOWN_RESULT ')]
            self.assertEqual(len(lines), 1, result.stderr[-1500:])
            data = json.loads(lines[0].partition(' ')[2])
            self.assertTrue(data['lock_released'])
            self.assertTrue(data['signals_restored'])
            self.assertEqual(data['integrity'], 'ok')
            with b.process_lock(Path(directory)):
                pass
            return result, data

    def check_accept(self, platform):
        result, data = self.probe('accept', platform)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(data['inside'])
        self.assertEqual(data['late'], [], 'API operations started after SIGTERM inside the transaction')
        self.assertEqual(data['posts'], [])
        self.assertIn(len(data['states']), (0, 3))
        self.assertTrue(all(state == 'pending' for state in data['states']))
        self.assertEqual(data['inbox'], 2 if data['states'] else 0)
        self.assertEqual(data['cursor'], ('3' if platform == 'tg' else 'accept-end') if data['states'] else None)
        self.assertEqual(data['after']['meta']['runtime_state'], 'stopped')

    def test_sigterm_inside_telegram_accept_is_atomic_and_starts_no_api(self):
        self.check_accept('tg')

    def test_sigterm_inside_max_accept_is_atomic_and_starts_no_api(self):
        self.check_accept('max')

    def test_sigterm_while_arming_stop_waiter_is_not_lost(self):
        result, data = self.probe('wait', 'tg')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(data['late'], [])
        self.assertEqual(data['after']['meta']['runtime_state'], 'stopped')

    def test_full_final_runtime_write_rolls_back_and_reports_error(self):
        result, data = self.probe('full', 'tg')
        self.assertEqual(result.returncode, 1)
        self.assertEqual(data['error'], sqlite3.SQLITE_FULL)
        self.assertEqual(data['before'], data['after'])
        self.assertEqual(data['states'], ['uncertain', 'pending'])
        self.assertEqual(data['after']['meta']['runtime_state'], 'running')
        errors = [json.loads(line) for line in result.stderr.splitlines() if line.startswith('{')]
        self.assertTrue(any(row.get('event') == 'stopped_with_error' and row.get('error_code') == 'storage_error' for row in errors))
        self.assertNotIn('bridge_stopped', result.stdout)


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


class CycleApi:
    """Deterministic API boundary; all bridge/SQLite/retention code stays real."""
    def __init__(self):
        self.queued = {'tg': [], 'max': []}
        self.marker = None
        self.next_id = {'tg': 1000000, 'max': 0}
        self.posts, self.photos = [], []

    def queue(self, platform, events, marker=None):
        self.queued[platform] = list(events)
        if platform == 'max':
            self.marker = marker if marker is not None else events[-1]['message']['body']['mid']

    async def request(self, platform, path, **kwargs):
        await asyncio.sleep(0)
        if path in ('getUpdates', '/updates'):
            events, self.queued[platform] = self.queued[platform], []
            return events if platform == 'tg' else {'updates': events, 'marker': self.marker}
        if path not in ('sendMessage', 'sendPhoto', '/messages'):
            raise AssertionError('Unexpected API operation in cycle fixture')
        self.next_id[platform] += 1
        remote = self.next_id[platform] if platform == 'tg' else f'copy-max-{self.next_id[platform]}'
        body = kwargs.get('body', {})
        parent = (body.get('reply_parameters', {}).get('message_id') if platform == 'tg'
                  else body.get('link', {}).get('mid'))
        self.posts.append({'platform': platform, 'parent': str(parent) if parent is not None else None,
                           'remote_id': str(remote), 'photo': path == 'sendPhoto' or bool(body.get('attachments'))})
        return {'message_id': remote} if platform == 'tg' else {'message': {'body': {'mid': remote}}}

    async def prepare_photo(self, source, reference, destination):
        self.photos.append((source, reference, destination))
        await asyncio.sleep(0)
        return (b'\xff\xd8\xffcycle-photo', 'image/jpeg', 'cycle.jpg') if destination == 'tg' else {'token': reference}


class RetentionCycleTests(unittest.IsolatedAsyncioTestCase):
    CYCLES = 60
    STEP_DAYS = 7
    STATE_ROOT = None  # Smoke overrides this with its owned volume; make test uses /tmp.
    PARTS_PER_CYCLE = 22  # 16 ordinary inputs (two split in two) + four single-part replies.
    JOB_FIELDS = 'id,source,chat,mid,destination,part,parent,state,remote_id,attempts,ready,error,created,updated'

    def metadata(self, store):
        return {'jobs': [tuple(row) for row in store.db.execute(f'SELECT {self.JOB_FIELDS} FROM jobs ORDER BY id')],
                'inbox': [tuple(row) for row in store.db.execute('SELECT * FROM inbox ORDER BY platform,chat,mid')],
                'identity': store.get('identity'), 'cursor_tg': store.get('cursor_tg'), 'cursor_max': store.get('cursor_max')}

    def measure(self, state, store, monitor, policy):
        disk = b.storage.snapshot(state, policy)
        self.assertEqual(disk['state'], 'ok')
        files = disk['files']
        rows = store.db.execute("SELECT count(*),coalesce(sum(length(CAST(payload AS BLOB))),0) FROM jobs WHERE state IN ('sent','skipped') AND payload!=?",
                                (r.CLEARED,)).fetchone()
        metadata = self.metadata(store)
        return {'messages': len(metadata['inbox']), 'parts': len(metadata['jobs']),
                'live_parts': rows[0], 'payload_bytes': rows[1],
                'metadata_bytes': len(json.dumps(metadata, ensure_ascii=False, separators=(',', ':')).encode()),
                'bridge_db': files['bridge.sqlite3'], 'bridge_wal': files['bridge.sqlite3-wal'],
                'bridge_free_pages': r.metrics(store.db)['reusable_pages'],
                'monitor_db': files['monitor.sqlite3'], 'monitor_wal': files['monitor.sqlite3-wal'],
                'monitor_free_pages': r.metrics(monitor.db)['reusable_pages'],
                'alerts_bytes': sum(size for name, size in files.items() if name.startswith('alerts.jsonl')),
                'managed_bytes': disk['managed_bytes'], 'free_bytes': disk['free_bytes']}

    def sweep(self, owner, now):
        tables = ('jobs', 'quarantine', 'audit') if owner.kind == 'bridge' else ('alerts',)
        largest = max(owner.db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] for table in tables)
        # Two bounded sweeps cover a starting cursor in the middle of an existing page.
        for _ in range(2 * ((largest + owner.policy.batch_size - 1) // owner.policy.batch_size + 1)):
            report = owner.run(now=now)
            self.assertEqual((report['state'], report['reason']), ('ok', None))

    async def drain(self, bridge, api, count):
        before = len(api.posts)
        for _ in range(count + 1):
            await bridge.deliver('tg')
            await bridge.deliver('max')
            if len(api.posts) == before + count:
                break
        self.assertEqual(len(api.posts), before + count)
        self.assertEqual(bridge.store.db.execute("SELECT count(*) FROM jobs WHERE state!='sent'").fetchone()[0], 0)

    async def exercise(self, enabled, start):
        with tempfile.TemporaryDirectory(dir=self.STATE_ROOT) as directory:
            state, now = Path(directory), [start]
            policy = b.storage.Settings(free_warn_bytes=2, free_critical_bytes=1,
                                        alerts_max_bytes=2048, alerts_backup_count=2)
            store, monitor = b.Store(state / 'bridge.sqlite3'), b.AlertMonitor(state / 'monitor.sqlite3')
            api = CycleApi()
            bridge = b.Bridge({'TG_CHAT_ID': '-100', 'MAX_CHAT_ID': '-200'}, api, store,
                              {'tg': '99', 'max': '88'}, send_intervals={'tg': 0, 'max': 0})
            owner = r.Maintainer(store.db, 'bridge', r.Settings(content_enabled=int(enabled)))
            history = r.Maintainer(monitor.db, 'monitor', owner.policy)
            sink = b.storage.AlertFile(state / 'alerts.jsonl', policy)
            sequence, first, first_events = {'tg': 0, 'max': 0}, {}, {}
            curve, reused = [], 0

            def event(platform, text, *, photo=False, parent=None):
                sequence[platform] += 1
                mid = sequence[platform]
                value = tg(mid, text=text) if platform == 'tg' else mx(f'src-max-{mid}', text=text)
                msg = value['message']
                if photo:
                    if platform == 'tg':
                        del msg['text']
                        msg.update(caption=text, photo=[{'file_id': f'cycle-{mid}', 'file_size': 16}])
                    else:
                        msg['body']['attachments'] = [{'type': 'image', 'payload': {'url': f'https://i.oneme.ru/cycle-{mid}.jpg'}}]
                if parent is not None:
                    if platform == 'tg':
                        msg['reply_to_message'] = {'message_id': int(parent)}
                    else:
                        msg['link'] = {'type': 'reply', 'message': {'mid': str(parent)}}
                return value

            def observation(issues):
                return {'issues': issues, 'platforms': {'tg': {}, 'max': {}}, 'generated_at': now[0]}

            try:
                with store.db:
                    store.set('identity', json.dumps(['99', '88', '-100', '-200']))
                with patch.object(b.time, 'time', side_effect=lambda: now[0]), redirect_stdout(io.StringIO()):
                    for cycle in range(1, self.CYCLES + 1):
                        before = self.measure(state, store, monitor, policy)
                        batches = {}
                        for platform in ('tg', 'max'):
                            bodies = ['plain ASCII ' * 20, 'identical ' * 70, 'identical ' * 70,
                                      '🌿А' * 1800, 'фото ' + 'я' * 400, 'я' * 2400,
                                      'mixed English русский ' * 80, f'small {cycle}']
                            batches[platform] = [event(platform, text, photo=(i == 4)) for i, text in enumerate(bodies)]
                            api.queue(platform, batches[platform])
                            await bridge.poll(platform)
                        await self.drain(bridge, api, self.PARTS_PER_CYCLE - 4)
                        for platform in ('tg', 'max'):
                            if platform not in first:
                                first_events[platform] = batches[platform][0]
                                first[platform] = dict(store.db.execute('SELECT * FROM jobs WHERE source=? ORDER BY id LIMIT 1', (platform,)).fetchone())
                            identical_mids = [str(value['message']['message_id']) if platform == 'tg'
                                              else value['message']['body']['mid'] for value in batches[platform][1:3]]
                            copies = [store.db.execute('SELECT remote_id FROM jobs WHERE source=? AND mid=?', (platform, mid)).fetchone()[0]
                                      for mid in identical_mids]
                            self.assertNotEqual(*copies)
                        replies = {
                            'tg': [event('tg', 'reply native', parent=first['tg']['mid']),
                                   event('tg', 'reply copy', parent=first['max']['remote_id'])],
                            'max': [event('max', 'reply native', parent=first['max']['mid']),
                                    event('max', 'reply copy', parent=first['tg']['remote_id'])]}
                        count = len(api.posts)
                        for platform in ('tg', 'max'):
                            api.queue(platform, replies[platform])
                            await bridge.poll(platform)
                        await self.drain(bridge, api, 4)
                        self.assertCountEqual([(post['platform'], post['parent']) for post in api.posts[count:]],
                                              [('max', first['tg']['remote_id']), ('max', first['max']['mid']),
                                               ('tg', first['max']['remote_id']), ('tg', first['tg']['mid'])])
                        delivered = self.measure(state, store, monitor, policy)
                        reused += delivered['bridge_free_pages'] < before['bridge_free_pages']
                        metadata = self.metadata(store)
                        issue = {'key': 'cycle:transient', 'code': 'storage_size', 'severity': 'warning'}
                        monitor.observe(observation([issue]))
                        monitor.observe(observation([]))
                        monitor.observe(observation([]))
                        self.assertEqual(monitor.flush(sink), 2)
                        now[0] += self.STEP_DAYS * DAY
                        self.sweep(owner, now[0])
                        self.sweep(history, now[0])
                        sink.maintain()
                        self.assertEqual(self.metadata(store), metadata)
                        cleaned = self.measure(state, store, monitor, policy)
                        expected_live = self.PARTS_PER_CYCLE * (min(cycle, 4) if enabled else cycle)
                        self.assertEqual(cleaned['live_parts'], expected_live)
                        self.assertEqual(cleaned['messages'], cycle * 20)
                        self.assertEqual(cleaned['parts'], cycle * self.PARTS_PER_CYCLE)
                        self.assertLessEqual(cleaned['alerts_bytes'], policy.alerts_max_bytes * 3)
                        self.assertEqual(monitor.db.execute('SELECT count(*) FROM alerts WHERE delivered=0').fetchone()[0], 0)
                        self.assertLessEqual(monitor.db.execute('SELECT count(*) FROM alerts').fetchone()[0], 26)
                        for platform in ('tg', 'max'):
                            payload = store.db.execute('SELECT payload FROM jobs WHERE id=?', (first[platform]['id'],)).fetchone()[0]
                            self.assertEqual(payload == r.CLEARED, bool(enabled and cycle >= 5))
                            api.queue(platform, [first_events[platform], *replies[platform]], marker=store.get('cursor_max'))
                            await bridge.poll(platform)
                        old_posts = len(api.posts)
                        await bridge.deliver('tg')
                        await bridge.deliver('max')
                        self.assertEqual(len(api.posts), old_posts)
                        self.assertEqual(self.metadata(store), metadata)
                        self.assertEqual(len(api.posts), cycle * self.PARTS_PER_CYCLE)
                        curve.append({'cycle': cycle, 'arrival': before, 'delivered': delivered, 'cleaned': cleaned})

                    # Keep real unresolved queue states and an undelivered outbox across further sweeps.
                    for platform in ('tg', 'max'):
                        values = [event(platform, 'protected cycle fixture') for _ in range(4)]
                        api.queue(platform, values)
                        await bridge.poll(platform)
                        with store.db:
                            for value, status in zip(values, ('pending', 'sending', 'failed', 'uncertain')):
                                mid = str(value['message']['message_id']) if platform == 'tg' else value['message']['body']['mid']
                                store.db.execute('UPDATE jobs SET state=?,created=?,updated=?,ready=? WHERE source=? AND mid=?',
                                                 (status, now[0] - 101 * DAY, now[0] - 100 * DAY, now[0] + DAY, platform, mid))
                    protected = [tuple(row) for row in store.db.execute("SELECT * FROM jobs WHERE state!='sent' ORDER BY id")]
                    monitor.observe(observation([{'key': 'cycle:open', 'code': 'storage_free'}]))
                    monitor.flush(sink)
                    with monitor.db:
                        monitor.alert('problem', now[0], {'key': 'cycle:undelivered', 'code': 'storage_free'})
                    protected_alerts = [tuple(row) for row in monitor.db.execute('SELECT * FROM alerts ORDER BY id DESC LIMIT 2')]
                    metadata = self.metadata(store)
                    now[0] += 200 * DAY
                    self.sweep(owner, now[0])
                    self.sweep(history, now[0])
                    sink.maintain()
                    self.assertEqual([tuple(row) for row in store.db.execute("SELECT * FROM jobs WHERE state!='sent' ORDER BY id")], protected)
                    self.assertEqual([tuple(row) for row in monitor.db.execute('SELECT * FROM alerts ORDER BY id DESC LIMIT 2')], protected_alerts)
                    self.assertEqual(monitor.db.execute('SELECT count(*) FROM alerts').fetchone()[0], 2)
                    self.assertEqual(self.metadata(store), metadata)
                    final = self.measure(state, store, monitor, policy)
                    self.assertEqual(final['live_parts'], 0 if enabled else self.CYCLES * self.PARTS_PER_CYCLE)
                    self.assertEqual(len(api.posts), self.CYCLES * self.PARTS_PER_CYCLE)
                    self.assertEqual(len(api.photos), self.CYCLES * 2)
                    self.assertEqual(len({(post['platform'], post['remote_id']) for post in api.posts}), len(api.posts))
                    if enabled:
                        self.assertGreater(reused, 0)
                    peak_wal = {key: max(point[stage][key] for point in curve for stage in ('arrival', 'delivered', 'cleaned'))
                                for key in ('bridge_wal', 'monitor_wal')}
                    # Scenario bound (small batches, default automatic checkpoints), not a production quota.
                    self.assertLessEqual(max(peak_wal.values()), 8 * 1024**2)
                    store.db.close()
                    monitor.db.close()
                    with (patch.object(b, 'ENV', state / 'absent.env'),
                          patch.dict(os.environ, {'RETENTION_CONTENT_ENABLED': str(int(enabled)),
                                                'STORAGE_FREE_WARN_BYTES': '2', 'STORAGE_FREE_CRITICAL_BYTES': '1'})):
                        self.assertEqual(b.main(['maintenance', '--apply', '--compact', '--state-dir', str(state)]), 0)
                    with b.Store(state / 'bridge.sqlite3') as reopened:
                        self.assertEqual(self.metadata(reopened), metadata)
                        self.assertEqual(reopened.db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
                    with sqlite3.connect(state / 'monitor.sqlite3') as reopened:
                        self.assertEqual(reopened.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
                    files = b.storage.snapshot(state, policy)['files']
                    return {'enabled': enabled, 'storage': 'stand-volume' if self.STATE_ROOT else 'temporary-directory',
                            'cycles': self.CYCLES, 'delivered_messages': self.CYCLES * 20,
                            'publications': len(api.posts), 'protected_jobs': len(protected), 'reused_cycles': reused,
                            'peak_wal': peak_wal, 'final': final,
                            'compacted_bytes': sum(files.values()), 'curve': curve}
            finally:
                store.db.close()
                monitor.db.close()

    async def test_many_cycles_preserve_delivery_and_reuse_storage(self):
        start = 1800000000.0  # Logical time only: no real waiting for 30/90 days.
        cleaned = await self.exercise(True, start)
        control = await self.exercise(False, start)
        self.assertEqual(cleaned['final']['metadata_bytes'], control['final']['metadata_bytes'])
        self.assertEqual(cleaned['final']['parts'], control['final']['parts'])
        self.assertEqual(cleaned['final']['messages'], control['final']['messages'])
        self.assertEqual(cleaned['final']['payload_bytes'], 0)
        self.assertGreater(control['final']['payload_bytes'], 0)
        self.assertLess(cleaned['compacted_bytes'], control['compacted_bytes'])
        report = {}
        for name, result in (('cleaned', cleaned), ('control', control)):
            report[name] = {key: value for key, value in result.items() if key != 'curve'}
            report[name]['samples'] = [point for point in result['curve'] if point['cycle'] in (1, 30, 60)]
        print('D5 cycles measurements: ' + json.dumps(report, sort_keys=True))


if __name__ == '__main__':
    unittest.main()
