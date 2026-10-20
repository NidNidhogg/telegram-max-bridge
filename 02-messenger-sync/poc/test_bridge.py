import importlib.util
from contextlib import redirect_stdout
import io
import json
import sqlite3
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

spec = importlib.util.spec_from_file_location('bridge', Path(__file__).with_name('bridge.py'))
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)


def tg(mid=1, text='Привет', author=10, chat=-100):
    return {'update_id': mid, 'message': {'message_id': mid, 'chat': {'id': chat},
            'from': {'id': author, 'first_name': 'Анна'}, 'text': text}}


def mx(mid='m1', text='Привет', author=20, chat=-200):
    return {'update_type': 'message_created', 'message': {'recipient': {'chat_id': chat},
            'sender': {'user_id': author, 'first_name': 'Иван'}, 'body': {'mid': mid, 'text': text}}}


class FakeApi:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def request(self, service, path, **kwargs):
        self.calls.append((service, path, kwargs))
        if self.error:
            raise self.error
        return {'message': {'body': {'mid': 'copy1'}}} if service == 'max' else {'message_id': 999}

    def prepare_photo(self, *args):
        if self.error:
            raise self.error
        return {'token': 'test'}


class BridgeTests(unittest.TestCase):
    def test_telegram_photo_with_generic_mime_is_accepted(self):
        api = b.Api.__new__(b.Api)
        api.cfg = {'TG_BOT_TOKEN': 'test'}
        api.request = MagicMock(return_value={'file_path': 'photos/test.jpg'})
        api.opener = MagicMock()
        response = api.opener.open.return_value.__enter__.return_value
        response.read.return_value = b'\xff\xd8\xfftest-photo'
        response.headers.get_content_type.return_value = 'application/octet-stream'
        self.assertEqual(api.download('tg', 'test')[1], 'image/jpeg')

    def test_image_mime_does_not_allow_non_image_content(self):
        api = b.Api.__new__(b.Api)
        api.cfg = {'TG_BOT_TOKEN': 'test'}
        api.request = MagicMock(return_value={'file_path': 'photos/test.jpg'})
        api.opener = MagicMock()
        response = api.opener.open.return_value.__enter__.return_value
        response.read.return_value = b'<html>error</html>'
        response.headers.get_content_type.return_value = 'image/jpeg'
        with self.assertRaises(b.BridgeError):
            api.download('tg', 'test')

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = b.Store(Path(self.tmp.name) / 'state.db')
        self.cfg = {'TG_CHAT_ID': '-100', 'MAX_CHAT_ID': '-200'}

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def bridge(self, api):
        return b.Bridge(self.cfg, api, self.store, {'tg': '99', 'max': '88'})

    def jobs(self):
        return self.store.db.execute('SELECT * FROM jobs ORDER BY id').fetchall()

    def test_deduplicates_redelivered_event_and_persists_cursor(self):
        for _ in range(2):
            self.store.accept('tg', [tg()], 2, '-100', '99')
        self.assertEqual(len(self.jobs()), 1)
        self.assertEqual(self.store.get('cursor_tg'), '2')

    def test_identical_human_messages_are_distinct(self):
        self.store.accept('tg', [tg(1), tg(2, author=11)], 3, '-100', '99')
        self.assertEqual(len(self.jobs()), 2)

    def test_own_bot_echo_and_unconfigured_chat_ignored(self):
        self.store.accept('tg', [tg(author=99), tg(2, chat=-333)], 3, '-100', '99')
        self.store.accept('max', [mx(author=88), mx('m2', chat=-333)], 3, '-200', '88')
        self.assertEqual(len(self.jobs()), 0)

    def test_other_bot_message_is_not_filtered(self):
        event = tg()
        event['message']['from']['is_bot'] = True
        self.store.accept('tg', [event], 2, '-100', '99')
        self.assertEqual(len(self.jobs()), 1)

    def test_reply_to_copy_maps_back_to_original(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'sent', remote_id='copy-in-max')
        event = mx()
        event['message']['link'] = {'type': 'reply', 'message': {'mid': 'copy-in-max', 'text': 'Привет'}}
        self.store.accept('max', [event], 2, '-200', '88')
        self.assertEqual(self.store.parent_id(self.jobs()[1]), '1')

    def test_reply_to_native_message_maps_to_mirrored_copy(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'sent', remote_id='copy-in-max')
        event = tg(2)
        event['message']['reply_to_message'] = tg()['message']
        self.store.accept('tg', [event], 3, '-100', '99')
        self.assertEqual(self.store.parent_id(self.jobs()[1]), 'copy-in-max')

    def test_photo_caption_and_largest_version(self):
        event = tg(text='')
        event['message']['caption'] = 'Подпись'
        event['message']['photo'] = [{'file_id': 'small', 'file_size': 10}, {'file_id': 'large', 'file_size': 100}]
        message = b.normalize('tg', event, '-100', '99')
        part = b.units(message)[0]
        self.assertEqual(part['reference'], 'large')
        self.assertIn('Подпись', part['text'])

    def test_max_multiple_images(self):
        event = mx()
        event['message']['body']['attachments'] = [
            {'type': 'image', 'payload': {'url': 'https://i.oneme.ru/1'}},
            {'type': 'image', 'payload': {'url': 'https://i.oneme.ru/2'}}]
        parts = b.units(b.normalize('max', event, '-200', '88'))
        self.assertEqual([p['kind'] for p in parts], ['photo', 'photo'])

    def test_text_split_is_utf16_safe(self):
        text = '😀' * 4000
        parts = b.split_text(text)
        self.assertEqual(''.join(parts), text)
        self.assertTrue(all(len(p.encode('utf-16-le')) // 2 <= 3500 for p in parts))

    def test_bad_image_is_quarantined_without_blocking_good_events(self):
        event = mx()
        event['message']['body']['attachments'] = [{'type': 'image', 'payload': {}}]
        for _ in range(2):
            self.store.accept('max', [mx('first'), event, mx('last')], 3, '-200', '88')
        self.assertEqual([job['mid'] for job in self.jobs()], ['first', 'last'])
        self.assertEqual(self.store.get('cursor_max'), '3')
        rows = self.store.db.execute('SELECT * FROM quarantine').fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(rows[0]['payload']), event)
        self.assertEqual(rows[0]['state'], 'open')

    def test_malformed_selected_fields_are_isolated_but_foreign_chat_is_ignored(self):
        bad = [mx('bad-text', text=[]), mx('bad-body'), mx('bad-attachment')]
        bad[1]['message']['body'] = ['invalid']
        bad[2]['message']['body']['attachments'] = ['invalid']
        foreign = mx('foreign', chat=-999)
        foreign['message']['body'] = ['private']
        self.store.accept('max', [*bad, foreign, mx('good')], 4, '-200', '88')
        self.assertEqual(len(self.jobs()), 1)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM quarantine').fetchone()[0], 3)

    def test_database_error_rolls_back_jobs_quarantine_and_cursor(self):
        event = mx('bad', text=[])
        self.store.db.execute("""CREATE TRIGGER fail_cursor BEFORE INSERT ON meta
                               BEGIN SELECT RAISE(ABORT, 'injected'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.accept('max', [mx('good'), event], 3, '-200', '88')
        self.assertEqual(len(self.jobs()), 0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM quarantine').fetchone()[0], 0)
        self.assertIsNone(self.store.get('cursor_max'))

    def test_programming_error_is_not_silently_quarantined(self):
        with patch.object(b, 'normalize', side_effect=RuntimeError('bug')):
            with self.assertRaises(RuntimeError):
                self.store.accept('tg', [tg()], 2, '-100', '99')
        self.assertIsNone(self.store.get('cursor_tg'))

    def test_quarantine_survives_restart_and_can_be_reprocessed_once(self):
        event = mx('bad', text=[])
        self.store.accept('max', [event], 3, '-200', '88')
        self.store.db.close()
        self.store = b.Store(Path(self.tmp.name) / 'state.db')
        self.assertEqual(self.store.retry_quarantine(1, '-200', '88'), 'open')
        repaired = b.normalize('max', mx('bad'), '-200', '88')
        with patch.object(b, 'normalize', return_value=repaired):
            self.assertEqual(self.store.retry_quarantine(1, '-200', '88'), 'resolved')
            self.store.retry_quarantine(1, '-200', '88')
        self.assertEqual(len(self.jobs()), 1)
        self.assertEqual(self.store.get('cursor_max'), '3')

    def test_legacy_database_is_backed_up_and_migrated_without_losing_jobs(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'sent', remote_id='old-copy')
        self.store.db.execute('DROP TABLE quarantine')
        self.store.db.execute('PRAGMA user_version=0')
        self.store.db.close()
        self.store = b.Store(Path(self.tmp.name) / 'state.db')
        self.assertEqual(self.jobs()[0]['remote_id'], 'old-copy')
        self.assertEqual(self.store.get('cursor_tg'), '2')
        self.assertEqual(self.store.db.execute('PRAGMA user_version').fetchone()[0], b.SCHEMA_VERSION)
        self.assertEqual(len(list(Path(self.tmp.name).glob('state.db.v0-*.bak'))), 1)

    def test_failed_retry_is_idempotent_and_unblocks_delivery(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'failed', 'rejected')
        self.store.manage_job(1, 'retry', 'fixed')
        self.store.manage_job(1, 'retry', 'fixed')
        self.bridge(FakeApi()).deliver('max')
        self.assertEqual(self.jobs()[0]['state'], 'sent')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM audit').fetchone()[0], 1)

    def test_uncertain_retry_requires_explicit_duplicate_risk(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'uncertain')
        with self.assertRaises(b.BridgeError):
            self.store.manage_job(1, 'retry', 'reviewed')
        self.assertEqual(self.jobs()[0]['state'], 'uncertain')
        self.store.manage_job(1, 'retry', 'reviewed', accept_duplicate_risk=True)
        self.assertEqual(self.jobs()[0]['state'], 'pending')

    def test_manual_confirm_is_idempotent_and_restores_reply_mapping(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'uncertain')
        for _ in range(2):
            self.store.manage_job(1, 'confirm', 'found copy', remote_id='found-copy')
        event = mx()
        event['message']['link'] = {'type': 'reply', 'message': {'mid': 'found-copy'}}
        self.store.accept('max', [event], 3, '-200', '88')
        self.assertEqual(self.store.parent_id(self.jobs()[1]), '1')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM audit').fetchone()[0], 1)

    def test_confirm_rejects_duplicate_or_invalid_remote_id(self):
        self.store.accept('max', [mx('first'), mx('second')], 3, '-200', '88')
        self.store.update(1, 'sent', remote_id='123')
        self.store.update(2, 'uncertain')
        for remote_id in ('123', '00123', '-1', 'not-a-number', 'https://secret.test/x'):
            with self.subTest(remote_id=remote_id), self.assertRaises(b.BridgeError):
                self.store.manage_job(2, 'confirm', 'reviewed', remote_id=remote_id)
        self.assertEqual(self.jobs()[1]['state'], 'uncertain')

    def test_skip_remaining_preserves_sent_prefix_and_other_messages(self):
        self.store.accept('tg', [tg(1, text='x' * 9000), tg(2)], 3, '-100', '99')
        self.store.update(1, 'sent', remote_id='prefix')
        self.store.update(2, 'failed')
        for _ in range(2):
            self.store.manage_job(2, 'skip', 'approved omission', remaining=True)
        self.assertEqual([j['state'] for j in self.jobs()], ['sent', 'skipped', 'skipped', 'pending'])
        self.bridge(FakeApi()).deliver('max')
        self.assertEqual(self.jobs()[3]['state'], 'sent')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM audit').fetchone()[0], 2)

    def test_active_or_delivered_job_cannot_be_overwritten(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'sending')
        for action in ('retry', 'skip', 'confirm'):
            with self.subTest(action=action), self.assertRaises(b.BridgeError):
                self.store.manage_job(1, action, 'reviewed', remote_id='copy')
        self.store.update(1, 'sent', remote_id='original-copy')
        with self.assertRaises(b.BridgeError):
            self.store.manage_job(1, 'confirm', 'reviewed', remote_id='different-copy')

    def test_audit_write_failure_rolls_back_operator_action(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'failed')
        self.store.db.execute("""CREATE TRIGGER fail_audit BEFORE INSERT ON audit
                               BEGIN SELECT RAISE(ABORT, 'injected'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.manage_job(1, 'retry', 'fixed')
        self.assertEqual(self.jobs()[0]['state'], 'failed')

    def test_cli_maintenance_is_offline_and_never_displays_payload(self):
        self.store.accept('tg', [tg(text='PRIVATE_MESSAGE')], 2, '-100', '99')
        self.store.update(1, 'uncertain')
        self.store.db.close()
        path = Path(self.tmp.name) / 'bridge.sqlite3'
        (Path(self.tmp.name) / 'state.db').rename(path)
        self.store = b.Store(path)
        output = io.StringIO()
        with patch.object(b, 'Api', side_effect=AssertionError('network forbidden')), redirect_stdout(output):
            b.main(['queue', 'show', '1', '--state-dir', self.tmp.name])
            b.main(['queue', 'confirm', '1', '--remote-id', 'found', '--reason', 'reviewed', '--state-dir', self.tmp.name])
        self.assertNotIn('PRIVATE_MESSAGE', output.getvalue())
        self.assertEqual(self.jobs()[0]['state'], 'sent')

    def test_cli_mutation_is_rejected_while_bridge_holds_process_lock(self):
        # Use the CLI's filename so it reaches the lock rather than a missing-DB error.
        path = Path(self.tmp.name) / 'bridge.sqlite3'
        other = b.Store(path)
        other.db.close()
        with b.process_lock(Path(self.tmp.name)):
            with self.assertRaisesRegex(b.BridgeError, 'Другой процесс'):
                b.main(['queue', 'skip', '1', '--reason', 'reviewed', '--state-dir', self.tmp.name])

    def test_readonly_legacy_inspection_does_not_migrate_database(self):
        self.store.db.execute('PRAGMA user_version=0')
        self.store.db.commit()
        reader = b.Store(Path(self.tmp.name) / 'state.db', readonly=True)
        try:
            self.assertEqual(reader.db.execute('PRAGMA user_version').fetchone()[0], 0)
            self.assertEqual(b.queue_view(reader, 'queue', 'list'), [])
        finally:
            reader.db.close()
        self.assertEqual(list(Path(self.tmp.name).glob('*.bak')), [])

    def test_cli_quarantine_retry_uses_stored_identity(self):
        path = Path(self.tmp.name) / 'bridge.sqlite3'
        other = b.Store(path)
        other.accept('max', [mx(text=[])], 3, '-200', '88')
        with other.db:
            other.set('identity', json.dumps(['99', '88', '-100', '-200']))
        other.db.close()
        with redirect_stdout(io.StringIO()), patch.object(b, 'Api', side_effect=AssertionError('network forbidden')):
            b.main(['quarantine', 'retry', '1', '--reason', 'handler reviewed', '--state-dir', self.tmp.name])
        other = b.Store(path)
        try:
            self.assertEqual(other.db.execute('SELECT attempts FROM quarantine').fetchone()[0], 1)
            self.assertEqual(other.db.execute('SELECT scope FROM audit').fetchone()[0], 'quarantine')
        finally:
            other.db.close()

    def test_unknown_media_hosts_rejected(self):
        for url in ('http://i.oneme.ru/a', 'https://localhost/a', 'https://oneme.ru.evil.test/a',
                    'https://127.0.0.1/a', 'https://i.oneme.ru:8080/a'):
            with self.assertRaises(b.BridgeError):
                b.check_media_url(url)
        b.check_media_url('https://i.oneme.ru/image')

    def healthy_telemetry(self, now=1000):
        with self.store.db:
            self.store.set('runtime_state', 'running')
            self.store.set('run_started', now - 500)
            self.store.set('heartbeat', now)
            for platform in ('tg', 'max'):
                self.store.set('last_poll_success_' + platform, now)
                self.store.set('last_poll_error_' + platform, '')

    def test_empty_queue_is_healthy_after_successful_empty_polls(self):
        self.healthy_telemetry()
        snapshot = b.health_snapshot(self.store, now=1000, running=True)
        self.assertTrue(snapshot['ok'])
        self.assertEqual(snapshot['platforms']['tg']['delivery']['counts'], {})
        self.assertIsNone(snapshot['platforms']['max']['delivery']['last_success_at'])

    def test_poll_failure_and_recovery_are_persisted_per_platform(self):
        now = b.time.time()
        self.healthy_telemetry(now)
        with self.assertRaises(b.TransportError):
            self.bridge(FakeApi(b.TransportError('synthetic'))).poll('tg')
        failed = b.health_snapshot(self.store, running=True)
        self.assertEqual([i['key'] for i in failed['issues']], ['poll:tg'])
        api = MagicMock()
        api.request.return_value = []
        self.bridge(api).poll('tg')
        self.assertTrue(b.health_snapshot(self.store, running=True)['ok'])

    def test_fresh_heartbeat_does_not_hide_stopped_process(self):
        self.healthy_telemetry()
        snapshot = b.health_snapshot(self.store, now=1000, running=False)
        self.assertEqual([i['code'] for i in snapshot['issues']], ['process_stopped'])

    def test_stale_heartbeat_and_poll_are_unhealthy_even_with_process_lock(self):
        self.healthy_telemetry()
        snapshot = b.health_snapshot(self.store, now=1300, running=True, stale_after=180)
        self.assertEqual({i['code'] for i in snapshot['issues']}, {'heartbeat_stale', 'poll_stale'})

    def test_queue_age_and_blocking_job_are_visible_without_payload(self):
        self.store.accept('tg', [tg(text='PRIVATE_BODY')], 2, '-100', '99')
        self.store.update(1, 'failed', 'PRIVATE_ERROR')
        with self.store.db:
            self.store.db.execute('UPDATE jobs SET created=600')
        self.healthy_telemetry()
        snapshot = b.health_snapshot(self.store, now=1000, running=True, queue_age=300)
        self.assertEqual({i['code'] for i in snapshot['issues']}, {'delivery_blocked', 'queue_too_old'})
        self.assertEqual(snapshot['platforms']['max']['delivery']['oldest_unresolved_age_seconds'], 400)
        self.assertNotIn('PRIVATE', json.dumps(snapshot))

    def test_unknown_legacy_queue_age_is_not_fabricated(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        with self.store.db:
            self.store.db.execute('UPDATE jobs SET created=NULL,updated=NULL')
        self.healthy_telemetry()
        snapshot = b.health_snapshot(self.store, now=1000, running=True)
        self.assertEqual([i['code'] for i in snapshot['issues']], ['queue_age_unknown'])
        self.assertIsNone(snapshot['platforms']['max']['delivery']['oldest_pending_age_seconds'])

    def test_actual_old_schema_gains_timestamps_and_preserves_backup(self):
        path = Path(self.tmp.name) / 'legacy.sqlite3'
        db = sqlite3.connect(path)
        db.executescript('''CREATE TABLE jobs(
            id INTEGER PRIMARY KEY,source TEXT,chat TEXT,mid TEXT,destination TEXT,part INTEGER,
            payload TEXT,parent TEXT,state TEXT DEFAULT 'pending',remote_id TEXT,
            attempts INTEGER DEFAULT 0,ready REAL DEFAULT 0,error TEXT,UNIQUE(source,chat,mid,part));
            INSERT INTO jobs(source,chat,mid,destination,part,payload) VALUES('tg','-100','1','max',0,'{}');
            PRAGMA user_version=2;''')
        db.close()
        migrated = b.Store(path)
        try:
            row = migrated.db.execute('SELECT * FROM jobs').fetchone()
            self.assertIsNone(row['created'])
            self.assertEqual(row['state'], 'pending')
            backup_path = next(Path(self.tmp.name).glob('legacy.sqlite3.v2-*.bak'))
            backup = sqlite3.connect(backup_path)
            try:
                self.assertNotIn('created', [c[1] for c in backup.execute('PRAGMA table_info(jobs)')])
                self.assertEqual(backup.execute('SELECT count(*) FROM jobs').fetchone()[0], 1)
            finally:
                backup.close()
        finally:
            migrated.db.close()

    def test_delivery_success_timestamp_is_recorded_with_sent_job(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.bridge(FakeApi()).deliver('max')
        self.assertIsNotNone(self.store.get('last_delivery_success_max'))
        self.assertIsNone(self.store.get('last_delivery_success_tg'))

    def test_monitor_emits_problem_once_across_restart_and_one_recovery(self):
        self.healthy_telemetry()
        monitor_path = Path(self.tmp.name) / 'monitor.sqlite3'
        monitor = b.AlertMonitor(monitor_path)
        emitted = []
        self.store.poll_failure('tg', b.TransportError('PRIVATE_TOKEN_OR_URL'))
        failed = b.health_snapshot(self.store, now=1000, running=True)
        monitor.observe(failed)
        self.assertEqual(monitor.flush(emitted.append), 1)
        monitor.db.close()
        monitor = b.AlertMonitor(monitor_path)
        try:
            repeated = b.health_snapshot(self.store, now=1001, running=True)
            monitor.observe(repeated)
            self.assertEqual(monitor.flush(emitted.append), 0)
            self.healthy_telemetry()
            monitor.observe(b.health_snapshot(self.store, now=1000, running=True))
            self.assertEqual(monitor.flush(emitted.append), 1)
            monitor.observe(b.health_snapshot(self.store, now=1001, running=True))
            self.assertEqual(monitor.flush(emitted.append), 0)
            self.assertEqual([a['kind'] for a in emitted], ['problem', 'recovery'])
            self.assertNotIn('PRIVATE', json.dumps(emitted))
        finally:
            monitor.db.close()

    def test_monitor_preserves_alert_when_sink_fails(self):
        path = Path(self.tmp.name) / 'monitor.sqlite3'
        monitor = b.AlertMonitor(path)
        monitor.observe(b.health_snapshot(None, now=1000))
        event_id = monitor.db.execute('SELECT event_id FROM alerts').fetchone()[0]
        with self.assertRaises(OSError):
            monitor.flush(MagicMock(side_effect=OSError('sink unavailable')))
        monitor.db.close()
        monitor = b.AlertMonitor(path)
        try:
            monitor.observe(b.health_snapshot(None, now=1001))
            emitted = []
            self.assertEqual(monitor.flush(emitted.append), 1)
            self.assertEqual(emitted[0]['event_id'], event_id)
        finally:
            monitor.db.close()

    def test_unreadable_state_does_not_falsely_resolve_known_problems(self):
        monitor = b.AlertMonitor(':memory:')
        try:
            self.healthy_telemetry()
            self.store.poll_failure('tg', b.TransportError('synthetic'))
            monitor.observe(b.health_snapshot(self.store, now=1000, running=True))
            emitted = []
            monitor.flush(emitted.append)
            monitor.observe(b.health_snapshot(None, now=1001))
            monitor.flush(emitted.append)
            self.assertEqual([a['kind'] for a in emitted], ['problem', 'problem'])
            self.healthy_telemetry()
            monitor.observe(b.health_snapshot(self.store, now=1002, running=True))
            monitor.flush(emitted.append)
            self.assertEqual([a['kind'] for a in emitted], ['problem', 'problem', 'recovery', 'recovery'])
        finally:
            monitor.db.close()

    def test_missing_state_health_is_machine_readable_and_nonzero(self):
        output = io.StringIO()
        with redirect_stdout(output):
            code = b.main(['health', '--json', '--state-dir', str(Path(self.tmp.name) / 'missing')])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(output.getvalue())['issues'][0]['code'], 'state_missing')
        self.assertFalse((Path(self.tmp.name) / 'missing').exists())

    def test_monitor_once_is_quiet_on_unchanged_state(self):
        output = io.StringIO()
        with redirect_stdout(output):
            b.main(['monitor', '--once', '--state-dir', self.tmp.name])
            b.main(['monitor', '--once', '--state-dir', self.tmp.name])
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['kind'], 'problem')

    def test_corrupt_state_is_reported_without_raw_database_error(self):
        path = Path(self.tmp.name) / 'bridge.sqlite3'
        path.write_text('PRIVATE_DATABASE_CONTENT')
        snapshot = b.read_health(Path(self.tmp.name))
        self.assertEqual(snapshot['issues'][0]['code'], 'state_unavailable')
        self.assertNotIn('PRIVATE', json.dumps(snapshot))

    def test_live_process_lock_is_detected_and_released(self):
        state = Path(self.tmp.name)
        self.assertFalse(b.bridge_running(state))
        with b.process_lock(state):
            self.assertTrue(b.bridge_running(state))
        self.assertFalse(b.bridge_running(state))

    def test_ambiguous_publish_is_not_retried_and_blocks_destination(self):
        self.store.accept('tg', [tg(1), tg(2)], 3, '-100', '99')
        api = FakeApi(b.TransportError('network'))
        bridge = self.bridge(api)
        bridge.deliver('max')
        self.assertEqual(self.jobs()[0]['state'], 'uncertain')
        bridge.deliver('max')
        self.assertEqual(len(api.calls), 1)

    def test_rate_limit_remains_pending(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.bridge(FakeApi(b.ApiError('max', 429, 10))).deliver('max')
        self.assertEqual(self.jobs()[0]['state'], 'pending')
        self.assertGreater(self.jobs()[0]['ready'], 0)

    def test_photo_download_failure_safe_to_retry(self):
        event = tg(text='')
        event['message']['photo'] = [{'file_id': 'image'}]
        self.store.accept('tg', [event], 2, '-100', '99')
        self.bridge(FakeApi(b.TransportError('download failed'))).deliver('max')
        self.assertEqual(self.jobs()[0]['state'], 'pending')

    def test_prepublication_server_failure_safe_to_retry(self):
        event = tg(text='')
        event['message']['photo'] = [{'file_id': 'image'}]
        self.store.accept('tg', [event], 2, '-100', '99')
        self.bridge(FakeApi(b.ApiError('tg', 503))).deliver('max')
        self.assertEqual(self.jobs()[0]['state'], 'pending')

    def test_publication_server_failure_is_uncertain(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.bridge(FakeApi(b.ApiError('max', 503))).deliver('max')
        self.assertEqual(self.jobs()[0]['state'], 'uncertain')

    def test_crash_during_publish_becomes_uncertain_on_restart(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'sending')
        self.store.recover()
        self.assertEqual(self.jobs()[0]['state'], 'uncertain')

    def test_successful_copy_is_recorded(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.bridge(FakeApi()).deliver('max')
        self.assertEqual(self.jobs()[0]['remote_id'], 'copy1')
        self.assertEqual(self.jobs()[0]['state'], 'sent')


if __name__ == '__main__':
    unittest.main()
