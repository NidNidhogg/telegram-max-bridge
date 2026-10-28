import asyncio
import importlib.util
from contextlib import redirect_stdout
import email.utils
import io
import json
import sqlite3
import subprocess
import sys
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

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

    async def request(self, service, path, **kwargs):
        self.calls.append((service, path, kwargs))
        if self.error:
            raise self.error
        return {'message': {'body': {'mid': 'copy1'}}} if service == 'max' else {'message_id': 999}

    async def prepare_photo(self, *args):
        if self.error:
            raise self.error
        return {'token': 'test'}


class BridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_telegram_photo_with_generic_mime_is_accepted(self):
        api = b.Api.__new__(b.Api)
        api.cfg = {'TG_BOT_TOKEN': 'test'}
        api.tg_file_base = 'https://api.telegram.org/file/bottest'
        api.request = AsyncMock(return_value={'file_path': 'photos/test.jpg'})
        api.read = AsyncMock(return_value=(200, b'\xff\xd8\xfftest-photo'))
        self.assertEqual((await api.download('tg', 'test'))[1], 'image/jpeg')

    async def test_image_mime_does_not_allow_non_image_content(self):
        api = b.Api.__new__(b.Api)
        api.cfg = {'TG_BOT_TOKEN': 'test'}
        api.tg_file_base = 'https://api.telegram.org/file/bottest'
        api.request = AsyncMock(return_value={'file_path': 'photos/test.jpg'})
        api.read = AsyncMock(return_value=(200, b'<html>error</html>'))
        with self.assertRaises(b.BridgeError):
            await api.download('tg', 'test')

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

    async def test_deduplicates_redelivered_event_and_persists_cursor(self):
        for _ in range(2):
            self.store.accept('tg', [tg()], 2, '-100', '99')
        self.assertEqual(len(self.jobs()), 1)
        self.assertEqual(self.store.get('cursor_tg'), '2')

    async def test_only_pending_due_head_can_be_claimed_once(self):
        self.store.accept('tg', [tg(1), tg(2)], 3, '-100', '99')
        first, second = self.jobs()
        self.assertFalse(self.store.start_sending(second))
        self.assertTrue(self.store.start_sending(first))
        self.assertFalse(self.store.start_sending(first))
        self.assertFalse(self.store.start_sending(second))
        self.store.update(1, 'sent', remote_id='first')
        self.store.update(2, 'pending', delay=60)
        self.assertFalse(self.store.start_sending(second))

    async def test_identical_human_messages_are_distinct(self):
        self.store.accept('tg', [tg(1), tg(2, author=11)], 3, '-100', '99')
        self.assertEqual(len(self.jobs()), 2)

    async def test_own_bot_echo_and_unconfigured_chat_ignored(self):
        self.store.accept('tg', [tg(author=99), tg(2, chat=-333)], 3, '-100', '99')
        self.store.accept('max', [mx(author=88), mx('m2', chat=-333)], 3, '-200', '88')
        self.assertEqual(len(self.jobs()), 0)

    async def test_other_bot_message_is_not_filtered(self):
        event = tg()
        event['message']['from']['is_bot'] = True
        self.store.accept('tg', [event], 2, '-100', '99')
        self.assertEqual(len(self.jobs()), 1)

    async def test_reply_to_copy_maps_back_to_original(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'sent', remote_id='copy-in-max')
        event = mx()
        event['message']['link'] = {'type': 'reply', 'message': {'mid': 'copy-in-max', 'text': 'Привет'}}
        self.store.accept('max', [event], 2, '-200', '88')
        self.assertEqual(self.store.parent_id(self.jobs()[1]), '1')

    async def test_reply_to_native_message_maps_to_mirrored_copy(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'sent', remote_id='copy-in-max')
        event = tg(2)
        event['message']['reply_to_message'] = tg()['message']
        self.store.accept('tg', [event], 3, '-100', '99')
        self.assertEqual(self.store.parent_id(self.jobs()[1]), 'copy-in-max')

    async def test_photo_caption_and_largest_version(self):
        event = tg(text='')
        event['message']['caption'] = 'Подпись'
        event['message']['photo'] = [{'file_id': 'small', 'file_size': 10}, {'file_id': 'large', 'file_size': 100}]
        message = b.normalize('tg', event, '-100', '99')
        part = b.units(message)[0]
        self.assertEqual(part['reference'], 'large')
        self.assertIn('Подпись', part['text'])

    async def test_max_multiple_images(self):
        event = mx()
        event['message']['body']['attachments'] = [
            {'type': 'image', 'payload': {'url': 'https://i.oneme.ru/1'}},
            {'type': 'image', 'payload': {'url': 'https://i.oneme.ru/2'}}]
        parts = b.units(b.normalize('max', event, '-200', '88'))
        self.assertEqual([p['kind'] for p in parts], ['photo', 'photo'])

    async def test_text_split_is_utf16_safe(self):
        text = '😀' * 4000
        parts = b.split_text(text)
        self.assertEqual(''.join(parts), text)
        self.assertTrue(all(len(p.encode('utf-16-le')) // 2 <= 3500 for p in parts))

    async def test_bad_image_is_quarantined_without_blocking_good_events(self):
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

    async def test_malformed_selected_fields_are_isolated_but_foreign_chat_is_ignored(self):
        bad = [mx('bad-text', text=[]), mx('bad-body'), mx('bad-attachment')]
        bad[1]['message']['body'] = ['invalid']
        bad[2]['message']['body']['attachments'] = ['invalid']
        foreign = mx('foreign', chat=-999)
        foreign['message']['body'] = ['private']
        self.store.accept('max', [*bad, foreign, mx('good')], 4, '-200', '88')
        self.assertEqual(len(self.jobs()), 1)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM quarantine').fetchone()[0], 3)

    async def test_database_error_rolls_back_jobs_quarantine_and_cursor(self):
        event = mx('bad', text=[])
        self.store.db.execute("""CREATE TRIGGER fail_cursor BEFORE INSERT ON meta
                               BEGIN SELECT RAISE(ABORT, 'injected'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.accept('max', [mx('good'), event], 3, '-200', '88')
        self.assertEqual(len(self.jobs()), 0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM quarantine').fetchone()[0], 0)
        self.assertIsNone(self.store.get('cursor_max'))

    async def test_programming_error_is_not_silently_quarantined(self):
        with patch.object(b, 'normalize', side_effect=RuntimeError('bug')):
            with self.assertRaises(RuntimeError):
                self.store.accept('tg', [tg()], 2, '-100', '99')
        self.assertIsNone(self.store.get('cursor_tg'))

    async def test_quarantine_survives_restart_and_can_be_reprocessed_once(self):
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

    async def test_legacy_database_is_backed_up_and_migrated_without_losing_jobs(self):
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

    async def test_failed_retry_is_idempotent_and_unblocks_delivery(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'failed', 'rejected')
        self.store.manage_job(1, 'retry', 'fixed')
        self.store.manage_job(1, 'retry', 'fixed')
        await self.bridge(FakeApi()).deliver('max')
        self.assertEqual(self.jobs()[0]['state'], 'sent')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM audit').fetchone()[0], 1)

    async def test_uncertain_retry_requires_explicit_duplicate_risk(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'uncertain')
        with self.assertRaises(b.BridgeError):
            self.store.manage_job(1, 'retry', 'reviewed')
        self.assertEqual(self.jobs()[0]['state'], 'uncertain')
        self.store.manage_job(1, 'retry', 'reviewed', accept_duplicate_risk=True)
        self.assertEqual(self.jobs()[0]['state'], 'pending')

    async def test_manual_confirm_is_idempotent_and_restores_reply_mapping(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'uncertain')
        for _ in range(2):
            self.store.manage_job(1, 'confirm', 'found copy', remote_id='found-copy')
        event = mx()
        event['message']['link'] = {'type': 'reply', 'message': {'mid': 'found-copy'}}
        self.store.accept('max', [event], 3, '-200', '88')
        self.assertEqual(self.store.parent_id(self.jobs()[1]), '1')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM audit').fetchone()[0], 1)

    async def test_confirm_rejects_duplicate_or_invalid_remote_id(self):
        self.store.accept('max', [mx('first'), mx('second')], 3, '-200', '88')
        self.store.update(1, 'sent', remote_id='123')
        self.store.update(2, 'uncertain')
        for remote_id in ('123', '00123', '-1', 'not-a-number', 'https://secret.test/x'):
            with self.subTest(remote_id=remote_id), self.assertRaises(b.BridgeError):
                self.store.manage_job(2, 'confirm', 'reviewed', remote_id=remote_id)
        self.assertEqual(self.jobs()[1]['state'], 'uncertain')

    async def test_skip_remaining_preserves_sent_prefix_and_other_messages(self):
        self.store.accept('tg', [tg(1, text='x' * 9000), tg(2)], 3, '-100', '99')
        self.store.update(1, 'sent', remote_id='prefix')
        self.store.update(2, 'failed')
        for _ in range(2):
            self.store.manage_job(2, 'skip', 'approved omission', remaining=True)
        self.assertEqual([j['state'] for j in self.jobs()], ['sent', 'skipped', 'skipped', 'pending'])
        await self.bridge(FakeApi()).deliver('max')
        self.assertEqual(self.jobs()[3]['state'], 'sent')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM audit').fetchone()[0], 2)

    async def test_active_or_delivered_job_cannot_be_overwritten(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'sending')
        for action in ('retry', 'skip', 'confirm'):
            with self.subTest(action=action), self.assertRaises(b.BridgeError):
                self.store.manage_job(1, action, 'reviewed', remote_id='copy')
        self.store.update(1, 'sent', remote_id='original-copy')
        with self.assertRaises(b.BridgeError):
            self.store.manage_job(1, 'confirm', 'reviewed', remote_id='different-copy')

    async def test_audit_write_failure_rolls_back_operator_action(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'failed')
        self.store.db.execute("""CREATE TRIGGER fail_audit BEFORE INSERT ON audit
                               BEGIN SELECT RAISE(ABORT, 'injected'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.manage_job(1, 'retry', 'fixed')
        self.assertEqual(self.jobs()[0]['state'], 'failed')

    async def test_cli_maintenance_is_offline_and_never_displays_payload(self):
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

    async def test_cli_mutation_is_rejected_while_bridge_holds_process_lock(self):
        # Use the CLI's filename so it reaches the lock rather than a missing-DB error.
        path = Path(self.tmp.name) / 'bridge.sqlite3'
        other = b.Store(path)
        other.db.close()
        with b.process_lock(Path(self.tmp.name)):
            with self.assertRaisesRegex(b.BridgeError, 'Другой процесс'):
                b.main(['queue', 'skip', '1', '--reason', 'reviewed', '--state-dir', self.tmp.name])

    async def test_readonly_legacy_inspection_does_not_migrate_database(self):
        self.store.db.execute('PRAGMA user_version=0')
        self.store.db.commit()
        reader = b.Store(Path(self.tmp.name) / 'state.db', readonly=True)
        try:
            self.assertEqual(reader.db.execute('PRAGMA user_version').fetchone()[0], 0)
            self.assertEqual(b.queue_view(reader, 'queue', 'list'), [])
        finally:
            reader.db.close()
        self.assertEqual(list(Path(self.tmp.name).glob('*.bak')), [])

    async def test_cli_quarantine_retry_uses_stored_identity(self):
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

    async def test_unknown_media_hosts_rejected(self):
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

    def telemetry(self, workers, run_id='run-now', now=1000):
        self.healthy_telemetry(now)
        with self.store.db:
            self.store.set('run_id', run_id)
            self.store.set('workers', json.dumps({'run_id': run_id, 'workers': workers}))

    def codes(self, snapshot):
        return {(i['code'], i.get('worker') or i.get('platform')) for i in snapshot['issues']}

    async def test_worker_telemetry_reports_stall_and_pause_but_not_planned_waits(self):
        ok = {'phase': 'wait', 'since': 990, 'until': 1001, 'reason': None}
        stalled = {'phase': 'deliver', 'since': 500, 'until': 600, 'reason': None}
        self.telemetry({'poll:tg': ok, 'send:tg': ok, 'send:max': stalled,
                        'poll:max': {'phase': 'paused', 'since': 900, 'until': None, 'reason': 'auth_rejected'}})
        snapshot = b.health_snapshot(self.store, now=1000, running=True, stale_after=180)
        codes = self.codes(snapshot)
        self.assertTrue({('platform_paused', 'max'), ('worker_stalled', 'send:max')} <= codes, codes)
        self.assertFalse({('worker_stalled', 'poll:tg'), ('worker_stalled', 'send:tg')} & codes)
        self.assertEqual(next(i for i in snapshot['issues'] if i['code'] == 'platform_paused')['reason'], 'auth_rejected')
        stall = next(i for i in snapshot['issues'] if i['code'] == 'worker_stalled')
        self.assertEqual((stall['platform'], stall['phase']), ('max', 'deliver'))
        # A planned wait longer than a day (server Retry-After) is not a stall.
        self.telemetry({'poll:tg': {'phase': 'wait', 'since': 990, 'until': 1000 + 2 * 86400, 'reason': None}})
        self.assertNotIn('worker_stalled', {c for c, _ in self.codes(b.health_snapshot(self.store, now=1000, running=True))})
        # Telemetry of a previous run or of a stopped process never describes current workers.
        self.telemetry({'send:max': stalled}, run_id='old-run')
        with self.store.db:
            self.store.set('run_id', 'new-run')
        snapshot = b.health_snapshot(self.store, now=1000, running=True)
        self.assertEqual((snapshot['workers'], 'worker_stalled' in {c for c, _ in self.codes(snapshot)}), ({}, False))
        self.telemetry({'send:max': stalled})
        self.assertEqual(b.health_snapshot(self.store, now=1000, running=False)['workers'], {})
        # A frozen whole process shows heartbeat_stale only.
        codes = {c for c, _ in self.codes(b.health_snapshot(self.store, now=2000, running=True, stale_after=180))}
        self.assertIn('heartbeat_stale', codes)
        self.assertNotIn('worker_stalled', codes)

    async def test_waiting_head_shows_reason_and_full_retry_after_after_reopen(self):
        self.store.accept('tg', [tg(text='PRIVATE_MESSAGE')], 2, '-100', '99')
        self.store.update(1, 'pending', b.error_text('rate_limited', 429, 'max: API error 429'), delay=2 * 86400)
        self.store.db.close()
        self.store = b.Store(Path(self.tmp.name) / 'state.db')
        now = time.time()
        self.healthy_telemetry(now)
        snapshot = b.health_snapshot(self.store, now=now, running=True)
        head = snapshot['platforms']['max']['delivery']['head']
        self.assertEqual((head['id'], head['state'], head['reason'], head['wait']), (1, 'pending', 'rate_limited', 'retry'))
        self.assertGreater(head['retry_at'], now + 2 * 86400 - 60)
        self.assertGreater(head['wait_seconds'], 2 * 86400 - 60)
        self.assertNotIn('PRIVATE', json.dumps(snapshot))
        output = io.StringIO()
        with redirect_stdout(output):
            b.print_health(snapshot)
        self.assertIn('reason=rate_limited wait=retry', output.getvalue())

    async def test_monitor_ignores_age_and_attempt_changes_of_one_problem(self):
        monitor = b.AlertMonitor(':memory:')
        try:
            self.store.accept('tg', [tg()], 2, '-100', '99')
            self.store.update(1, 'failed', b.error_text('api_rejected', 400, 'max: API error 400'))
            emitted = []
            for now in (1000, 1500, 9000):
                self.healthy_telemetry(now)
                with self.store.db:
                    self.store.db.execute('UPDATE jobs SET attempts=attempts+1')
                monitor.observe(b.health_snapshot(self.store, now=now, running=True, queue_age=10 ** 9))
                monitor.flush(emitted.append)
            self.assertEqual([(a['kind'], a['issue']['code']) for a in emitted], [('problem', 'delivery_blocked')])
            self.assertEqual(emitted[0]['issue']['reason'], 'api_rejected')
        finally:
            monitor.db.close()

    async def test_empty_queue_is_healthy_after_successful_empty_polls(self):
        self.healthy_telemetry()
        snapshot = b.health_snapshot(self.store, now=1000, running=True)
        self.assertTrue(snapshot['ok'])
        self.assertEqual(snapshot['platforms']['tg']['delivery']['counts'], {})
        self.assertIsNone(snapshot['platforms']['max']['delivery']['last_success_at'])

    async def test_poll_failure_and_recovery_are_persisted_per_platform(self):
        now = b.time.time()
        self.healthy_telemetry(now)
        with self.assertRaises(b.TransportError):
            await self.bridge(FakeApi(b.TransportError('synthetic'))).poll('tg')
        failed = b.health_snapshot(self.store, running=True)
        self.assertEqual([i['key'] for i in failed['issues']], ['poll:tg'])
        api = MagicMock()
        api.request = AsyncMock(return_value=[])
        await self.bridge(api).poll('tg')
        self.assertTrue(b.health_snapshot(self.store, running=True)['ok'])

    async def test_fresh_heartbeat_does_not_hide_stopped_process(self):
        self.healthy_telemetry()
        snapshot = b.health_snapshot(self.store, now=1000, running=False)
        self.assertEqual([i['code'] for i in snapshot['issues']], ['process_stopped'])

    async def test_stale_heartbeat_and_poll_are_unhealthy_even_with_process_lock(self):
        self.healthy_telemetry()
        snapshot = b.health_snapshot(self.store, now=1300, running=True, stale_after=180)
        self.assertEqual({i['code'] for i in snapshot['issues']}, {'heartbeat_stale', 'poll_stale'})

    async def test_queue_age_and_blocking_job_are_visible_without_payload(self):
        self.store.accept('tg', [tg(text='PRIVATE_BODY')], 2, '-100', '99')
        self.store.update(1, 'failed', 'PRIVATE_ERROR')
        with self.store.db:
            self.store.db.execute('UPDATE jobs SET created=600')
        self.healthy_telemetry()
        snapshot = b.health_snapshot(self.store, now=1000, running=True, queue_age=300)
        self.assertEqual({i['code'] for i in snapshot['issues']}, {'delivery_blocked', 'queue_too_old'})
        self.assertEqual(snapshot['platforms']['max']['delivery']['oldest_unresolved_age_seconds'], 400)
        self.assertNotIn('PRIVATE', json.dumps(snapshot))

    async def test_unknown_legacy_queue_age_is_not_fabricated(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        with self.store.db:
            self.store.db.execute('UPDATE jobs SET created=NULL,updated=NULL')
        self.healthy_telemetry()
        snapshot = b.health_snapshot(self.store, now=1000, running=True)
        self.assertEqual([i['code'] for i in snapshot['issues']], ['queue_age_unknown'])
        self.assertIsNone(snapshot['platforms']['max']['delivery']['oldest_pending_age_seconds'])

    async def test_actual_old_schema_gains_timestamps_and_preserves_backup(self):
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

    async def test_delivery_success_timestamp_is_recorded_with_sent_job(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        await self.bridge(FakeApi()).deliver('max')
        self.assertIsNotNone(self.store.get('last_delivery_success_max'))
        self.assertIsNone(self.store.get('last_delivery_success_tg'))

    async def test_monitor_emits_problem_once_across_restart_and_one_recovery(self):
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
            # Recovery is confirmed by a second clean observation, also across a monitor restart.
            monitor.observe(b.health_snapshot(self.store, now=1000, running=True))
            self.assertEqual(monitor.flush(emitted.append), 0)
            monitor.db.close()
            monitor = b.AlertMonitor(monitor_path)
            monitor.observe(b.health_snapshot(self.store, now=1001, running=True))
            self.assertEqual(monitor.flush(emitted.append), 1)
            monitor.observe(b.health_snapshot(self.store, now=1002, running=True))
            self.assertEqual(monitor.flush(emitted.append), 0)
            self.assertEqual([a['kind'] for a in emitted], ['problem', 'recovery'])
            self.assertEqual({a['event'] for a in emitted}, {'alert_problem', 'alert_recovery'})
            self.assertTrue(all(a['component'] == 'monitor' and a['timestamp'].endswith('Z') for a in emitted))
            self.assertNotIn('PRIVATE', json.dumps(emitted))
        finally:
            monitor.db.close()

    async def test_monitor_preserves_alert_when_sink_fails(self):
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

    async def test_unreadable_state_does_not_falsely_resolve_known_problems(self):
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
            monitor.observe(b.health_snapshot(self.store, now=1003, running=True))
            monitor.flush(emitted.append)
            self.assertEqual([a['kind'] for a in emitted], ['problem', 'problem', 'recovery', 'recovery'])
        finally:
            monitor.db.close()

    async def test_missing_state_health_is_machine_readable_and_nonzero(self):
        output = io.StringIO()
        with redirect_stdout(output):
            code = b.main(['health', '--json', '--state-dir', str(Path(self.tmp.name) / 'missing')])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(output.getvalue())['issues'][0]['code'], 'state_missing')
        self.assertFalse((Path(self.tmp.name) / 'missing').exists())

    async def test_monitor_once_is_quiet_on_unchanged_state(self):
        output = io.StringIO()
        with redirect_stdout(output):
            b.main(['monitor', '--once', '--state-dir', self.tmp.name])
            b.main(['monitor', '--once', '--state-dir', self.tmp.name])
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['kind'], 'problem')

    async def test_corrupt_state_is_reported_without_raw_database_error(self):
        path = Path(self.tmp.name) / 'bridge.sqlite3'
        path.write_text('PRIVATE_DATABASE_CONTENT')
        snapshot = b.read_health(Path(self.tmp.name))
        self.assertEqual(snapshot['issues'][0]['code'], 'state_unavailable')
        self.assertNotIn('PRIVATE', json.dumps(snapshot))

    async def test_live_process_lock_is_detected_and_released(self):
        state = Path(self.tmp.name)
        self.assertFalse(b.bridge_running(state))
        with b.process_lock(state):
            self.assertTrue(b.bridge_running(state))
        self.assertFalse(b.bridge_running(state))

    async def test_ambiguous_publish_is_not_retried_and_blocks_destination(self):
        self.store.accept('tg', [tg(1), tg(2)], 3, '-100', '99')
        api = FakeApi(b.TransportError('network'))
        bridge = self.bridge(api)
        await bridge.deliver('max')
        self.assertEqual(self.jobs()[0]['state'], 'uncertain')
        await bridge.deliver('max')
        self.assertEqual(len(api.calls), 1)

    async def test_rate_limit_remains_pending(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        await self.bridge(FakeApi(b.ApiError('max', 429, 10))).deliver('max')
        self.assertEqual(self.jobs()[0]['state'], 'pending')
        self.assertGreater(self.jobs()[0]['ready'], 0)

    async def test_photo_download_failure_safe_to_retry(self):
        event = tg(text='')
        event['message']['photo'] = [{'file_id': 'image'}]
        self.store.accept('tg', [event], 2, '-100', '99')
        await self.bridge(FakeApi(b.TransportError('download failed'))).deliver('max')
        self.assertEqual(self.jobs()[0]['state'], 'pending')

    async def test_prepublication_server_failure_safe_to_retry(self):
        event = tg(text='')
        event['message']['photo'] = [{'file_id': 'image'}]
        self.store.accept('tg', [event], 2, '-100', '99')
        await self.bridge(FakeApi(b.ApiError('tg', 503))).deliver('max')
        self.assertEqual(self.jobs()[0]['state'], 'pending')

    async def test_publication_server_failure_is_uncertain(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        await self.bridge(FakeApi(b.ApiError('max', 503))).deliver('max')
        self.assertEqual(self.jobs()[0]['state'], 'uncertain')

    async def test_crash_during_publish_becomes_uncertain_on_restart(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        self.store.update(1, 'sending')
        self.store.recover()
        self.assertEqual(self.jobs()[0]['state'], 'uncertain')

    async def test_successful_copy_is_recorded(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        await self.bridge(FakeApi()).deliver('max')
        self.assertEqual(self.jobs()[0]['remote_id'], 'copy1')
        self.assertEqual(self.jobs()[0]['state'], 'sent')



class ScriptedApi(FakeApi):
    """Each publication/preparation call consumes the next scripted outcome (None means success)."""
    def __init__(self, publish=(), prepare=()):
        super().__init__()
        self.publish, self.prepare, self.prepared = list(publish), list(prepare), 0

    async def request(self, service, path, **kwargs):
        self.calls.append((service, path, kwargs))
        outcome = self.publish.pop(0) if self.publish else None
        if outcome:
            raise outcome
        return {'message': {'body': {'mid': f'copy{len(self.calls)}'}}} if service == 'max' else {'message_id': len(self.calls)}

    async def prepare_photo(self, *args):
        self.prepared += 1
        outcome = self.prepare.pop(0) if self.prepare else None
        if outcome:
            raise outcome
        return {'token': f'upload{self.prepared}'}


class ResilienceTests(unittest.IsolatedAsyncioTestCase):
    """D2: retry timing, classification of failures and reuse of prepared MAX attachments."""
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = b.Store(Path(self.tmp.name) / 'state.db')
        self.cfg = {'TG_CHAT_ID': '-100', 'MAX_CHAT_ID': '-200'}

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def bridge(self, api, rand=0.0):
        bridge = b.Bridge(self.cfg, api, self.store, {'tg': '99', 'max': '88'}, send_intervals={'tg': 0, 'max': 0})
        bridge.random = lambda: rand
        return bridge

    def job(self, jid=1):
        return self.store.db.execute('SELECT * FROM jobs WHERE id=?', (jid,)).fetchone()

    def make_ready(self):
        with self.store.db:
            self.store.db.execute('UPDATE jobs SET ready=0')

    def photo_job(self):
        event = tg(text='подпись')
        event['message']['photo'] = [{'file_id': 'image'}]
        self.store.accept('tg', [event], 2, '-100', '99')

    def test_retry_after_accepts_seconds_and_http_date(self):
        now = 1_000_000_000.0
        date = email.utils.formatdate(now + 90, usegmt=True)
        cases = {'120': 120, ' 7 ': 7, 30: 30, 2.5: 2.5, date: 90,
                 email.utils.formatdate(now - 90, usegmt=True): 0}
        for value, expected in cases.items():
            self.assertAlmostEqual(b.retry_after_seconds(value, now=now), expected, places=3, msg=repr(value))

    def test_retry_after_rejects_invalid_values_without_raising(self):
        invalid = [None, '', '  ', '-5', '1.5', '+3', 'soon', '٣', True, False, -1, 0, float('nan'),
                   float('inf'), float('-inf'), {'s': 1}, [5], 'Wed, 99 Foo 2015 07:28:00 GMT',
                   'Wed, 21 Oct 2015 07:28:00 -0000', 'Wed, 21 Oct 99999 07:28:00 GMT']
        for value in invalid:
            self.assertEqual(b.retry_after_seconds(value, now=0), 0, repr(value))
        # Beyond float range: unusable hint, the caller falls back to its own backoff.
        for huge in (10 ** 400, '9' * 400, '9' * 5000, 1e301):
            self.assertEqual(b.retry_after_seconds(huge), 0, repr(huge)[:20])
        self.assertEqual(b.retry_after_seconds('0' * 5000 + '7'), 7)
        error = b.Api.error('max', 429, {'code': '429'}, '9' * 5000)
        self.assertEqual((error.code, error.retry_after, error.status), ('429', 0, 429))

    def test_server_delays_longer_than_a_day_are_not_shortened(self):
        now, two_days = 1_000_000_000.0, 2 * 86400
        self.assertEqual(b.retry_after_seconds(str(two_days), now=now), two_days)
        date = email.utils.formatdate(now + two_days, usegmt=True)
        self.assertAlmostEqual(b.retry_after_seconds(date, now=now), two_days, places=3)
        self.assertEqual(b.Api.error('tg', 429, {'parameters': {'retry_after': two_days}}).retry_after, two_days)
        self.assertEqual(b.retry_after_seconds(1e300), 1e300)

    async def test_server_wait_longer_than_a_day_is_persisted(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        before = time.time()
        await self.bridge(ScriptedApi(publish=[b.ApiError('max', 429, 2 * 86400, status=429)])).deliver('max')
        self.assertEqual(self.job()['state'], 'pending')
        self.assertGreaterEqual(self.job()['ready'], before + 2 * 86400)

    async def test_http_5xx_after_publication_wins_over_retryable_body_code(self):
        for mid, code in ((1, '429'), (2, 'attachment.not.ready')):
            self.store.accept('tg', [tg(mid)], mid + 1, '-100', '99')
            api = ScriptedApi(publish=[b.ApiError('max', code, 5, status=503)])
            await self.bridge(api).deliver('max')
            self.assertEqual((self.job(mid)['state'], len(api.calls)), ('uncertain', 1), code)
            self.make_ready()
            self.store.recover()
            restarted = ScriptedApi()
            await self.bridge(restarted).deliver('max')
            self.assertEqual((self.job(mid)['state'], restarted.calls), ('uncertain', []), code)
            self.store.update(mid, 'sent', remote_id=f'found{mid}', counted=True)

    async def test_media_download_rejections_use_retry_after_or_fail(self):
        self.photo_job()
        before = time.time()
        api = ScriptedApi(prepare=[b.ApiError('tg-media', 429, 60, status=429)])
        await self.bridge(api).deliver('max')
        self.assertEqual((self.job()['state'], api.calls), ('pending', []))
        self.assertGreaterEqual(self.job()['ready'], before + 60)
        self.make_ready()
        api = ScriptedApi(prepare=[b.ApiError('tg-media', 404, status=404)])
        await self.bridge(api).deliver('max')
        self.assertEqual((self.job()['state'], api.calls), ('failed', []))

    def test_api_error_takes_the_latest_of_telegram_and_http_hints(self):
        error = b.Api.error('tg', 429, {'error_code': 429, 'parameters': {'retry_after': 5}}, '30')
        self.assertEqual((error.code, error.retry_after), ('429', 30))
        error = b.Api.error('tg', 429, {'error_code': 429, 'parameters': {'retry_after': 50}}, '30')
        self.assertEqual(error.retry_after, 50)
        error = b.Api.error('tg', 429, {'parameters': {'retry_after': 'nan'}}, 'bogus')
        self.assertEqual(error.retry_after, 0)
        # MAX does not document a JSON retry hint: only the HTTP header counts.
        error = b.Api.error('max', 429, {'code': '429', 'parameters': {'retry_after': 500}}, None)
        self.assertEqual((error.code, error.retry_after), ('429', 0))
        self.assertEqual(b.Api.error('max', 503, {}, '12').retry_after, 12)

    def test_backoff_is_positive_bounded_and_exponential(self):
        self.assertEqual([b.backoff(n, 2, 120, rand=lambda: 0) for n in range(0, 9)],
                         [2, 2, 4, 8, 16, 32, 64, 120, 120])
        self.assertEqual(b.backoff(10 ** 6, 2, 120, rand=lambda: 1), 120 * (1 + b.BACKOFF_JITTER))
        for rand in (-5.0, 0.0, 0.5, 1.0, 7.0):
            delay = b.backoff(3, 2, 120, rand=lambda: rand)
            self.assertTrue(8 <= delay <= 8 * (1 + b.BACKOFF_JITTER), (rand, delay))

    async def test_connect_failure_during_publication_stays_pending_then_one_copy(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        api = ScriptedApi(publish=[b.ConnectError('synthetic')] * 3)
        bridge = self.bridge(api)
        delays = []
        for _ in range(3):
            before = time.time()
            await bridge.deliver('max')
            job = self.job()
            self.assertEqual(job['state'], 'pending')
            delays.append(job['ready'] - before)
            await bridge.deliver('max')  # not yet due: no request
            self.make_ready()
        self.assertEqual(len(api.calls), 3)
        self.assertTrue(delays[0] < delays[1] < delays[2] <= 120, delays)
        await bridge.deliver('max')
        self.assertEqual((self.job()['state'], len(api.calls)), ('sent', 4))

    async def test_possible_publication_failures_become_uncertain_once(self):
        for error in (b.TransportError('lost response'), b.ApiError('max', 503, 1), b.ApiError('max', 500)):
            with self.subTest(error=error):
                self.tearDown()
                self.setUp()
                self.store.accept('tg', [tg()], 2, '-100', '99')
                api = ScriptedApi(publish=[error])
                bridge = self.bridge(api)
                await bridge.deliver('max')
                self.make_ready()
                await bridge.deliver('max')
                self.assertEqual((self.job()['state'], len(api.calls)), ('uncertain', 1))

    async def test_permanent_failures_stop_the_job_without_frequent_retries(self):
        for error in (b.CertificateError('tls'), b.ApiError('max', 401), b.ApiError('max', 403),
                      b.ApiError('max', 400)):
            with self.subTest(error=error):
                self.tearDown()
                self.setUp()
                self.store.accept('tg', [tg()], 2, '-100', '99')
                api = ScriptedApi(publish=[error])
                bridge = self.bridge(api)
                for _ in range(3):
                    await bridge.deliver('max')
                    self.make_ready()
                self.assertEqual((self.job()['state'], len(api.calls)), ('failed', 1))

    async def test_photo_preparation_failures_back_off_and_success_resets(self):
        self.photo_job()
        self.store.accept('tg', [tg(2, 'дальше')], 3, '-100', '99')
        api = ScriptedApi(prepare=[b.TransportError('download'), b.ApiError('tg', 502), b.ConnectError('dns')])
        bridge = self.bridge(api)
        delays = []
        for _ in range(3):
            before = time.time()
            await bridge.deliver('max')
            self.assertEqual(self.job()['state'], 'pending')
            delays.append(self.job()['ready'] - before)
            self.make_ready()
        self.assertTrue(1.9 < delays[0] < delays[1] < delays[2], delays)
        self.assertEqual(api.calls, [])
        await bridge.deliver('max')
        self.assertEqual((self.job()['state'], api.prepared), ('sent', 4))
        api.publish = [b.ConnectError('next')]
        before = time.time()
        await bridge.deliver('max')
        self.assertLess(self.job(2)['ready'] - before, 2.1)

    async def test_attachment_not_ready_reuses_the_upload_and_publishes_once(self):
        self.photo_job()
        not_ready = b.ApiError('max', 'attachment.not.ready')
        api = ScriptedApi(publish=[not_ready, not_ready])
        bridge = self.bridge(api)
        for _ in range(3):
            await bridge.deliver('max')
            self.make_ready()
        self.assertEqual((self.job()['state'], api.prepared, len(api.calls)), ('sent', 1, 3))
        payloads = [call[2]['body']['attachments'] for call in api.calls]
        self.assertEqual(payloads, [[{'type': 'image', 'payload': {'token': 'upload1'}}]] * 3)
        self.assertEqual(bridge.prepared, {})

    async def test_expired_prepared_attachment_is_uploaded_again(self):
        self.photo_job()
        api = ScriptedApi(publish=[b.ApiError('max', 429)])
        bridge = self.bridge(api)
        await bridge.deliver('max')
        self.make_ready()
        with patch.object(b, 'ATTACHMENT_REUSE', -1):
            bridge.prepared = {k: (v[0] - 10_000, v[1]) for k, v in bridge.prepared.items()}
            await bridge.deliver('max')
        self.assertEqual((self.job()['state'], api.prepared), ('sent', 2))

    async def test_retry_after_longer_than_backoff_is_respected(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        api = ScriptedApi(publish=[b.ApiError('max', 429, 3000)])
        before = time.time()
        await self.bridge(api).deliver('max')
        self.assertGreaterEqual(self.job()['ready'], before + 3000)

    async def test_database_errors_are_not_masked_as_network_retries(self):
        self.store.accept('tg', [tg()], 2, '-100', '99')
        api = ScriptedApi(publish=[sqlite3.OperationalError('disk I/O error')])
        with self.assertRaises(sqlite3.OperationalError):
            await self.bridge(api).deliver('max')
        self.assertEqual(self.job()['state'], 'sending')


class DurabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'bridge.sqlite3'

    def open(self):
        store = b.Store(self.path)
        self.addCleanup(store.db.close)
        return store

    def batch(self, count=3, size=100):
        # Several messages, one of them split into parts, and one quarantined event.
        events = [tg(i, f'msg-{i}-' + 'x' * size) for i in range(1, count + 1)]
        events.append(tg(count + 1, 'длинное ' * 600))
        events.append({'update_id': count + 2, 'message': {'message_id': count + 2, 'chat': {'id': -100},
                       'from': {'id': 10}, 'photo': [{'file_size': 1}]}})
        return events

    def assert_nothing_acknowledged(self):
        with sqlite3.connect(self.path) as conn:
            counts = [conn.execute(f'SELECT count(*) FROM {t}').fetchone()[0] for t in ('jobs', 'inbox', 'quarantine')]
            cursor = conn.execute("SELECT value FROM meta WHERE key='cursor_tg'").fetchone()
            self.assertEqual(conn.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
        self.assertEqual((counts, cursor), ([0, 0, 0], None))

    def assert_batch_once(self, store, events):
        store.accept('tg', events, 99, '-100', '99')
        store.accept('tg', events, 99, '-100', '99')
        parts = len(b.units(b.normalize('tg', events[-2], '-100', '99')))
        self.assertGreater(parts, 1)
        jobs = store.db.execute('SELECT count(*) FROM jobs').fetchone()[0]
        self.assertEqual(jobs, len(events) - 2 + parts)
        self.assertEqual(store.db.execute('SELECT count(*) FROM quarantine').fetchone()[0], 1)
        self.assertEqual(store.get('cursor_tg'), '99')

    def test_writer_uses_verified_wal_and_full_synchronous(self):
        store = self.open()
        self.assertEqual(store.db.execute('PRAGMA journal_mode').fetchone()[0], 'wal')
        self.assertEqual(store.db.execute('PRAGMA synchronous').fetchone()[0], 2)
        store.accept('tg', [tg()], 2, '-100', '99')
        store.db.close()
        # Committed data is visible to a fresh connection; WAL mode is persistent.
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(conn.execute('PRAGMA journal_mode').fetchone()[0], 'wal')
            self.assertEqual(conn.execute('SELECT count(*) FROM jobs').fetchone()[0], 1)

    def test_settings_failure_stops_start_without_changing_the_database(self):
        with sqlite3.connect(self.path) as conn:
            conn.execute('CREATE TABLE keep(x)')
        original = self.path.read_bytes()
        blocker = sqlite3.connect(self.path)
        blocker.execute('BEGIN IMMEDIATE')
        try:
            # Switching to WAL needs an exclusive lock: a real SQLITE_BUSY stops the start.
            with self.assertRaises(sqlite3.OperationalError):
                b.Store(self.path)
        finally:
            blocker.rollback()
            blocker.close()
        self.assertEqual(self.path.read_bytes(), original)

    def test_corrupt_or_newer_database_is_refused_and_kept(self):
        self.path.write_bytes(b'not a database' * 100)
        with self.assertRaises(sqlite3.DatabaseError):
            b.Store(self.path)
        self.assertEqual(self.path.read_bytes(), b'not a database' * 100)
        self.path.unlink()
        self.open().db.execute(f'PRAGMA user_version={b.SCHEMA_VERSION + 1}')
        original = self.path.read_bytes()
        with self.assertRaises(b.BridgeError):
            b.Store(self.path)
        self.assertEqual(self.path.read_bytes(), original)
        self.assertEqual(sorted(p.name for p in self.path.parent.iterdir() if '.bak' in p.name), [])

    def test_real_sqlite_full_rolls_back_the_whole_batch_and_cursor(self):
        store = self.open()
        pages = store.db.execute('PRAGMA page_count').fetchone()[0]
        store.db.execute(f'PRAGMA max_page_count={pages + 2}')
        events = self.batch(count=40, size=2000)
        with self.assertRaises(sqlite3.OperationalError) as caught:
            store.accept('tg', events, 99, '-100', '99')
        self.assertIn('full', str(caught.exception))
        self.assertNotIsInstance(caught.exception, b.BridgeError)
        store.db.close()
        self.assert_nothing_acknowledged()
        self.assert_batch_once(self.open(), events)

    def test_real_sqlite_full_rolls_back_a_max_batch_too(self):
        store = self.open()
        events = [mx(f'm{i}', 'y' * 3000) for i in range(40)] + [mx('bad', text=[])]
        store.db.execute(f"PRAGMA max_page_count={store.db.execute('PRAGMA page_count').fetchone()[0] + 2}")
        with self.assertRaises(sqlite3.OperationalError):
            store.accept('max', events, 'marker-2', '-200', '88')
        store.db.close()
        store = self.open()
        self.assertEqual((store.get('cursor_max'), store.db.execute('SELECT count(*) FROM jobs').fetchone()[0]), (None, 0))
        store.accept('max', events, 'marker-2', '-200', '88')
        store.accept('max', events, 'marker-2', '-200', '88')
        self.assertEqual(store.db.execute('SELECT count(*) FROM jobs').fetchone()[0], 40)
        self.assertEqual(store.db.execute('SELECT count(*) FROM quarantine').fetchone()[0], 1)
        self.assertEqual(store.get('cursor_max'), 'marker-2')

    def test_real_commit_failure_is_not_acknowledged_and_batch_is_kept_once(self):
        self.open().db.close()
        events = self.batch(count=40, size=3000)
        # A child process gets a file size limit: statements fit in the page cache, so the
        # first write happens in COMMIT (WAL frames) and fails there with a real I/O error.
        script = """
import importlib.util, json, resource, signal, sqlite3, sys
spec = importlib.util.spec_from_file_location('bridge', sys.argv[1])
b = importlib.util.module_from_spec(spec); spec.loader.exec_module(b)
store = b.Store(sys.argv[2])
store.db.execute('PRAGMA cache_size=-65536')
real_set = b.Store.set
def set_(self, key, value):
    real_set(self, key, value)
    if key == 'cursor_tg':
        print('before-commit', flush=True)
b.Store.set = set_
signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
limit = 64 * 1024
resource.setrlimit(resource.RLIMIT_FSIZE, (limit, limit))
try:
    store.accept('tg', json.loads(sys.stdin.read()), 99, '-100', '99')
except sqlite3.Error as exc:
    print('commit-error', type(exc).__name__, flush=True)
"""
        for name in ('bridge.sqlite3', 'bridge.sqlite3-wal'):
            if (self.path.parent / name).exists():
                self.assertLess((self.path.parent / name).stat().st_size, 64 * 1024)
        result = subprocess.run([sys.executable, '-c', script, str(Path(b.__file__)), str(self.path)],
                                input=json.dumps(events), capture_output=True, text=True, timeout=60)
        self.assertEqual(result.stdout.split(), ['before-commit', 'commit-error', 'OperationalError'], result)
        self.assert_nothing_acknowledged()
        self.assert_batch_once(self.open(), events)

    def test_foreign_binding_is_rejected_before_the_queue_is_touched(self):
        store = self.open()
        store.accept('tg', [tg()], 2, '-100', '99')
        with store.db:
            store.set('identity', json.dumps(['1', '2', '-100', '-200']))
            store.db.execute("UPDATE jobs SET state='sending'")
        store.db.close()

        class NoApi:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_exc):
                pass

        with patch.object(b, 'config', return_value={'TG_CHAT_ID': '-100', 'MAX_CHAT_ID': '-200'}), \
                patch.object(b, 'Api', return_value=NoApi()), \
                patch.object(b, 'doctor', AsyncMock(return_value={'tg': '99', 'max': '88'})), \
                patch.object(b, 'run_bridge', AsyncMock()) as run:
            with self.assertRaisesRegex(b.BridgeError, 'другой паре'):
                asyncio.run(b.network_command(SimpleNamespace(command='run'), self.path.parent, self.path))
        run.assert_not_awaited()
        conn = sqlite3.connect(self.path)
        try:
            # recover() did not run: the foreign database is left exactly as it was.
            self.assertEqual(conn.execute('SELECT state FROM jobs').fetchone()[0], 'sending')
            self.assertEqual(json.loads(conn.execute("SELECT value FROM meta WHERE key='identity'").fetchone()[0])[0], '1')
        finally:
            conn.close()

    def test_stand_snapshot_ignores_sidecar_files_but_detects_data_changes(self):
        spec = importlib.util.spec_from_file_location('stand_ctl', Path(__file__).parent / 'stand' / 'stand_ctl.py')
        ctl = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(ctl)
        store = self.open()
        store.accept('tg', [tg()], 2, '-100', '99')
        store.db.close()
        before = ctl.logical_snapshot(self.path)
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(b.main(['status', '--json', '--state-dir', str(self.path.parent)]), 0)
        self.assertEqual(json.loads(output.getvalue())['schema_version'], b.SCHEMA_VERSION)
        self.assertEqual(ctl.logical_snapshot(self.path), before)
        reader = sqlite3.connect(self.path)
        try:
            reader.execute('SELECT count(*) FROM jobs').fetchone()
            self.assertTrue(Path(str(self.path) + '-wal').exists() and Path(str(self.path) + '-shm').exists())
            self.assertEqual(ctl.logical_snapshot(self.path), before)
        finally:
            reader.close()
        for change in ("UPDATE meta SET value='3' WHERE key='cursor_tg'", "UPDATE jobs SET state='sent'",
                       'PRAGMA user_version=2'):
            conn = sqlite3.connect(self.path)
            try:
                conn.execute(change)
                conn.commit()
            finally:
                conn.close()
            after = ctl.logical_snapshot(self.path)
            self.assertNotEqual(after, before, change)
            before = after

    def test_recover_is_idempotent_and_keeps_every_state(self):
        store = self.open()
        store.accept('tg', [tg(i) for i in range(1, 7)], 7, '-100', '99')
        store.accept('max', [mx('m1')], 'mk', '-200', '88')
        with store.db:
            store.db.execute("UPDATE jobs SET state='sent',remote_id='r1' WHERE id=1")
            store.db.execute("UPDATE jobs SET state='failed' WHERE id=2")
            store.db.execute("UPDATE jobs SET state='uncertain' WHERE id=3")
            store.db.execute("UPDATE jobs SET state='skipped' WHERE id=4")
            store.db.execute("UPDATE jobs SET state='sending' WHERE id=5")
            store.db.execute("UPDATE jobs SET ready=12345.5 WHERE id=6")
            store.audit('job', 2, 'retry', 'failed', 'failed', 'test')
        store.db.close()
        for _ in range(2):
            store = self.open()
            store.recover()
            rows = store.db.execute('SELECT id,state,remote_id,ready FROM jobs ORDER BY id').fetchall()
            self.assertEqual([tuple(r)[:3] for r in rows],
                             [(1, 'sent', 'r1'), (2, 'failed', None), (3, 'uncertain', None), (4, 'skipped', None),
                              (5, 'uncertain', None), (6, 'pending', None), (7, 'pending', None)])
            self.assertEqual(rows[5]['ready'], 12345.5)
            self.assertEqual((store.get('cursor_tg'), store.get('cursor_max')), ('7', 'mk'))
            self.assertEqual(store.db.execute('SELECT count(*) FROM audit').fetchone()[0], 1)
            store.db.close()


if __name__ == '__main__':
    unittest.main()
