import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock

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

    def test_cursor_and_jobs_are_atomic_on_bad_image(self):
        event = mx()
        event['message']['body']['attachments'] = [{'type': 'image', 'payload': {}}]
        with self.assertRaises(b.BridgeError):
            self.store.accept('max', [mx('first'), event], 3, '-200', '88')
        self.assertEqual(len(self.jobs()), 0)
        self.assertIsNone(self.store.get('cursor_max'))

    def test_unknown_media_hosts_rejected(self):
        for url in ('http://i.oneme.ru/a', 'https://localhost/a', 'https://oneme.ru.evil.test/a',
                    'https://127.0.0.1/a', 'https://i.oneme.ru:8080/a'):
            with self.assertRaises(b.BridgeError):
                b.check_media_url(url)
        b.check_media_url('https://i.oneme.ru/image')

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
