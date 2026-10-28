import tempfile
import unittest
from pathlib import Path

import bridge as b

SECRET_TG = 'file-tg-secret-123'
SECRET_MAX = 'file-max-secret-456'


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = Path(self.tmp.name) / '.env'

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, text):
        self.env.write_text(text)

    def load(self, environ=None, require_chats=False):
        return b.config(require_chats=require_chats, environ=environ or {}, env_file=self.env)

    def test_file_only_keeps_local_env_compatibility(self):
        self.write(f'# comment\nTG_BOT_TOKEN="{SECRET_TG}"\nMAX_BOT_TOKEN=\'{SECRET_MAX}\'\n'
                   'TG_CHAT_ID=-100\nMAX_CHAT_ID= -200 \n')
        cfg = self.load(require_chats=True)
        self.assertEqual((cfg['TG_BOT_TOKEN'], cfg['MAX_BOT_TOKEN']), (SECRET_TG, SECRET_MAX))
        self.assertEqual((cfg['TG_CHAT_ID'], cfg['MAX_CHAT_ID']), ('-100', '-200'))

    def test_environment_overrides_file(self):
        self.write(f'TG_BOT_TOKEN={SECRET_TG}\nMAX_BOT_TOKEN={SECRET_MAX}\nTG_CHAT_ID=-1\nMAX_CHAT_ID=-2\n')
        cfg = self.load({'TG_BOT_TOKEN': 'env-tg', 'MAX_CHAT_ID': ' -300 '}, require_chats=True)
        self.assertEqual(cfg['TG_BOT_TOKEN'], 'env-tg')
        self.assertEqual(cfg['MAX_BOT_TOKEN'], SECRET_MAX)
        self.assertEqual((cfg['TG_CHAT_ID'], cfg['MAX_CHAT_ID']), ('-1', '-300'))

    def test_environment_only_without_file(self):
        cfg = self.load({'TG_BOT_TOKEN': 'a', 'MAX_BOT_TOKEN': 'b', 'TG_CHAT_ID': '-1', 'MAX_CHAT_ID': '-2'},
                        require_chats=True)
        self.assertEqual(cfg['MAX_CHAT_ID'], '-2')

    def test_unrelated_environment_is_ignored(self):
        cfg = self.load({'TG_BOT_TOKEN': 'a', 'MAX_BOT_TOKEN': 'b', 'PATH': '/bin', 'HOME': '/x'})
        self.assertNotIn('PATH', cfg)
        self.assertNotIn('HOME', cfg)

    def assert_safe_error(self, environ, expected, require_chats=False):
        with self.assertRaises(b.BridgeError) as caught:
            self.load(environ, require_chats=require_chats)
        message = str(caught.exception)
        self.assertIn(expected, message)
        for secret in (SECRET_TG, SECRET_MAX, 'env-secret-789'):
            self.assertNotIn(secret, message)
        return message

    def test_missing_token_fails_clearly(self):
        self.write(f'MAX_BOT_TOKEN={SECRET_MAX}\n')
        self.assert_safe_error({}, 'Не задан TG_BOT_TOKEN')

    def test_empty_environment_value_overrides_file_and_fails(self):
        self.write(f'TG_BOT_TOKEN={SECRET_TG}\nMAX_BOT_TOKEN={SECRET_MAX}\n')
        self.assert_safe_error({'MAX_BOT_TOKEN': '   '}, 'Не задан MAX_BOT_TOKEN')

    def test_missing_and_empty_chats_fail_for_run(self):
        base = {'TG_BOT_TOKEN': 'env-secret-789', 'MAX_BOT_TOKEN': 'env-secret-789'}
        self.assert_safe_error({**base, 'MAX_CHAT_ID': '-2'}, 'TG_CHAT_ID', require_chats=True)
        self.assert_safe_error({**base, 'TG_CHAT_ID': '-1', 'MAX_CHAT_ID': ''}, 'MAX_CHAT_ID', require_chats=True)

    def test_chats_optional_for_diagnostics(self):
        cfg = self.load({'TG_BOT_TOKEN': 'a', 'MAX_BOT_TOKEN': 'b'})
        self.assertFalse(cfg.get('TG_CHAT_ID'))

    def test_invalid_chat_id_does_not_echo_value(self):
        message = self.assert_safe_error({'TG_BOT_TOKEN': 'a', 'MAX_BOT_TOKEN': 'b', 'TG_CHAT_ID': 'env-secret-789'},
                                         'TG_CHAT_ID должен быть целым числом')
        self.assertNotIn('env-secret-789', message)

    def test_missing_ca_file_fails_without_path_leak(self):
        missing = str(Path(self.tmp.name) / 'env-secret-789.pem')
        message = self.assert_safe_error({'TG_BOT_TOKEN': 'a', 'MAX_BOT_TOKEN': 'b', 'MAX_CA_FILE': missing},
                                         'MAX_CA_FILE')
        self.assertNotIn(missing, message)

    def test_existing_ca_file_and_empty_ca_are_accepted(self):
        ca = Path(self.tmp.name) / 'ca.pem'
        ca.write_text('x')
        self.assertEqual(self.load({'TG_BOT_TOKEN': 'a', 'MAX_BOT_TOKEN': 'b', 'MAX_CA_FILE': str(ca)})['MAX_CA_FILE'],
                         str(ca))
        self.write('MAX_CA_FILE=.state/missing.pem\n')
        self.assertEqual(self.load({'TG_BOT_TOKEN': 'a', 'MAX_BOT_TOKEN': 'b', 'MAX_CA_FILE': ''})['MAX_CA_FILE'], '')


if __name__ == '__main__':
    unittest.main()
