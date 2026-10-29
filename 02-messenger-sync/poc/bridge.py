#!/usr/bin/env python3
"""Async Telegram/MAX test bridge with durable SQLite delivery; polling is test-only."""
from __future__ import annotations

import argparse
import asyncio
import contextvars
import email.utils
import fcntl
import hashlib
import json
import math
import mimetypes
import os
import random
import re
import secrets
import signal
import sqlite3
import ssl
import sys
import time
import urllib.parse
from contextlib import contextmanager
from pathlib import Path

import aiohttp
import storage

ROOT = Path(__file__).resolve().parents[1]
ENV = ROOT / '.env'
STATE = ROOT / '.state'
MAX_API = 'https://platform-api2.max.ru'
PHOTO_LIMIT = 10 * 1024 * 1024
SCHEMA_VERSION = 3
# Server-requested waits above a day are clamped so that absurd values stay representable.
RETRY_AFTER_DIGITS = 300
# Safe retries: exponential delay base*2**(n-1) up to cap, then +0..20% jitter.
POLL_BACKOFF = (2, 30)
SEND_BACKOFF = (2, 120)
BACKOFF_JITTER = 0.2
# A prepared MAX attachment is reused for this long after attachment.not.ready / 429.
ATTACHMENT_REUSE = 600
# Operation budgets: a worker phase that runs this long past its deadline is reported as stalled.
LOG_FIELDS = ('platform', 'direction', 'worker', 'job_id', 'part', 'state', 'phase', 'attempt', 'http_status',
              'error_code', 'retry_at', 'delay_seconds', 'duration_ms', 'count', 'kind', 'issue_code', 'event_id')
SAFE_VALUE = re.compile(r'[A-Za-z0-9_.:>-]{1,64}')
RUN = {'id': None, 'component': None}
ERROR_CODES = frozenset({'tls_failed', 'attachment_not_ready', 'server_error', 'rate_limited', 'auth_rejected',
                         'poll_conflict', 'api_rejected', 'connect_failed', 'transport_error', 'bridge_error',
                         'storage_error', 'storage_pressure', 'internal_error', 'cancelled', 'bad_response', 'interrupted'})
WORKERS = ('poll:tg', 'poll:max', 'send:tg', 'send:max')
RECOVERY_CONFIRMATIONS = 2
# Progress entry of the worker running in the current task; every HTTP request extends its deadline.
WORKER = contextvars.ContextVar('worker', default=None)


def touch():
    current = WORKER.get()
    if current is not None:
        entry, budget = current
        entry['progress_at'] = time.time()
        entry['until'] = max(entry['until'] or 0, entry['progress_at'] + budget)


def server_failure(exc):
    """HTTP 5xx by status or by an ASCII three-digit code; untrusted codes never raise."""
    return any(re.fullmatch(r'[0-9]{3}', str(c)) and int(c) >= 500 for c in (exc.code, exc.status or 0))
MEDIA_DOMAINS = ('oneme.ru', 'max.ru', 'okcdn.ru', 'mycdn.me', 'userapi.com',
                 'vkuserphoto.ru', 'vk-cdn.net')


class BridgeError(Exception):
    """Only controlled, secret-free messages may be put in this exception."""


class ApiError(BridgeError):
    def __init__(self, service, code, retry_after=0, status=None):
        self.code = str(code)
        self.retry_after = retry_after
        # HTTP status is kept apart from the API code: a 5xx may follow a publication.
        self.status = status
        super().__init__(f'{service}: API error {self.code}')


class TransportError(BridgeError):
    """Network failure after the request may have reached the server."""


class ConnectError(TransportError):
    """No connection was established, so the request was provably not sent."""


class CertificateError(BridgeError):
    """TLS certificate verification failed; a retry cannot help."""


def retry_after_seconds(value, now=None):
    """Seconds from Telegram retry_after or HTTP Retry-After (delay-seconds or HTTP-date).

    Valid server delays are never shortened. Unusable hints (bool, empty, <=0, NaN/Infinity,
    bad dates, values beyond float range) give 0, so the caller falls back to its own backoff.
    """
    try:
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            return 0
        if isinstance(value, str):
            text = value.strip()
            if re.fullmatch('[0-9]+', text):
                digits = text.lstrip('0') or '0'
                # int() rejects very long digit strings in Python 3.12; such values are unrepresentable anyway.
                if len(digits) > RETRY_AFTER_DIGITS:
                    return 0
                value = int(digits)
            else:
                date = email.utils.parsedate_to_datetime(text)
                if date is None or date.tzinfo is None:
                    return 0
                value = date.timestamp() - (time.time() if now is None else now)
        value = float(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return 0
    if not math.isfinite(value) or value <= 0 or value > 10.0 ** RETRY_AFTER_DIGITS:
        return 0
    return value


def backoff(failures, base, cap, rand=random.random):
    """Positive bounded exponential delay with bounded jitter: [d, d*(1+BACKOFF_JITTER)]."""
    delay = min(cap, base * 2 ** min(max(failures, 1) - 1, 30))
    return delay * (1 + BACKOFF_JITTER * min(max(rand(), 0.0), 1.0))


def utc(moment):
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(moment)) + f'.{int(moment % 1 * 1000):03d}Z'


def log(level, event, message=None, stream=None, **fields):
    """One JSON line per event; only allow-listed fields with safe scalar values (no text, URLs, bodies)."""
    record = {'timestamp': utc(time.time()), 'level': level, 'component': RUN['component'] or 'bridge', 'event': event, 'run_id': RUN['id']}
    for key in LOG_FIELDS:
        value = fields.get(key)
        if isinstance(value, bool) or value is None:
            continue
        if isinstance(value, float) and math.isfinite(value):
            record[key] = round(value, 3)
        elif isinstance(value, int) or (isinstance(value, str) and SAFE_VALUE.fullmatch(value)):
            record[key] = value
    if message is not None:
        record['message'] = message
    print(json.dumps(record, ensure_ascii=False), file=stream or sys.stdout, flush=True)


def error_info(exc):
    """Safe classification of an error: (code from a fixed vocabulary, HTTP status or None)."""
    if isinstance(exc, CertificateError):
        return 'tls_failed', None
    if isinstance(exc, ApiError):
        status = exc.status if isinstance(exc.status, int) and not isinstance(exc.status, bool) else None
        code = exc.code
        # HTTP 5xx wins over a body code, as in the delivery policy.
        if server_failure(exc):
            return 'server_error', status
        if code == 'attachment.not.ready':
            return 'attachment_not_ready', status
        return {'429': 'rate_limited', '401': 'auth_rejected', '403': 'auth_rejected',
                '409': 'poll_conflict'}.get(code, 'api_rejected'), status
    if isinstance(exc, ConnectError):
        return 'connect_failed', None
    if isinstance(exc, TransportError):
        return 'transport_error', None
    if isinstance(exc, BridgeError):
        return 'bridge_error', None
    if isinstance(exc, sqlite3.Error):
        return 'storage_error', None
    return 'internal_error', None


def error_text(code, status, message):
    return f"{code}{f' http={status}' if status else ''}: {message}"


def error_code(text):
    code = (text or '').split(':', 1)[0].split(' ', 1)[0]
    return code if code in ERROR_CODES else None


class EventError(BridgeError):
    """An expected input validation error, safe to retain without the payload."""


def event_object(value):
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise EventError('Ожидался объект в событии')
    return value


def event_text(value):
    if value is None:
        return ''
    if not isinstance(value, str):
        raise EventError('Ожидалась строка в событии')
    return value


def event_list(value):
    if value is None:
        return []
    if not isinstance(value, list):
        raise EventError('Ожидался список в событии')
    return value


def selected_message(platform, event, chat_id):
    # An envelope with no identifiable chat cannot safely be archived or acknowledged.
    if not isinstance(event, dict):
        raise TransportError('Невозможно определить чат входящего события')
    if platform == 'max' and event.get('update_type') != 'message_created':
        return None
    msg = event.get('message')
    if msg is None:
        return None
    chat_field, id_field = ('chat', 'id') if platform == 'tg' else ('recipient', 'chat_id')
    if (not isinstance(msg, dict) or not isinstance(msg.get(chat_field), dict)
            or msg[chat_field].get(id_field) is None):
        raise TransportError('Невозможно определить чат входящего события')
    return msg if str(msg[chat_field][id_field]) == str(chat_id) else None


def event_key(platform, event):
    if platform == 'tg':
        key = event.get('update_id')
    else:
        body = event['message'].get('body')
        key = body.get('mid') if isinstance(body, dict) else None
    if isinstance(key, (str, int)) and not isinstance(key, bool) and str(key):
        return 'id:' + str(key)
    raw = json.dumps(event, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    return 'sha256:' + hashlib.sha256(raw.encode()).hexdigest()


CONFIG_KEYS = ('TG_BOT_TOKEN', 'MAX_BOT_TOKEN', 'TG_BOT_USERNAME', 'MAX_BOT_USERNAME',
               'MAX_CA_FILE', 'TG_CHAT_ID', 'MAX_CHAT_ID')


def config_values(keys, environ=None, env_file=None):
    """Read local .env and override only the requested keys from the process environment."""
    environ = os.environ if environ is None else environ
    env_file = ENV if env_file is None else env_file
    values = {}
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                values[key.strip()] = value.strip().strip('\"\'')
    for key in keys:
        if key in environ:
            values[key] = environ[key].strip()
    return values


def storage_settings(environ=None, env_file=None):
    try:
        return storage.Settings.load(config_values(storage.DEFAULTS, environ, env_file))
    except ValueError as exc:
        raise BridgeError(str(exc)) from None


def config(require_chats=False, environ=None, env_file=None):
    values = config_values(CONFIG_KEYS, environ, env_file)
    for key in ('TG_BOT_TOKEN', 'MAX_BOT_TOKEN'):
        if not values.get(key):
            raise BridgeError(f'Не задан {key}: укажите переменную окружения или строку в локальном .env')
    for key in ('TG_CHAT_ID', 'MAX_CHAT_ID'):
        if require_chats and not values.get(key):
            raise BridgeError(f'Не выбран тестовый чат: {key}')
        if values.get(key):
            try:
                values[key] = str(int(values[key]))
            except ValueError:
                raise BridgeError(f'{key} должен быть целым числом') from None
    if values.get('MAX_CA_FILE'):
        ca_path = Path(values['MAX_CA_FILE'])
        if not (ca_path if ca_path.is_absolute() else ROOT / ca_path).is_file():
            raise BridgeError('MAX_CA_FILE: файл сертификата не найден')
    return values


def check_media_url(url):
    parsed = urllib.parse.urlsplit(url)
    host = (parsed.hostname or '').lower()
    if (parsed.scheme != 'https' or parsed.username or parsed.password
            or parsed.port not in (None, 443)
            or not any(host == d or host.endswith('.' + d) for d in MEDIA_DOMAINS)):
        raise BridgeError('Неизвестный адрес медиа MAX: требуется проверка домена')


def multipart(fields, field, data, filename, mime):
    boundary = 'sync_' + secrets.token_hex(16)
    parts = []
    for key, value in fields.items():
        if not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False)
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode())
    safe_name = Path(filename).name.replace('"', '').replace('\r', '').replace('\n', '')
    parts.append((f'--{boundary}\r\nContent-Disposition: form-data; name="{field}"; '
                  f'filename="{safe_name}"\r\nContent-Type: {mime}\r\n\r\n').encode())
    parts.extend([data, f'\r\n--{boundary}--\r\n'.encode()])
    return b''.join(parts), f'multipart/form-data; boundary={boundary}'


class Api:
    """Bounded async transport; publication is never retried by this adapter."""
    def __init__(self, cfg, *, tg_base=None, max_base=None, tg_file_base=None, timeout=None):
        self.cfg = cfg
        self.tg_base = tg_base or 'https://api.telegram.org/bot' + cfg['TG_BOT_TOKEN']
        self.max_base = max_base or MAX_API
        self.tg_file_base = tg_file_base or 'https://api.telegram.org/file/bot' + cfg['TG_BOT_TOKEN']
        self.timeout = timeout or aiohttp.ClientTimeout(total=25, connect=5, sock_read=20)
        self.contexts = {'tg': ssl.create_default_context(), 'max': ssl.create_default_context()}
        if cfg.get('MAX_CA_FILE'):
            ca_path = Path(cfg['MAX_CA_FILE'])
            self.contexts['max'].load_verify_locations(cafile=str(ca_path if ca_path.is_absolute() else ROOT / ca_path))
        self.sessions = {}

    async def __aenter__(self):
        try:
            for platform in ('tg', 'max'):
                connector = aiohttp.TCPConnector(ssl=self.contexts[platform], limit=6, limit_per_host=4)
                self.sessions[platform] = aiohttp.ClientSession(connector=connector, timeout=self.timeout,
                    cookie_jar=aiohttp.DummyCookieJar(), trust_env=False, auto_decompress=False,
                    headers={'Accept-Encoding': 'identity'})
        except BaseException:
            await self.__aexit__(None, None, None)
            raise
        return self

    async def __aexit__(self, *_exc):
        for session in self.sessions.values():
            await session.close()
        self.sessions.clear()

    async def read(self, platform, url, *, method='GET', headers=None, data=None,
                   limit=2 * 1024 * 1024, redirects=False, meta=None):
        try:
            for hop in range(6):
                touch()
                async with self.sessions[platform].request(method, url, headers=headers,
                        data=data, allow_redirects=False) as response:
                    if response.status in (301, 302, 303, 307, 308) and redirects:
                        if method != 'GET' or hop == 5 or 'Location' not in response.headers:
                            raise BridgeError('Небезопасное перенаправление картинки')
                        url = urllib.parse.urljoin(url, response.headers['Location'])
                        check_media_url(url)
                        headers = {'User-Agent': 'messenger-sync/1'}
                        continue
                    chunks, size = [], 0
                    async for chunk in response.content.iter_chunked(65536):
                        size += len(chunk)
                        if size > limit:
                            raise TransportError('Превышен предел размера ответа')
                        chunks.append(chunk)
                    if meta is not None:
                        meta['retry_after'] = response.headers.get('Retry-After')
                    return response.status, b''.join(chunks)
        except aiohttp.ClientConnectorCertificateError:
            raise CertificateError('Ошибка проверки TLS-сертификата') from None
        except (aiohttp.ClientConnectorError, aiohttp.ConnectionTimeoutError):
            # Raised while connecting (or waiting for a pooled connection): nothing was sent.
            raise ConnectError('Соединение не установлено; запрос не отправлен') from None
        except (aiohttp.ClientError, OSError, TimeoutError):
            raise TransportError('Ошибка соединения или TLS') from None

    @staticmethod
    def error(service, status, result, retry_header=None):
        result = result if isinstance(result, dict) else {}
        code = result.get('error_code', result.get('code', status))
        if (not isinstance(code, (str, int)) or len(str(code)) > 64
                or (str(code) != 'attachment.not.ready' and not re.fullmatch(r'[0-9]{1,64}', str(code)))):
            code = status
        # Only Telegram documents parameters.retry_after; MAX gets the standard header only.
        params = result.get('parameters') if service == 'tg' else None
        retry = retry_after_seconds(params.get('retry_after')) if isinstance(params, dict) else 0
        return ApiError(service, code, max(retry, retry_after_seconds(retry_header)), status=status)

    async def request(self, service, path, method='GET', query=None, body=None, upload=None):
        if service == 'tg':
            url, platform, headers = self.tg_base + '/' + path, 'tg', {}
        elif service == 'max':
            url, platform = self.max_base + path, 'max'
            headers = {'Authorization': self.cfg['MAX_BOT_TOKEN']}
        else:
            check_media_url(path)
            url, platform, headers = path, 'max', {}
        if query:
            url += '?' + urllib.parse.urlencode({k: v for k, v in query.items() if v is not None})
        if upload:
            data, mime = upload
            headers['Content-Type'] = mime
        elif body is not None:
            data = json.dumps(body, ensure_ascii=False).encode()
            headers['Content-Type'] = 'application/json'
        else:
            data = None
        meta = {}
        status, raw = await self.read(platform, url, method=method, headers=headers, data=data, meta=meta)
        retry_header = meta.get('retry_after')
        try:
            result = json.loads(raw)
        except (ValueError, UnicodeError):
            if status >= 300:
                raise self.error(service, status, {}, retry_header) from None
            raise TransportError(f'{service}: не удалось разобрать ответ API') from None
        if status >= 300:
            raise self.error(service, status, result, retry_header)
        if not isinstance(result, dict):
            raise TransportError(f'{service}: неожиданный формат ответа')
        if service == 'tg':
            if not result.get('ok'):
                raise self.error(service, 400, result)
            if 'result' not in result:
                raise TransportError('tg: отсутствует результат API')
            return result['result']
        return result

    async def download(self, platform, reference):
        if platform == 'tg':
            info = await self.request('tg', 'getFile', query={'file_id': reference})
            if info.get('file_size', 0) > PHOTO_LIMIT:
                raise BridgeError('Картинка превышает предел PoC 10 MiB')
            file_path = info.get('file_path', '')
            if not file_path or '..' in file_path.split('/'):
                raise BridgeError('Не получен безопасный путь картинки Telegram')
            url = self.tg_file_base + '/' + file_path
        else:
            check_media_url(reference)
            url = reference
        meta = {}
        status, data = await self.read(platform, url, limit=PHOTO_LIMIT, redirects=True, meta=meta)
        if status != 200:
            # 429 and 5xx are temporary; any other status is a definite rejection of this media.
            raise ApiError(platform + '-media', status, retry_after_seconds(meta.get('retry_after')), status=status)
        if data.startswith(b'\xff\xd8\xff'):
            mime = 'image/jpeg'
        elif data.startswith(b'\x89PNG\r\n\x1a\n'):
            mime = 'image/png'
        elif data.startswith(b'RIFF') and data[8:12] == b'WEBP':
            mime = 'image/webp'
        else:
            raise BridgeError('Для PoC поддерживаются картинки JPEG, PNG и WEBP')
        return data, mime

    async def prepare_photo(self, source, reference, destination):
        data, mime = await self.download(source, reference)
        filename = 'image' + (mimetypes.guess_extension(mime) or '.jpg')
        if destination == 'tg':
            return data, mime, filename
        endpoint = await self.request('max', '/uploads', method='POST', query={'type': 'image'})
        uploaded = await self.request('media', endpoint['url'], method='POST',
            upload=multipart({}, 'data', data, filename, mime))
        if uploaded.get('photos'):
            return {'photos': uploaded['photos']}
        token = uploaded.get('token') or endpoint.get('token')
        if token:
            return {'token': token}
        raise BridgeError('Ответ загрузки MAX не содержит токен картинки')


def split_text(text, limit=3500):
    """Telegram counts UTF-16 units; keep astral emoji within the same limit too."""
    result, current, size = [], [], 0
    for char in text:
        units = 2 if ord(char) > 0xffff else 1
        if size + units > limit:
            result.append(''.join(current))
            current, size = [], 0
        current.append(char)
        size += units
    if current:
        result.append(''.join(current))
    return result


def normalize(platform, event, chat_id, own_id):
    msg = selected_message(platform, event, chat_id)
    if msg is None:
        return None
    if platform == 'tg':
        sender = event_object(msg.get('from'))
        if str(sender.get('id')) == str(own_id):
            return None
        author = event_object(msg.get('sender_chat')) or sender
        name = event_text(author.get('title')) or ' '.join(filter(None, [
            event_text(author.get('first_name')), event_text(author.get('last_name'))])) or 'Участник'
        text = event_text(msg.get('text')) or event_text(msg.get('caption'))
        photos = event_list(msg.get('photo'))
        for photo in photos:
            photo = event_object(photo)
            if not event_text(photo.get('file_id')):
                raise EventError('В событии Telegram отсутствует ID картинки')
            for key in ('file_size', 'width'):
                value = photo.get(key, 0)
                if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                    raise EventError('Неверный размер картинки Telegram')
        images = [max(photos, key=lambda p: (p.get('file_size', 0), p.get('width', 0)))['file_id']] if photos else []
        reply = event_object(msg.get('reply_to_message'))
        parent = reply.get('message_id')
        quote = event_text(reply.get('text')) or event_text(reply.get('caption'))
        unsupported = any(k in msg for k in ('video', 'document', 'voice', 'audio', 'sticker', 'animation', 'poll', 'contact', 'location', 'video_note'))
        mid = msg.get('message_id')
    else:
        sender = event_object(msg.get('sender'))
        if str(sender.get('user_id')) == str(own_id):
            return None
        name = ' '.join(filter(None, [event_text(sender.get('first_name')),
                                     event_text(sender.get('last_name'))])) or event_text(sender.get('name')) or 'Участник'
        body = event_object(msg.get('body'))
        mid, text = body.get('mid'), event_text(body.get('text'))
        attachments = event_list(body.get('attachments'))
        images, unsupported = [], False
        for attachment in attachments:
            attachment = event_object(attachment)
            if attachment.get('type') == 'image':
                reference = event_text(event_object(attachment.get('payload')).get('url'))
                if not reference:
                    raise EventError('В событии MAX отсутствует URL картинки')
                images.append(reference)
            else:
                unsupported = True
        link = event_object(msg.get('link'))
        reply = event_object(link.get('message')) if link.get('type') == 'reply' else {}
        parent, quote = reply.get('mid'), event_text(reply.get('text'))
    if not (text or images or unsupported):
        return None
    if not isinstance(mid, (str, int)) or isinstance(mid, bool) or not str(mid):
        raise EventError('В событии отсутствует корректный ID сообщения')
    if parent is not None and (not isinstance(parent, (str, int)) or isinstance(parent, bool)):
        raise EventError('Неверный ID исходного сообщения ответа')
    # No rich HTML/Markdown: sender names and message text remain plain text.
    name = ' '.join(name.split())[:100]
    return {'platform': platform, 'chat': str(chat_id), 'mid': str(mid), 'name': name,
            'text': text, 'images': images, 'parent': str(parent) if parent is not None else None,
            'quote': quote[:250], 'unsupported': unsupported}


def units(message):
    label = 'Telegram' if message['platform'] == 'tg' else 'MAX'
    text = f"{message['name']} · {label}\n{message['text']}"
    if message['quote']:
        text += '\n\nВ ответ на: ' + message['quote']
    if message['unsupported']:
        text += '\n[Вложение вне объёма теста не перенесено]'
    # The first photo carries a short caption; long text is delivered as separate pieces.
    chunks = split_text(text, 900 if message['images'] else 3500)
    result = []
    if message['images']:
        result.append({'kind': 'photo', 'text': chunks.pop(0), 'reference': message['images'][0]})
        result.extend({'kind': 'text', 'text': part} for part in chunks)
        result.extend({'kind': 'photo', 'text': f"{message['name']} · {label}", 'reference': ref}
                      for ref in message['images'][1:])
    else:
        result.extend({'kind': 'text', 'text': part} for part in chunks)
    return result


class Store:
    def __init__(self, path, readonly=False):
        self.db = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True) if readonly else sqlite3.connect(path, timeout=0.1)
        try:
            self.initialize(path, readonly)
        except BaseException:
            self.db.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.db.close()

    def initialize(self, path, readonly):
        self.db.row_factory = sqlite3.Row
        version = self.db.execute('PRAGMA user_version').fetchone()[0]
        if version > SCHEMA_VERSION:
            self.db.close()
            raise BridgeError('База создана более новой версией программы')
        if readonly:
            return
        existing = self.db.execute("SELECT 1 FROM sqlite_master WHERE name='jobs'").fetchone()
        if existing and version < SCHEMA_VERSION and str(path) != ':memory:':
            backup_path = Path(str(path) + f'.v{version}-' + secrets.token_hex(6) + '.bak')
            backup = sqlite3.connect(backup_path)
            try:
                self.db.backup(backup)
            finally:
                backup.close()
            backup_path.chmod(0o600)
        # Durability settings are explicit and verified instead of relying on build defaults.
        mode = self.db.execute('PRAGMA journal_mode=WAL').fetchone()[0].lower()
        self.db.execute('PRAGMA synchronous=FULL')
        synchronous = self.db.execute('PRAGMA synchronous').fetchone()[0]
        if mode != ('memory' if str(path) == ':memory:' else 'wal') or synchronous != 2:
            raise BridgeError('SQLite не включил WAL и synchronous=FULL; запуск остановлен, база не изменена')
        self.db.executescript('''
            BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS inbox(
                platform TEXT, chat TEXT, mid TEXT, PRIMARY KEY(platform,chat,mid));
            CREATE TABLE IF NOT EXISTS jobs(
                id INTEGER PRIMARY KEY, source TEXT, chat TEXT, mid TEXT, destination TEXT,
                part INTEGER, payload TEXT, parent TEXT, state TEXT DEFAULT 'pending',
                remote_id TEXT, attempts INTEGER DEFAULT 0, ready REAL DEFAULT 0,
                error TEXT, created REAL, updated REAL, UNIQUE(source,chat,mid,part));
            CREATE TABLE IF NOT EXISTS quarantine(
                id INTEGER PRIMARY KEY, platform TEXT NOT NULL, chat TEXT NOT NULL,
                event_key TEXT NOT NULL, payload TEXT NOT NULL, error TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'open', attempts INTEGER NOT NULL DEFAULT 0,
                created REAL NOT NULL, updated REAL NOT NULL,
                UNIQUE(platform,chat,event_key));
            CREATE TABLE IF NOT EXISTS audit(
                id INTEGER PRIMARY KEY, scope TEXT NOT NULL, target_id INTEGER NOT NULL,
                action TEXT NOT NULL, old_state TEXT NOT NULL, new_state TEXT NOT NULL,
                reason TEXT NOT NULL, created REAL NOT NULL);
        ''')
        try:
            columns = {row['name'] for row in self.db.execute('PRAGMA table_info(jobs)')}
            for name in ('created', 'updated'):
                if name not in columns:
                    self.db.execute(f'ALTER TABLE jobs ADD COLUMN {name} REAL')
            self.db.execute(f'PRAGMA user_version={SCHEMA_VERSION}')
            self.db.commit()
        except Exception:
            self.db.rollback()
            self.db.close()
            raise

    def get(self, key):
        row = self.db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
        return row[0] if row else None

    def set(self, key, value):
        self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)', (key, str(value)))

    def has_table(self, name):
        return self.db.execute('SELECT 1 FROM sqlite_master WHERE type=\'table\' AND name=?', (name,)).fetchone() is not None

    def accept(self, platform, events, cursor, chat_id, own_id):
        # Cursor, valid jobs and quarantined inputs commit together. DB/programming
        # failures still roll back the whole batch; only expected input errors are isolated.
        with self.db:
            for event in events:
                if selected_message(platform, event, chat_id) is None:
                    continue
                try:
                    self.accept_one(platform, event, chat_id, own_id)
                except EventError as exc:
                    now = time.time()
                    self.db.execute('''INSERT OR IGNORE INTO quarantine
                        (platform,chat,event_key,payload,error,created,updated) VALUES(?,?,?,?,?,?,?)''',
                        (platform, str(chat_id), event_key(platform, event),
                         json.dumps(event, ensure_ascii=False), str(exc), now, now))
            if cursor is not None:
                self.set('cursor_' + platform, cursor)
            self.set('last_poll_success_' + platform, time.time())
            self.set('last_poll_error_' + platform, '')

    def accept_one(self, platform, event, chat_id, own_id):
        message = normalize(platform, event, chat_id, own_id)
        if message is not None:
            # Build all parts before inserting, so validation never leaves a partial message.
            parts = units(message)
            inserted = self.db.execute('INSERT OR IGNORE INTO inbox VALUES(?,?,?)',
                                      (platform, message['chat'], message['mid'])).rowcount
            if inserted:
                destination = 'max' if platform == 'tg' else 'tg'
                for i, part in enumerate(parts):
                    now = time.time()
                    self.db.execute('''INSERT INTO jobs(source,chat,mid,destination,part,payload,parent,created,updated)
                                      VALUES(?,?,?,?,?,?,?,?,?)''',
                                    (platform, message['chat'], message['mid'], destination,
                                     i, json.dumps(part, ensure_ascii=False), message['parent'], now, now))
        self.db.execute('''UPDATE quarantine SET state='resolved',updated=?
                           WHERE platform=? AND chat=? AND event_key=? AND state='open' ''',
                        (time.time(), platform, str(chat_id), event_key(platform, event)))

    def retry_quarantine(self, qid, chat_id, own_id, reason='Повторная обработка после проверки'):
        with self.db:
            row = self.db.execute('SELECT * FROM quarantine WHERE id=?', (qid,)).fetchone()
            if row is None:
                raise BridgeError('Событие карантина не найдено')
            if row['chat'] != str(chat_id):
                raise BridgeError('Событие принадлежит другому чату')
            if row['state'] != 'open':
                return row['state']
            event = json.loads(row['payload'])
            try:
                self.accept_one(row['platform'], event, chat_id, own_id)
            except EventError as exc:
                self.db.execute('UPDATE quarantine SET error=?,updated=? WHERE id=?',
                                (str(exc), time.time(), qid))
            self.db.execute('UPDATE quarantine SET attempts=attempts+1 WHERE id=?', (qid,))
            state = self.db.execute('SELECT state FROM quarantine WHERE id=?', (qid,)).fetchone()[0]
            self.audit('quarantine', qid, 'retry', row['state'], state, reason)
        return self.db.execute('SELECT state FROM quarantine WHERE id=?', (qid,)).fetchone()[0]

    def audit(self, scope, target_id, action, old_state, new_state, reason):
        self.db.execute('''INSERT INTO audit
            (scope,target_id,action,old_state,new_state,reason,created) VALUES(?,?,?,?,?,?,?)''',
            (scope, target_id, action, old_state, new_state, reason, time.time()))

    def identity(self):
        try:
            ids = json.loads(self.get('identity') or 'null')
        except ValueError:
            ids = None
        if not isinstance(ids, list) or len(ids) != 4 or not all(isinstance(v, str) and v for v in ids):
            raise BridgeError('В базе нет проверенной привязки ботов и чатов; сначала выполните run')
        return {'tg': (ids[0], ids[2]), 'max': (ids[1], ids[3])}

    def manage_job(self, jid, action, reason, remote_id=None, accept_duplicate_risk=False, remaining=False):
        if not reason or not reason.strip() or len(reason) > 1000:
            raise BridgeError('Укажите причину действия длиной от 1 до 1000 символов')
        with self.db:
            # The CLI also owns process.lock: no publisher may be active during maintenance.
            job = self.db.execute('SELECT * FROM jobs WHERE id=?', (jid,)).fetchone()
            if job is None:
                raise BridgeError('Задача не найдена')
            old = job['state']
            if old == 'sending':
                raise BridgeError('Нельзя менять активно отправляемую задачу')
            if action == 'retry':
                if old == 'pending':
                    return old
                if old not in ('failed', 'uncertain'):
                    raise BridgeError('Повтор разрешён только для failed или uncertain')
                if old == 'uncertain' and not accept_duplicate_risk:
                    raise BridgeError('Результат отправки неизвестен; после сверки нужен --accept-duplicate-risk')
                self.db.execute("UPDATE jobs SET state='pending',error=NULL,ready=0,updated=? WHERE id=?", (time.time(), jid))
                state = 'pending'
            elif action == 'confirm':
                if (not isinstance(remote_id, str) or not remote_id or len(remote_id) > 256
                        or any(c.isspace() or not c.isprintable() for c in remote_id)
                        or any(c in remote_id for c in '/?&#')):
                    raise BridgeError('Укажите корректный ID опубликованной копии')
                if job['destination'] == 'tg' and (not remote_id.isascii() or not remote_id.isdecimal() or int(remote_id) <= 0):
                    raise BridgeError('ID копии Telegram должен быть положительным целым числом')
                if job['destination'] == 'tg':
                    remote_id = str(int(remote_id))
                if old == 'sent' and job['remote_id'] == remote_id:
                    return old
                if old != 'uncertain':
                    raise BridgeError('Подтверждать копию можно только для uncertain')
                duplicate = self.db.execute('SELECT 1 FROM jobs WHERE destination=? AND remote_id=? AND id!=?',
                                            (job['destination'], remote_id, jid)).fetchone()
                if duplicate:
                    raise BridgeError('Этот ID копии уже связан с другой задачей')
                self.db.execute("UPDATE jobs SET state='sent',remote_id=?,error=NULL,ready=0,updated=? WHERE id=?", (remote_id, time.time(), jid))
                state = 'sent'
            elif action == 'skip':
                if old not in ('pending', 'failed', 'uncertain', 'skipped'):
                    raise BridgeError('Нельзя пропустить уже доставленную задачу')
                rows = [job]
                if remaining:
                    rows = self.db.execute('''SELECT * FROM jobs WHERE source=? AND chat=? AND mid=?
                        AND part>=? AND state NOT IN ('sent','skipped') ORDER BY part''',
                        (job['source'], job['chat'], job['mid'], job['part'])).fetchall()
                    if any(row['state'] == 'sending' for row in rows):
                        raise BridgeError('Одна из частей сообщения сейчас отправляется')
                for row in rows:
                    if row['state'] != 'skipped':
                        self.db.execute("UPDATE jobs SET state='skipped',error=NULL,ready=0,updated=? WHERE id=?", (time.time(), row['id']))
                        self.audit('job', row['id'], action, row['state'], 'skipped', reason)
                return 'skipped'
            else:
                raise BridgeError('Неизвестное действие очереди')
            self.audit('job', jid, action, old, state, reason)
        return state

    def parent_id(self, job):
        if not job['parent']:
            return None
        # Reply to a mirrored message: original is already local at the destination.
        row = self.db.execute('''SELECT mid FROM jobs WHERE source=? AND destination=?
                                 AND remote_id=? AND state='sent' ORDER BY id LIMIT 1''',
                              (job['destination'], job['source'], job['parent'])).fetchone()
        if row:
            return row[0]
        row = self.db.execute('''SELECT remote_id FROM jobs WHERE source=? AND chat=? AND mid=?
                                 AND state='sent' ORDER BY part LIMIT 1''',
                              (job['source'], job['chat'], job['parent'])).fetchone()
        return row[0] if row else None

    def update(self, jid, state, error=None, remote_id=None, delay=0, counted=False):
        # counted: start_sending already counted this attempt as a publication.
        with self.db:
            now = time.time()
            self.db.execute('''UPDATE jobs SET state=?,error=?,remote_id=?,ready=?,attempts=attempts+?,updated=?
                               WHERE id=?''', (state, error, remote_id, now + delay, 0 if counted else 1, now, jid))
            if state == 'sent':
                row = self.db.execute('SELECT destination FROM jobs WHERE id=?', (jid,)).fetchone()
                self.set('last_delivery_success_' + row['destination'], now)

    def start_sending(self, job):
        """Claim the head once, after preparation and immediately before publication."""
        with self.db:
            now = time.time()
            return self.db.execute('''UPDATE jobs SET state='sending',error=NULL,
                remote_id=NULL,ready=?,attempts=attempts+1,updated=?
                WHERE id=? AND state='pending' AND ready<=? AND id=(
                    SELECT min(id) FROM jobs WHERE destination=?
                    AND state NOT IN ('sent','skipped'))''',
                (now, now, job['id'], now, job['destination'])).rowcount == 1

    def recover(self):
        with self.db:
            self.db.execute("UPDATE jobs SET state='uncertain',error='interrupted: Процесс остановился при отправке',updated=? WHERE state='sending'", (time.time(),))

    def runtime(self, state, run_id=None):
        with self.db:
            now = time.time()
            self.set('runtime_state', state)
            self.set('heartbeat', now)
            if state == 'running':
                self.set('run_started', now)
                self.set('run_id', run_id or '')
                # Telemetry of a previous run never describes the workers of this one.
                self.set('workers', json.dumps({'run_id': run_id, 'workers': {}}))
            else:
                self.set('run_stopped', now)

    def heartbeat(self, workers=None):
        with self.db:
            self.set('heartbeat', time.time())
            if workers is not None:
                self.set('workers', json.dumps({'run_id': self.get('run_id'), 'workers': workers}))

    def poll_failure(self, platform, error):
        with self.db:
            self.set('last_poll_error_' + platform, str(error))
            self.set('last_poll_failure_' + platform, time.time())


def publication(destination, target, part, parent, photo):
    """Build the one request that can publish a visible message; no I/O or state."""
    if destination == 'tg':
        payload = {'chat_id': target}
        if parent:
            payload['reply_parameters'] = {'message_id': int(parent), 'allow_sending_without_reply': True}
        if photo:
            data, mime, filename = photo
            payload['caption'] = part['text']
            return 'tg', 'sendPhoto', dict(method='POST',
                upload=multipart(payload, 'photo', data, filename, mime))
        payload.update(text=part['text'], link_preview_options={'is_disabled': True})
        return 'tg', 'sendMessage', dict(method='POST', body=payload)
    payload = {'text': part['text']}
    if photo:
        payload['attachments'] = [{'type': 'image', 'payload': photo}]
    if parent:
        payload['link'] = {'type': 'reply', 'mid': parent}
    return 'max', '/messages', dict(method='POST', query={'chat_id': target}, body=payload)


class Bridge:
    def __init__(self, cfg, api, store, ids, *, send_intervals=None):
        self.cfg, self.api, self.store, self.ids = cfg, api, store, ids
        self.paused = set()
        self.random = random.random
        self.prepared = {}
        self.send_intervals = {'tg': 3.2, 'max': 0.6} if send_intervals is None else send_intervals
        # In-memory progress of each poller/sender; the heartbeat persists it and never reports progress.
        self.workers = {}
        total = getattr(getattr(api, 'timeout', None), 'total', None) or 25
        # Per HTTP request: an operation stays healthy while each of its requests starts in time.
        self.budgets = {'poll': total + 5, 'deliver': total + 5}

    def phase(self, worker, phase, until=None, reason=None):
        now = time.time()
        entry = {'phase': phase, 'since': now, 'until': until, 'reason': reason, 'progress_at': now}
        self.workers[worker] = entry
        WORKER.set((entry, self.budgets[phase]) if phase in self.budgets else None)

    def resumed(self, last, now, interval):
        """The event loop was frozen between two heartbeats (SIGSTOP, host suspend)."""
        # Frozen time does not count against a deadline that lived through the freeze, so a healthy worker
        # is not stalled on resume and a hung one stays stalled. The gap is > 2*interval + 1 s, so progress
        # before the midpoint preceded the freeze; later progress (a worker that ran before this heartbeat)
        # already has a fresh budget.
        boundary = (last + now) / 2
        for entry in self.workers.values():
            if entry['until'] is not None and entry['progress_at'] < boundary:
                entry['until'] += now - last - interval

    async def poll(self, platform):
        try:
            cursor = self.store.get('cursor_' + platform)
            if platform == 'tg':
                events = await self.api.request('tg', 'getUpdates', method='POST', body={
                    'offset': int(cursor) if cursor is not None else None,
                    'timeout': 0, 'allowed_updates': ['message']})
                next_cursor = max(e['update_id'] for e in events) + 1 if events else cursor
            else:
                result = await self.api.request('max', '/updates', query={
                    'marker': cursor, 'timeout': 0, 'types': 'message_created', 'limit': 100})
                events, next_cursor = result.get('updates', []), result.get('marker')
            self.store.accept(platform, events, next_cursor, self.cfg[platform.upper() + '_CHAT_ID'], self.ids[platform])
        except BridgeError as exc:
            self.store.poll_failure(platform, error_text(*error_info(exc), 'опрос не выполнен'))
            raise

    async def deliver(self, destination, stop=None):
        # Keep ordering; uncertain/failed publication blocks this destination until reviewed.
        job = self.store.db.execute("SELECT * FROM jobs WHERE destination=? AND state NOT IN ('sent','skipped') ORDER BY id LIMIT 1",
                                    (destination,)).fetchone()
        if not job or job['state'] != 'pending' or job['ready'] > time.time():
            return
        next_send = float(self.store.get('next_' + destination) or 0)
        if time.time() < next_send:
            return
        part = json.loads(job['payload'])
        if destination in self.paused or (part['kind'] == 'photo' and job['source'] in self.paused):
            return
        target = self.cfg[destination.upper() + '_CHAT_ID']
        parent = self.store.parent_id(job) if job['part'] == 0 else None
        publishing = False
        started = time.monotonic()
        cached = self.prepared.pop(job['id'], None)
        expires, photo = cached if cached and cached[0] > time.monotonic() else (0, None)

        def finish(state, exc, delay=0, code=None, message=None):
            status = None
            if code is None:
                code, status = error_info(exc)
            self.store.update(job['id'], state, error_text(code, status, message or str(exc)), delay=delay,
                              counted=publishing)
            level, event = {'pending': ('warning', 'delivery_retry_scheduled'), 'failed': ('error', 'delivery_failed'),
                            'uncertain': ('error', 'delivery_uncertain')}[state]
            log(level, event, platform=destination, direction=f"{job['source']}>{destination}", job_id=job['id'],
                part=job['part'], state=state, phase='publish' if publishing else 'prepare',
                attempt=job['attempts'] + 1, error_code=code, http_status=status,
                retry_at=time.time() + delay if state == 'pending' else None,
                delay_seconds=delay if state == 'pending' else None,
                duration_ms=int((time.monotonic() - started) * 1000))

        try:
            if part['kind'] == 'photo' and photo is None:
                photo = await self.api.prepare_photo(job['source'], part['reference'], destination)
                expires = time.monotonic() + ATTACHMENT_REUSE
            if (stop is not None and stop.is_set()) or destination in self.paused:
                return
            service, path, kwargs = publication(destination, target, part, parent, photo)
            # Commit before the first request that may publish a visible message.
            if not self.store.start_sending(job):
                return
            publishing = True
            result = await self.api.request(service, path, **kwargs)
            remote_id = result['message_id'] if destination == 'tg' else result['message']['body']['mid']
            if (not isinstance(remote_id, (str, int)) or isinstance(remote_id, bool)
                    or not str(remote_id) or (destination == 'tg' and
                    (not str(remote_id).isdigit() or int(remote_id) <= 0))):
                raise TransportError('API не вернул корректный ID опубликованного сообщения')
            remote_id = str(remote_id)
            self.store.update(job['id'], 'sent', remote_id=remote_id, counted=True)
            log('info', 'delivery_sent', platform=destination, direction=f"{job['source']}>{destination}",
                job_id=job['id'], part=job['part'], state='sent', phase='publish', attempt=job['attempts'] + 1,
                duration_ms=int((time.monotonic() - started) * 1000))
        except asyncio.CancelledError:
            if publishing:
                finish('uncertain', None, code='cancelled', message='Отмена во время публикации; требуется сверка')
            raise
        except ApiError as exc:
            # A definitive rejection is safe to retry. A server 5xx may be post-publication.
            retry = max(exc.retry_after, self.backoff(job))
            server_error = server_failure(exc)
            if publishing and server_error:
                # HTTP 5xx after the request may have published wins over a retryable body code.
                finish('uncertain', exc)
            elif exc.code in ('429', 'attachment.not.ready') or server_error:
                if publishing and isinstance(photo, dict):
                    # Reuse the uploaded MAX attachment instead of uploading again.
                    self.prepared[job['id']] = (expires, photo)
                finish('pending', exc, retry)
            else:
                finish('failed', exc)
        except TransportError as exc:
            # ConnectError proves the request was not sent, even during publication.
            if publishing and not isinstance(exc, ConnectError):
                finish('uncertain', exc)
            else:
                finish('pending', exc, self.backoff(job))
        except (KeyError, TypeError, ValueError):
            finish('uncertain' if publishing else 'failed', None, code='bad_response',
                   message='Неожиданный формат ответа API')
        except BridgeError as exc:
            finish('failed', exc)
        finally:
            with self.store.db:
                self.store.set('next_' + destination, time.time() + self.send_intervals[destination])

    def backoff(self, job):
        # attempts counts each preparation or publication attempt of this queue head once.
        return backoff(job['attempts'] + 1, *SEND_BACKOFF, rand=self.random)


async def run_bridge(bridge, stop=None, *, poll_interval=1, delivery_interval=0.2,
                     heartbeat_interval=2, shutdown_timeout=30):
    """One owner per poller/sender; expected service failures stay in that worker."""
    install_signals = stop is None
    stop = stop if stop is not None else asyncio.Event()
    loop = asyncio.get_running_loop()

    async def wait(delay):
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except TimeoutError:
            pass

    async def poller(platform):
        failures = 0
        while not stop.is_set():
            if platform in bridge.paused:
                await stop.wait()
                return
            delay = poll_interval
            worker = 'poll:' + platform
            gate = getattr(bridge, 'storage_gate', None)
            if gate is not None and not gate.allow_poll():
                bridge.phase(worker, 'storage_wait', time.time() + gate.policy.check_interval, reason='storage_pressure')
                await wait(gate.policy.check_interval)
                continue
            bridge.phase(worker, 'poll', time.time() + bridge.budgets['poll'])
            try:
                await bridge.poll(platform)
                if failures:
                    log('info', 'poll_recovered', platform=platform, worker=worker, count=failures)
                failures = 0
            except BridgeError as exc:
                failures += 1
                code, status = error_info(exc)
                if isinstance(exc, ApiError) and exc.code == '409':
                    log('error', 'poll_conflict', platform=platform, worker=worker, error_code=code, http_status=status)
                    raise BridgeError(f'{platform}: конфликт другого poller; мост остановлен') from None
                delay = backoff(failures, *POLL_BACKOFF, rand=bridge.random)
                if isinstance(exc, ApiError):
                    delay = max(delay, exc.retry_after)
                if (isinstance(exc, ApiError) and exc.code in ('401', '403')) or isinstance(exc, CertificateError):
                    bridge.paused.add(platform)
                    bridge.phase(worker, 'paused', reason=code)
                    log('error', 'platform_paused', platform=platform, worker=worker, error_code=code,
                        http_status=status, message='Приём/отправка приостановлены; проверьте права/сертификат и перезапустите мост')
                    continue
                if failures == 1 or failures % 30 == 0:
                    log('warning', 'poll_failed', platform=platform, worker=worker, attempt=failures, error_code=code,
                        http_status=status, delay_seconds=delay, retry_at=time.time() + delay)
            bridge.phase(worker, 'wait', time.time() + delay)
            await wait(delay)

    async def sender(platform):
        worker = 'send:' + platform
        while not stop.is_set():
            bridge.phase(worker, 'deliver', time.time() + bridge.budgets['deliver'])
            await bridge.deliver(platform, stop)
            bridge.phase(worker, 'wait', time.time() + delivery_interval)
            await wait(delivery_interval)

    async def heartbeat():
        last = None
        while not stop.is_set():
            now = time.time()
            if last is not None and now - last > 2 * heartbeat_interval + 1:
                bridge.resumed(last, now, heartbeat_interval)
            last = now
            bridge.store.heartbeat(bridge.workers)
            await wait(heartbeat_interval)

    if install_signals:
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, stop.set)
    RUN['id'] = RUN['id'] or secrets.token_hex(8)
    try:
        bridge.store.runtime('running', RUN['id'])
        log('info', 'bridge_started')
        try:
            async with asyncio.TaskGroup() as group:
                workers = [group.create_task(poller(p), name='poll:' + p) for p in ('tg', 'max')]
                workers += [group.create_task(sender(p), name='send:' + p) for p in ('tg', 'max')]
                workers.append(group.create_task(heartbeat(), name='heartbeat'))
                await stop.wait()
                _, pending = await asyncio.wait(workers, timeout=shutdown_timeout)
                for task in pending:
                    task.cancel()
        except BaseExceptionGroup as group_error:
            error = group_error
            while isinstance(error, BaseExceptionGroup):
                error = error.exceptions[0]
            raise error from None
    finally:
        if install_signals:
            for signum in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(signum)
        bridge.store.runtime('stopped', RUN['id'])


async def doctor(cfg, api, chats=False):
    tg = await api.request('tg', 'getMe')
    mx = await api.request('max', '/me')
    ids = {'tg': str(tg['id']), 'max': str(mx['user_id'])}
    for platform, bot in [('tg', tg), ('max', mx)]:
        expected = cfg.get(platform.upper() + '_BOT_USERNAME', '').lstrip('@').lower()
        if expected and (bot.get('username') or '').lower() != expected:
            raise BridgeError(f'{platform}: токен относится к другому боту; проверьте ожидаемый username')
        if RUN['component'] is None:
            print(f"{platform}: bot @{bot.get('username', '')}, id={ids[platform]}")
    webhook = await api.request('tg', 'getWebhookInfo')
    subscriptions = (await api.request('max', '/subscriptions')).get('subscriptions', [])
    if webhook.get('url') or subscriptions:
        raise BridgeError('У бота есть webhook-подписка. Нужна проверка перед переходом на тестовый polling; автоматически не отключаю.')
    if RUN['component'] is None:
        print('Webhook-подписок нет; тестовый polling доступен.')
    if chats:
        tg_chat = await api.request('tg', 'getChat', query={'chat_id': cfg['TG_CHAT_ID']})
        mx_chat = await api.request('max', '/chats/' + cfg['MAX_CHAT_ID'])
        if tg_chat.get('type') not in ('group', 'supergroup') or mx_chat.get('type') != 'chat':
            raise BridgeError('Для PoC разрешены только групповые чаты')
        member = await api.request('tg', 'getChatMember', query={'chat_id': cfg['TG_CHAT_ID'], 'user_id': ids['tg']})
        if member.get('status') not in ('administrator', 'creator') and not tg.get('can_read_all_group_messages'):
            raise BridgeError('Telegram: нужен отключённый Privacy Mode или роль администратора')
        if member.get('status') in ('left', 'kicked', 'restricted'):
            raise BridgeError('Telegram: бот отсутствует или ограничен; проверьте разрешения')
        permissions = tg_chat.get('permissions') or {}
        if member.get('status') not in ('administrator', 'creator') and (
                permissions.get('can_send_messages') is False or permissions.get('can_send_photos') is False):
            raise BridgeError('Telegram: в группе запрещена отправка текста или фото')
        mx_member = await api.request('max', '/chats/' + cfg['MAX_CHAT_ID'] + '/members/me')
        # In groups `write` grants moderation (edit/delete), not ordinary sending.
        # https://dev.max.ru/docs-api/objects/ChatMember
        if not mx_member.get('is_admin') or 'read_all_messages' not in (mx_member.get('permissions') or []):
            raise BridgeError('MAX: нужны роль администратора и право read_all_messages')
        if RUN['component'] is None:
            print(f"Чаты проверены: Telegram {cfg['TG_CHAT_ID']}; MAX {cfg['MAX_CHAT_ID']}")
    if RUN['component'] is not None:
        log('info', 'preflight_ok')
    return ids


async def discover(cfg, api, seconds, state=STATE):
    await doctor(cfg, api)
    code_file = state / 'discovery-code'
    if not code_file.exists():
        code_file.write_text('SYNC-CHECK-' + secrets.token_hex(3).upper())
    code = code_file.read_text().strip()
    print(f'Отправьте в обе тестовые группы сообщение: {code}', flush=True)
    found, tg_cursor, mx_cursor = {}, None, None
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and len(found) < 2:
        tg_events = await api.request('tg', 'getUpdates', method='POST', body={'offset': tg_cursor, 'timeout': 0, 'allowed_updates': ['message']})
        for event in tg_events:
            msg = event.get('message') or {}
            if msg.get('text') == code and msg.get('chat', {}).get('type') in ('group', 'supergroup'):
                found['tg'] = str(msg['chat']['id'])
        if tg_events:
            tg_cursor = max(e['update_id'] for e in tg_events) + 1
        mx = await api.request('max', '/updates', query={'marker': mx_cursor, 'timeout': 0, 'types': 'message_created'})
        mx_cursor = mx.get('marker')
        for event in mx.get('updates', []):
            msg = event.get('message') or {}
            if (msg.get('body') or {}).get('text') == code and msg.get('recipient', {}).get('chat_type') == 'chat':
                found['max'] = str(msg['recipient']['chat_id'])
        if found:
            (state / 'discovered.json').write_text(json.dumps(found))
        await asyncio.sleep(1)
    for platform, chat in found.items():
        print(f'{platform}: test_chat_id={chat}')
    if len(found) != 2:
        raise BridgeError('Не обнаружены обе группы. Проверьте права и повторите команду discover.')
    print('Обе группы найдены. Команда bind подключит только эти группы.')


def bind(state=STATE):
    found = json.loads((state / 'discovered.json').read_text())
    if set(found) != {'tg', 'max'}:
        raise BridgeError('Сначала обнаружьте обе группы через discover')
    lines = ENV.read_text().splitlines()
    for key, value in [('TG_CHAT_ID', found['tg']), ('MAX_CHAT_ID', found['max'])]:
        prefix = key + '='
        lines = [line for line in lines if not line.startswith(prefix)] + [prefix + str(int(value))]
    ENV.write_text('\n'.join(lines) + '\n')
    ENV.chmod(0o600)
    print('ID двух тестовых групп записаны в .env; сообщения не отправлены.')


@contextmanager
def process_lock(state, name='process.lock'):
    with (state / name).open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            if name != 'process.lock':
                raise BridgeError('Другой монитор уже использует этот каталог состояния') from None
            raise BridgeError('Другой процесс использует прототип; остановите его перед изменением очереди') from None
        yield


def bridge_running(state):
    try:
        with (state / 'process.lock').open('r') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
    except FileNotFoundError:
        pass
    return False


def health_snapshot(store, now=None, running=False, stale_after=180, queue_age=300):
    now = time.time() if now is None else now
    result = {'ok': False, 'generated_at': now, 'schema_version': None,
              'thresholds': {'stale_after_seconds': stale_after, 'queue_age_seconds': queue_age},
              'runtime': {}, 'platforms': {}, 'issues': [], 'warnings': [], 'storage': {}}
    issues = result['issues']

    def issue(key, code, **details):
        issues.append({'key': key, 'code': code, **details})

    if store is None:
        issue('state', 'state_missing')
        return result

    def timestamp(key):
        try:
            value = float(store.get(key))
            return value if math.isfinite(value) else None
        except (TypeError, ValueError):
            return None

    def age(value):
        return max(0, now - value) if value is not None else None

    version = store.db.execute('PRAGMA user_version').fetchone()[0]
    result['schema_version'] = version
    if version < SCHEMA_VERSION:
        issue('schema', 'migration_required')
    heartbeat = timestamp('heartbeat')
    started = timestamp('run_started')
    state = store.get('runtime_state') or 'unknown'
    result['runtime'] = {'state': state, 'process_running': running, 'heartbeat_at': heartbeat,
                         'heartbeat_age_seconds': age(heartbeat), 'started_at': started,
                         'stopped_at': timestamp('run_stopped'), 'run_id': store.get('run_id')}
    if not running or state == 'stopped':
        issue('runtime', 'process_stopped')
    elif heartbeat is None or age(heartbeat) > stale_after:
        issue('runtime', 'heartbeat_stale')
    result['workers'] = {}
    try:
        telemetry = json.loads(store.get('workers') or 'null')
    except ValueError:
        telemetry = None
    # A stopped event loop is reported once as heartbeat_stale, not as every worker stalling.
    fresh = heartbeat is not None and age(heartbeat) <= stale_after
    if (running and state == 'running' and fresh and isinstance(telemetry, dict)
            and telemetry.get('run_id') == result['runtime']['run_id'] and isinstance(telemetry.get('workers'), dict)):
        for name in WORKERS:
            entry = telemetry['workers'].get(name)
            if not isinstance(entry, dict):
                continue
            phase = entry.get('phase') if isinstance(entry.get('phase'), str) and SAFE_VALUE.fullmatch(entry['phase']) else 'unknown'
            reason = entry.get('reason') if entry.get('reason') in ERROR_CODES else None
            since, until = (v if isinstance(v, (int, float)) and math.isfinite(v) else None
                            for v in (entry.get('since'), entry.get('until')))
            platform = name.split(':')[1]
            # A phase is stalled only well after its own deadline (operation timeout or planned wake-up).
            worker_state = ('paused' if phase == 'paused' else
                            'stalled' if until is not None and now > until + stale_after else 'ok')
            result['workers'][name] = {'phase': phase, 'state': worker_state, 'since': since, 'until': until,
                                       'reason': reason}
            if worker_state == 'stalled':
                issue('worker:' + name, 'worker_stalled', platform=platform, worker=name, phase=phase)
            elif worker_state == 'paused':
                issue('paused:' + platform, 'platform_paused', platform=platform, reason=reason)
    columns = {row['name'] for row in store.db.execute('PRAGMA table_info(jobs)')}
    for platform in ('tg', 'max'):
        success = timestamp('last_poll_success_' + platform)
        failure = timestamp('last_poll_failure_' + platform)
        poll_error = bool(store.get('last_poll_error_' + platform))
        warming_up = success is None and started is not None and age(started) <= stale_after
        poll_state = 'error' if poll_error else 'starting' if warming_up else 'ok'
        if poll_error:
            issue('poll:' + platform, 'poll_error', platform=platform,
                  reason=error_code(store.get('last_poll_error_' + platform)))
        elif not warming_up and (success is None or age(success) > stale_after):
            poll_state = 'stale'
            issue('poll:' + platform, 'poll_stale', platform=platform)
        counts = {row['state']: row['n'] for row in store.db.execute(
            'SELECT state,count(*) AS n FROM jobs WHERE destination=? GROUP BY state', (platform,))}
        head = store.db.execute('''SELECT id,part,state,ready,attempts,error FROM jobs WHERE destination=?
            AND state NOT IN ('sent','skipped') ORDER BY id LIMIT 1''', (platform,)).fetchone()
        head_view = None
        if head:
            ready = head['ready'] if isinstance(head['ready'], (int, float)) else None
            waiting = (head['state'] == 'pending' and ready is not None and ready > now)
            head_view = {'id': head['id'], 'part': head['part'], 'state': head['state'], 'ready': head['ready'],
                         'attempts': head['attempts'], 'reason': error_code(head['error']),
                         'retry_at': ready if waiting else None,
                         'wait_seconds': ready - now if waiting else None,
                         'wait': 'retry' if waiting else 'manual' if head['state'] in ('failed', 'uncertain') else None}
        pending_age = unresolved_age = None
        unknown_age = 0
        if 'created' in columns:
            ages = store.db.execute('''SELECT
                min(CASE WHEN state='pending' THEN created END) AS pending,
                min(created) AS unresolved,
                sum(CASE WHEN created IS NULL THEN 1 ELSE 0 END) AS unknown
                FROM jobs WHERE destination=? AND state NOT IN ('sent','skipped')''', (platform,)).fetchone()
            pending_age, unresolved_age = age(ages['pending']), age(ages['unresolved'])
            unknown_age = ages['unknown'] or 0
        else:
            unknown_age = sum(n for key, n in counts.items() if key not in ('sent', 'skipped'))
        if head and head['state'] in ('failed', 'uncertain'):
            issue('blocked:' + platform, 'delivery_blocked', platform=platform, job_id=head['id'], state=head['state'],
                  reason=head_view['reason'])
        if unresolved_age is not None and unresolved_age > queue_age:
            issue('backlog:' + platform, 'queue_too_old', platform=platform)
        if unknown_age:
            issue('queue-age:' + platform, 'queue_age_unknown', platform=platform, count=unknown_age)
        q = None
        if store.has_table('quarantine'):
            q = store.db.execute("SELECT count(*) AS n,min(id) AS first FROM quarantine WHERE platform=? AND state='open'", (platform,)).fetchone()
        if q and q['n']:
            issue('quarantine:' + platform, 'input_quarantine', platform=platform, count=q['n'], first_id=q['first'])
        result['platforms'][platform] = {
            'poll': {'state': poll_state, 'last_success_at': success, 'last_failure_at': failure,
                     'success_age_seconds': age(success)},
            'delivery': {'last_success_at': timestamp('last_delivery_success_' + platform),
                         'counts': counts, 'head': head_view,
                         'oldest_pending_age_seconds': pending_age,
                         'oldest_unresolved_age_seconds': unresolved_age,
                         'unknown_age_count': unknown_age},
            'quarantine_open': q['n'] if q else 0}
    result['ok'] = not issues
    return result


def read_health(state, stale_after=180, queue_age=300, storage_policy=None):
    path = state / 'bridge.sqlite3'
    store = None
    try:
        if not path.exists():
            result = health_snapshot(None, stale_after=stale_after, queue_age=queue_age)
        else:
            store = Store(path, readonly=True)
            store.db.execute('BEGIN')
            result = health_snapshot(store, running=bridge_running(state), stale_after=stale_after, queue_age=queue_age)
    except (OSError, sqlite3.Error, BridgeError):
        result = health_snapshot(None, stale_after=stale_after, queue_age=queue_age)
        result['issues'] = [{'key': 'state', 'code': 'state_unavailable'}]
    finally:
        if store is not None:
            store.db.close()
    result['storage'] = storage.snapshot(state, storage_policy or storage_settings())
    result['issues'].extend(result['storage']['issues'])
    result['warnings'].extend(result['storage']['warnings'])
    result['storage']['intake_paused'] = any(w.get('phase') == 'storage_wait' for w in result.get('workers', {}).values())
    result['ok'] = not result['issues']
    return result


def print_health(snapshot):
    print('health=' + ('ok' if snapshot['ok'] else 'unhealthy'))
    runtime = snapshot['runtime']
    print(f"process={runtime.get('state', 'unknown')} running={runtime.get('process_running', False)} heartbeat_age={runtime.get('heartbeat_age_seconds')}")
    for platform, data in snapshot['platforms'].items():
        delivery = data['delivery']
        counts = ' '.join(f'{key}={value}' for key, value in sorted(delivery['counts'].items())) or 'empty'
        print(f"{platform}: poll={data['poll']['state']} last_poll={data['poll']['last_success_at']} last_delivery={delivery['last_success_at']} {counts} oldest_pending_age={delivery['oldest_pending_age_seconds']} oldest_unresolved_age={delivery['oldest_unresolved_age_seconds']} quarantine={data['quarantine_open']}")
    for name, worker in snapshot.get('workers', {}).items():
        print(f"worker {name}: state={worker['state']} phase={worker['phase']} until={worker['until']} reason={worker['reason']}")
    for platform, data in snapshot['platforms'].items():
        head = data['delivery']['head']
        if head:
            print(f"{platform}: head job={head['id']} part={head['part']} state={head['state']} reason={head['reason']} wait={head['wait']} retry_at={head['retry_at']}")
    if snapshot.get('storage'):
        disk = snapshot['storage']
        print(f"storage={disk['state']} managed_bytes={disk['managed_bytes']} free_bytes={disk['free_bytes']} intake_paused={disk['intake_paused']}")
    for issue in snapshot['issues']:
        print('issue ' + json.dumps(issue, ensure_ascii=False, sort_keys=True))
    for warning in snapshot.get('warnings', []):
        print('warning ' + json.dumps(warning, ensure_ascii=False, sort_keys=True))


class AlertMonitor:
    """Durable local alert outbox; no chat content or external messaging."""
    def __init__(self, path):
        self.db = sqlite3.connect(path, timeout=0.1)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS monitor_state(key TEXT PRIMARY KEY,value TEXT);
            CREATE TABLE IF NOT EXISTS alerts(id INTEGER PRIMARY KEY,event_id TEXT UNIQUE,
                payload TEXT NOT NULL,delivered INTEGER NOT NULL DEFAULT 0);
        ''')

    def observe(self, snapshot):
        current = {entry['key']: entry for entry in snapshot['issues'] + snapshot.get('warnings', [])}
        with self.db:
            row = self.db.execute("SELECT value FROM monitor_state WHERE key='issues'").fetchone()
            previous = json.loads(row[0]) if row else {}
            row = self.db.execute("SELECT value FROM monitor_state WHERE key='absent'").fetchone()
            absent = json.loads(row[0]) if row else {}
            if not snapshot['platforms']:
                # An unreadable/missing state is not evidence that known faults recovered.
                current = {**previous, **current}
            assessed = snapshot.get('workers') or {}
            for key, entry in previous.items():
                if key.startswith('storage:') and snapshot.get('storage', {}).get('state') == 'unknown':
                    current.setdefault(key, entry)
                # A worker that could not be observed (stale heartbeat, stopped process) has not recovered.
                worker = key[len('worker:'):] if key.startswith('worker:') else (
                    'poll:' + key[len('paused:'):] if key.startswith('paused:') else None)
                if worker and key not in current and worker not in assessed:
                    current[key] = entry
            known = dict(current)
            for key in sorted(set(current) | set(previous)):
                if key in current:
                    absent.pop(key, None)
                    if current[key] != previous.get(key):
                        self.alert('problem', snapshot['generated_at'], current[key])
                    continue
                # Recovery needs a confirming observation; until then the problem stays known.
                absent[key] = absent.get(key, 0) + 1
                if absent[key] >= RECOVERY_CONFIRMATIONS:
                    absent.pop(key)
                    self.alert('recovery', snapshot['generated_at'], previous[key])
                else:
                    known[key] = previous[key]
            self.db.execute("INSERT OR REPLACE INTO monitor_state VALUES('issues',?)", (json.dumps(known, sort_keys=True),))
            self.db.execute("INSERT OR REPLACE INTO monitor_state VALUES('absent',?)", (json.dumps(absent, sort_keys=True),))

    def alert(self, kind, moment, entry):
        # alerts.jsonl keeps event_id/kind/detected_at/issue; the log fields are additive.
        alert = {'event_id': secrets.token_hex(16), 'kind': kind, 'detected_at': moment, 'issue': entry,
                 'timestamp': utc(moment), 'level': ('warning' if entry.get('severity') == 'warning' else 'error') if kind == 'problem' else 'info',
                 'component': 'monitor', 'event': 'alert_' + kind, 'run_id': RUN['id']}
        self.db.execute('INSERT INTO alerts(event_id,payload) VALUES(?,?)',
                        (alert['event_id'], json.dumps(alert, ensure_ascii=False, sort_keys=True)))

    def flush(self, sink):
        count = 0
        for row in self.db.execute('SELECT id,payload FROM alerts WHERE delivered=0 ORDER BY id').fetchall():
            sink(json.loads(row['payload']))
            with self.db:
                self.db.execute('UPDATE alerts SET delivered=1 WHERE id=?', (row['id'],))
            count += 1
        return count


def emit_alert(alert, path=None):
    if path is not None:
        storage.AlertFile(path, storage_settings())(alert)
    else:
        print(json.dumps(alert, ensure_ascii=False, sort_keys=True), flush=True)


def monitor(state, args):
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    with process_lock(state, 'monitor.lock'):
        alerts = AlertMonitor(state / 'monitor.sqlite3')
        RUN['id'] = RUN['id'] or secrets.token_hex(8)
        log('info', 'monitor_started')
        stopping = False

        def stop(_signum, _frame):
            nonlocal stopping
            stopping = True

        previous_handlers = {sig: signal.signal(sig, stop) for sig in (signal.SIGTERM, signal.SIGINT)}
        try:
            sink = storage.AlertFile(args.alerts_file, args.storage) if args.alerts_file else None
            while not stopping:
                alerts.observe(read_health(state, args.stale_after, args.queue_age, args.storage))
                if sink is not None:
                    sink.maintain()
                alerts.flush(sink if sink is not None else emit_alert)
                if args.once:
                    return
                deadline = time.monotonic() + args.interval
                while not stopping and time.monotonic() < deadline:
                    time.sleep(min(0.25, max(0, deadline - time.monotonic())))
        finally:
            alerts.db.close()
            for sig, handler in previous_handlers.items():
                signal.signal(sig, handler)
            log('info', 'monitor_stopped')


def queue_view(store, command, action, target_id=None, limit=100):
    table = 'jobs' if command == 'queue' else 'quarantine'
    fields = ('id,source,chat,mid,destination,part,parent,state,remote_id,attempts,ready,error'
              if command == 'queue' else 'id,platform,chat,event_key,error,state,attempts,created,updated')
    if not store.has_table(table):
        return []
    if action == 'list':
        return [dict(row) for row in store.db.execute(f'SELECT {fields} FROM {table} ORDER BY id LIMIT ?', (limit,))]
    row = store.db.execute(f'SELECT {fields} FROM {table} WHERE id=?', (target_id,)).fetchone()
    if row is None:
        raise BridgeError('Задача или событие не найдены')
    result = dict(row)
    if command == 'queue':
        result['parts'] = [dict(part) for part in store.db.execute(
            'SELECT id,part,state,remote_id FROM jobs WHERE source=? AND chat=? AND mid=? ORDER BY part',
            (row['source'], row['chat'], row['mid']))]
    result['audit'] = []
    if store.has_table('audit'):
        scope = 'job' if command == 'queue' else 'quarantine'
        result['audit'] = [dict(entry) for entry in store.db.execute(
            'SELECT action,old_state,new_state,created FROM audit WHERE scope=? AND target_id=? ORDER BY id',
            (scope, target_id))]
    return result


def main(argv=None):
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['doctor', 'discover', 'bind', 'run', 'status', 'queue', 'quarantine', 'health', 'monitor'])
    parser.add_argument('action', nargs='?', choices=['list', 'show', 'retry', 'confirm', 'skip'])
    parser.add_argument('id', nargs='?', type=int)
    parser.add_argument('--seconds', type=int, default=90)
    parser.add_argument('--state-dir', type=Path, default=Path(os.environ.get('BRIDGE_STATE_DIR') or STATE))
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('--reason')
    parser.add_argument('--remote-id')
    parser.add_argument('--accept-duplicate-risk', action='store_true')
    parser.add_argument('--remaining', action='store_true')
    parser.add_argument('--json', action='store_true')
    parser.add_argument('--stale-after', type=float, default=180)
    parser.add_argument('--queue-age', type=float, default=300)
    parser.add_argument('--interval', type=float, default=30)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--alerts-file', type=Path)
    args = parser.parse_args(argv)
    if not all(math.isfinite(value) and value > 0 for value in (args.stale_after, args.queue_age, args.interval)):
        parser.error('Интервалы должны быть конечными положительными числами')
    if args.interval > 60:
        parser.error('--interval должен быть не более 60 секунд')
    if args.json and args.command not in ('status', 'health'):
        parser.error('--json разрешён только для status/health')
    if (args.once or args.alerts_file is not None) and args.command != 'monitor':
        parser.error('--once/--alerts-file разрешены только для monitor')
    maintenance = args.command in ('queue', 'quarantine')
    mutating = maintenance and args.action not in ('list', 'show')
    if maintenance:
        allowed = ('list', 'show', 'retry', 'confirm', 'skip') if args.command == 'queue' else ('list', 'show', 'retry')
        if args.action not in allowed:
            parser.error('Укажите допустимое действие очереди или карантина')
        if (args.action == 'list' and args.id is not None) or (args.action != 'list' and (args.id is None or args.id <= 0)):
            parser.error('Для действия нужен положительный ID; list выполняется без ID')
        if args.limit <= 0 or args.limit > 1000:
            parser.error('--limit должен быть от 1 до 1000')
        if mutating and (not args.reason or not args.reason.strip() or len(args.reason) > 1000):
            parser.error('Для изменения укажите --reason длиной от 1 до 1000 символов')
        if (args.remote_id is not None and args.action != 'confirm') or (args.action == 'confirm' and not args.remote_id):
            parser.error('--remote-id обязателен только для confirm')
        if args.remaining and (args.command != 'queue' or args.action != 'skip'):
            parser.error('--remaining разрешён только для queue skip')
        if args.accept_duplicate_risk and (args.command != 'queue' or args.action != 'retry'):
            parser.error('--accept-duplicate-risk разрешён только для queue retry')
    elif args.action is not None or args.id is not None or args.reason or args.remote_id or args.remaining or args.accept_duplicate_risk:
        parser.error('Действия и параметры очереди допустимы только для queue/quarantine')
    if args.command in ('run', 'monitor'):
        # Long-running commands write JSON Lines; the other CLI commands keep their human/JSON output.
        RUN['component'] = 'bridge' if args.command == 'run' else 'monitor'
        RUN['id'] = secrets.token_hex(8)
    args.storage = storage_settings() if args.command in ('run', 'monitor', 'status', 'health') else None
    state = args.state_dir
    if (args.alerts_file is not None and
            (args.alerts_file.name != 'alerts.jsonl' or args.alerts_file.parent.resolve() != state.resolve())):
        raise BridgeError('--alerts-file должен указывать на alerts.jsonl в выбранном --state-dir')
    db_path = state / 'bridge.sqlite3'
    if args.command == 'monitor':
        monitor(state, args)
        return
    if args.command in ('status', 'health'):
        snapshot = read_health(state, args.stale_after, args.queue_age, args.storage)
        if args.json:
            print(json.dumps(snapshot, ensure_ascii=False, indent=2))
        else:
            print_health(snapshot)
        return 0 if args.command == 'status' or snapshot['ok'] else 1
    if maintenance and not mutating:
        if not db_path.exists():
            if args.action == 'list':
                print('[]')
                return
            raise BridgeError('Очередь ещё не создана')
        store = Store(db_path, readonly=True)
        try:
            store.db.execute('BEGIN')
            print(json.dumps(queue_view(store, args.command, args.action, args.id, args.limit), ensure_ascii=False, indent=2))
        finally:
            store.db.close()
        return
    if mutating and not db_path.exists():
        raise BridgeError('Очередь ещё не создана')
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    with process_lock(state):
        if mutating:
            store = Store(db_path)
            try:
                store.recover()
                if args.command == 'queue':
                    result = store.manage_job(args.id, args.action, args.reason, args.remote_id,
                                              args.accept_duplicate_risk, args.remaining)
                else:
                    row = store.db.execute('SELECT platform FROM quarantine WHERE id=?', (args.id,)).fetchone()
                    if row is None:
                        raise BridgeError('Событие карантина не найдено')
                    own_id, chat_id = store.identity()[row['platform']]
                    result = store.retry_quarantine(args.id, chat_id, own_id, args.reason)
                print(f'{args.command} id={args.id} state={result}')
            finally:
                store.db.close()
            return
        if args.command == 'bind':
            bind(state)
            return
        return asyncio.run(network_command(args, state, db_path))


async def network_command(args, state, db_path):
    cfg = config(require_chats=args.command == 'run')
    async with Api(cfg) as api:
        if args.command == 'discover':
            await discover(cfg, api, args.seconds, state)
            return
        ids = await doctor(cfg, api, chats=bool(cfg.get('TG_CHAT_ID') and cfg.get('MAX_CHAT_ID')))
        if args.command == 'doctor':
            return
        with Store(db_path) as store:
            identity = json.dumps([ids['tg'], ids['max'], cfg['TG_CHAT_ID'], cfg['MAX_CHAT_ID']])
            if store.get('identity') not in (None, identity):
                raise BridgeError('База принадлежит другой паре ботов/чатов; используйте отдельное тестовое окружение')
            with store.db:
                store.set('identity', identity)
            store.recover()
            bridge = Bridge(cfg, api, store, ids)
            bridge.storage_gate = storage.Gate(state, getattr(args, 'storage', None) or storage.Settings())
            await run_bridge(bridge)
            log('info', 'bridge_stopped', message='Остановлено. Очередь сохранена.')



def report_failure(exc):
    """Exit message for a failed command: never a traceback, raw response or exception text of foreign code."""
    if isinstance(exc, (BridgeError, FileNotFoundError)):
        # BridgeError texts are fixed by this program; FileNotFoundError names only the local path.
        message = str(exc)
    elif isinstance(exc, (OSError, sqlite3.Error)):
        message = 'Ошибка локального хранилища или вывода; проверьте доступ и свободное место'
    else:
        message = 'Внутренняя ошибка: мост остановлен, очередь сохранена; требуется проверка версии'
    if RUN['component'] is None:
        print(message, file=sys.stderr)
    else:
        code = 'storage_error' if isinstance(exc, OSError) else error_info(exc)[0]
        log('error', 'stopped_with_error', message=message, stream=sys.stderr, error_code=code)
    return 1


if __name__ == '__main__':
    try:
        sys.exit(main() or 0)
    except Exception as exc:  # noqa: BLE001 - reported without details, see report_failure
        sys.exit(report_failure(exc))
