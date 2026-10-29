"""Async transport/runtime checks against a real local HTTP server; no live tokens."""
import asyncio
import os
from contextlib import redirect_stdout
import email.utils
import io
import gzip
import json
from pathlib import Path
import sqlite3
import ssl
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

import aiohttp
from aiohttp import web

import bridge as b
from smoke_p0 import tg, mx


def closed_port_url():
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()
    return f'http://127.0.0.1:{port}'


class HttpServer(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.calls = []
        self.handler = lambda request, body: web.json_response({'ok': True, 'result': {}})
        app = web.Application()
        app.router.add_route('*', '/{path:.*}', self.dispatch)
        self.runner = web.AppRunner(app, shutdown_timeout=0.2)
        await self.runner.setup()
        site = web.TCPSite(self.runner, '127.0.0.1', 0)
        await site.start()
        port = self.runner.addresses[0][1]
        self.url = f'http://127.0.0.1:{port}'
        self.addAsyncCleanup(self.runner.cleanup)

    async def dispatch(self, request):
        body = await request.read()
        self.calls.append((request.method, request.path, dict(request.headers), body))
        result = self.handler(request, body)
        return await result if asyncio.iscoroutine(result) else result

    async def api(self, timeout=None):
        api = b.Api({'TG_BOT_TOKEN': 'PRIVATE_TG', 'MAX_BOT_TOKEN': 'PRIVATE_MAX'},
                         tg_base=self.url, max_base=self.url, timeout=timeout)
        await api.__aenter__()
        self.addAsyncCleanup(api.__aexit__, None, None, None)
        return api

    def allow_local_media(self, url):
        if not url.startswith(self.url + '/'):
            self.original_media_check(url)


class HttpTests(HttpServer):

    async def test_post_connection_loss_is_not_retried_on_reused_connection(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                await response.read()

            def lose_response(request, body):
                request.transport.abort()
                return web.Response()

            self.handler = lose_response
            with self.assertRaises(aiohttp.ClientError):
                await session.post(self.url + '/publish', data=b'synthetic')
        self.assertEqual([c[0] for c in self.calls], ['GET', 'POST'])

    async def test_post_server_failure_is_not_retried(self):
        self.handler = lambda request, body: web.Response(status=503)
        async with aiohttp.ClientSession() as session:
            async with session.post(self.url, data=b'synthetic') as response:
                self.assertEqual(response.status, 503)
        self.assertEqual(len(self.calls), 1)

    async def test_api_json_authentication_and_error_redaction(self):
        api = await self.api()
        self.handler = lambda request, body: web.json_response({'message': {'body': {'mid': 'copy'}}})
        await api.request('max', '/messages', method='POST', query={'chat_id': '-200'}, body={'text': 'Привет'})
        self.assertEqual(self.calls[-1][2]['Authorization'], 'PRIVATE_MAX')
        self.assertEqual(json.loads(self.calls[-1][3])['text'], 'Привет')
        self.handler = lambda request, body: web.json_response({'ok': True, 'result': []})
        self.assertEqual(await api.request('tg', 'getUpdates', method='POST', body={'offset': 2}), [])
        self.assertNotIn('Authorization', self.calls[-1][2])
        self.handler = lambda request, body: web.json_response(
            {'code': 'PRIVATE_URL', 'description': 'PRIVATE_TOKEN', 'parameters': []}, status=403)
        with self.assertRaises(b.ApiError) as caught:
            await api.request('max', '/messages', method='POST')
        self.assertEqual(caught.exception.code, '403')
        self.assertNotIn('PRIVATE', str(caught.exception))

    async def test_api_rate_limit_and_attachment_not_ready(self):
        api = await self.api()
        self.handler = lambda request, body: web.json_response(
            {'ok': False, 'error_code': 429, 'parameters': {'retry_after': 120}}, status=429)
        with self.assertRaises(b.ApiError) as caught:
            await api.request('tg', 'sendMessage', method='POST')
        self.assertEqual((caught.exception.code, caught.exception.retry_after), ('429', 120))
        self.handler = lambda request, body: web.json_response({'code': 'attachment.not.ready'}, status=400)
        with self.assertRaises(b.ApiError) as caught:
            await api.request('max', '/messages', method='POST')
        self.assertEqual(caught.exception.code, 'attachment.not.ready')

    async def test_api_connection_loss_is_sanitized_and_not_retried(self):
        api = await self.api()

        def lose_response(request, body):
            request.transport.abort()
            return web.Response()

        self.handler = lose_response
        with self.assertRaises(b.TransportError) as caught:
            await api.request('max', '/messages', method='POST', body={'text': 'synthetic'})
        self.assertEqual(len(self.calls), 1)
        self.assertNotIn('PRIVATE', str(caught.exception))

    async def test_invalid_and_oversize_json_are_transport_failures(self):
        api = await self.api()
        for body in (b'not-json', b'[]', b'x' * (2 * 1024 * 1024 + 1)):
            self.handler = lambda request, data: web.Response(body=body)
            with self.assertRaises(b.TransportError):
                await api.request('max', '/messages', method='POST')

    async def test_unrequested_compression_is_not_expanded_past_read_limit(self):
        api = await self.api()
        self.handler = lambda request, body: web.Response(
            body=gzip.compress(b'x' * (2 * 1024 * 1024 + 1)), headers={'Content-Encoding': 'gzip'})
        with self.assertRaises(b.TransportError):
            await api.request('max', '/messages', method='POST')
        self.assertEqual(self.calls[0][2]['Accept-Encoding'], 'identity')

    async def test_api_redirect_never_republishes(self):
        api = await self.api()
        self.handler = lambda request, body: web.Response(status=307, headers={'Location': self.url + '/other'})
        with self.assertRaises(b.ApiError):
            await api.request('max', '/messages', method='POST')
        self.assertEqual(len(self.calls), 1)

    async def test_photo_checks_bytes_limit_and_redirect_target(self):
        api = await self.api()
        self.original_media_check = b.check_media_url
        with patch.object(b, 'check_media_url', side_effect=self.allow_local_media):
            self.handler = lambda request, body: web.Response(body=b'\xff\xd8\xffphoto', content_type='application/octet-stream')
            self.assertEqual((await api.download('max', self.url + '/image'))[1], 'image/jpeg')
            self.handler = lambda request, body: web.Response(body=b'not-an-image', content_type='image/jpeg')
            with self.assertRaises(b.BridgeError):
                await api.download('max', self.url + '/image')
            self.handler = lambda request, body: web.Response(body=b'x' * (b.PHOTO_LIMIT + 1))
            with self.assertRaises(b.TransportError):
                await api.download('max', self.url + '/large')
            self.handler = lambda request, body: web.Response(status=302, headers={'Location': 'https://evil.test/private'})
            previous = len(self.calls)
            with self.assertRaises(b.BridgeError):
                await api.download('max', self.url + '/redirect')
            self.assertEqual(len(self.calls), previous + 1)

    async def test_photo_upload_does_not_forward_api_token(self):
        api = await self.api()
        api.download = AsyncMock(return_value=(b'\xff\xd8\xffphoto', 'image/jpeg'))
        self.original_media_check = b.check_media_url

        def respond(request, body):
            if request.path == '/uploads':
                return web.json_response({'url': self.url + '/upload'})
            return web.json_response({'token': 'photo-token'})

        self.handler = respond
        with patch.object(b, 'check_media_url', side_effect=self.allow_local_media):
            self.assertEqual(await api.prepare_photo('tg', 'file', 'max'), {'token': 'photo-token'})
        self.assertEqual(self.calls[0][2]['Authorization'], 'PRIVATE_MAX')
        self.assertNotIn('Authorization', self.calls[1][2])
        self.assertIn(b'\xff\xd8\xffphoto', self.calls[1][3])
        self.assertIn('multipart/form-data', self.calls[1][2]['Content-Type'])

    async def test_timeout_and_explicit_cancellation_are_distinct(self):
        entered = asyncio.Event()

        async def wait(request, body):
            entered.set()
            await asyncio.sleep(0.2)
            return web.json_response({})

        self.handler = wait
        api = await self.api(aiohttp.ClientTimeout(total=0.05))
        with self.assertRaises(b.TransportError):
            await api.request('max', '/messages', method='POST')
        entered.clear()
        task = asyncio.create_task(api.request('max', '/messages', method='POST'))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(self.calls), 2)
        # Let timed-out synthetic server handlers finish before closing the listener.
        await asyncio.sleep(0.25)

    async def test_additional_ca_is_scoped_to_max_and_hostname_is_verified(self):
        if not shutil.which('openssl'):
            self.skipTest('openssl is needed to generate an ephemeral TLS test certificate')
        with tempfile.TemporaryDirectory() as temp:
            key, cert = Path(temp) / 'key.pem', Path(temp) / 'cert.pem'
            await asyncio.to_thread(subprocess.run, ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                '-keyout', str(key), '-out', str(cert), '-subj', '/CN=127.0.0.1', '-days', '1',
                '-addext', 'subjectAltName=IP:127.0.0.1'], check=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(cert, key)
            app = web.Application()
            app.router.add_route('*', '/{path:.*}', self.dispatch)
            runner = web.AppRunner(app)
            await runner.setup()
            loop = asyncio.get_running_loop()
            previous_handler = loop.get_exception_handler()

            def expected_tls_rejection(loop, context):
                if (isinstance(context.get('exception'), ConnectionResetError)
                        and context.get('message') == 'Error on transport creation for incoming connection'):
                    return
                if previous_handler:
                    previous_handler(loop, context)
                else:
                    loop.default_exception_handler(context)

            loop.set_exception_handler(expected_tls_rejection)
            try:
                await web.TCPSite(runner, '127.0.0.1', 0, ssl_context=context).start()
                port = runner.addresses[0][1]
                base = f'https://127.0.0.1:{port}'
                cfg = {'TG_BOT_TOKEN': 'synthetic', 'MAX_BOT_TOKEN': 'synthetic', 'MAX_CA_FILE': str(cert)}
                async with b.Api(cfg, tg_base=base, max_base=base) as api:
                    await api.request('max', '/me')
                    with self.assertRaises(b.CertificateError):
                        await api.request('tg', 'getMe')
                    api.max_base = f'https://localhost:{port}'
                    with self.assertRaises(b.CertificateError):
                        await api.request('max', '/me')
                await asyncio.sleep(0.05)
            finally:
                await runner.cleanup()
                loop.set_exception_handler(previous_handler)


class ResilienceHttpTests(HttpServer):
    async def test_refused_connection_is_provably_unsent_for_both_platforms(self):
        api = await self.api()
        api.tg_base = api.max_base = closed_port_url()
        for service, path in (('tg', 'sendMessage'), ('max', '/messages')):
            with self.assertRaises(b.ConnectError) as caught:
                await api.request(service, path, method='POST', body={'text': 'synthetic'})
            self.assertNotIn('PRIVATE', str(caught.exception))
        self.assertEqual(self.calls, [])

    async def test_http_retry_after_seconds_date_and_invalid_for_both_platforms(self):
        api = await self.api()
        cases = [('30', 30), (email.utils.formatdate(time.time() + 60, usegmt=True), 60),
                 ('soon', 0), ('-4', 0), ('Wed, 99 Foo 2015 07:28:00 GMT', 0)]
        for header, expected in cases:
            for service, path, body in (('tg', 'sendMessage', {'ok': False, 'error_code': 429}),
                                        ('max', '/messages', {'code': 'too.many.requests'})):
                with self.subTest(header=header, service=service):
                    self.handler = lambda request, _b, body=body, header=header: web.json_response(
                        body, status=429, headers={'Retry-After': header})
                    with self.assertRaises(b.ApiError) as caught:
                        await api.request(service, path, method='POST', body={'text': 'synthetic'})
                    self.assertEqual(caught.exception.code, '429')
                    self.assertAlmostEqual(caught.exception.retry_after, expected, delta=2)


    async def test_long_numeric_retry_after_keeps_a_definite_429(self):
        api = await self.api()
        for service, path, body in (('tg', 'sendMessage', {'ok': False, 'error_code': 429}),
                                    ('max', '/messages', {'code': 'too.many.requests'})):
            self.handler = lambda request, _b, body=body: web.json_response(
                body, status=429, headers={'Retry-After': '9' * 5000})
            with self.assertRaises(b.ApiError) as caught:
                await api.request(service, path, method='POST', body={'text': 'synthetic'})
            self.assertEqual((caught.exception.code, caught.exception.retry_after), ('429', 0))


class RuntimeTests(HttpServer):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        output = redirect_stdout(io.StringIO())
        output.__enter__()
        self.addCleanup(output.__exit__, None, None, None)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = b.Store(Path(self.tmp.name) / 'bridge.sqlite3')
        self.addCleanup(self.store.db.close)
        self.client = await self.api()
        self.bridge = b.Bridge({'TG_CHAT_ID': '-100', 'MAX_CHAT_ID': '-200'}, self.client,
                              self.store, {'tg': '99', 'max': '88'}, send_intervals={'tg': 0, 'max': 0})
        self.handler = self.normal

    def normal(self, request, body):
        if request.path == '/getUpdates':
            return web.json_response({'ok': True, 'result': []})
        if request.path == '/updates':
            return web.json_response({'updates': [], 'marker': 42})
        if request.path in ('/sendMessage', '/sendPhoto'):
            return web.json_response({'ok': True, 'result': {'message_id': len(self.calls)}})
        return web.json_response({'message': {'body': {'mid': 'copy-' + str(len(self.calls))}}})

    async def until(self, predicate, seconds=3):
        async with asyncio.timeout(seconds):
            while not predicate():
                await asyncio.sleep(0.005)

    def start(self, **kwargs):
        stop = asyncio.Event()
        task = asyncio.create_task(b.run_bridge(self.bridge, stop, poll_interval=0.01,
            delivery_interval=0.005, heartbeat_interval=0.01, shutdown_timeout=0.03, **kwargs))

        async def cleanup():
            stop.set()
            if not task.done():
                await asyncio.wait_for(task, 2)
            elif not task.cancelled():
                task.exception()

        self.addAsyncCleanup(cleanup)
        return stop, task

    def state(self, jid=1):
        return self.store.db.execute('SELECT state FROM jobs WHERE id=?', (jid,)).fetchone()[0]

    async def test_held_max_poll_does_not_stop_telegram_poll_send_or_heartbeat(self):
        entered, release = asyncio.Event(), asyncio.Event()
        self.store.accept('max', [mx('original')], 1, '-200', '88')

        async def handle(request, body):
            if request.path == '/updates':
                entered.set()
                await release.wait()
            if request.path == '/getUpdates':
                return web.json_response({'ok': True, 'result': [tg(1)]})
            return self.normal(request, body)

        self.handler = handle
        stop, task = self.start()
        try:
            await entered.wait()
            await self.until(lambda: self.state() == 'sent' and self.store.get('cursor_tg') == '2')
            before = self.store.get('heartbeat')
            await self.until(lambda: self.store.get('heartbeat') != before)
            self.assertEqual(self.store.get('cursor_max'), '1')
            self.assertFalse(task.done())
        finally:
            release.set()
            stop.set()
            await task

    async def test_held_telegram_poll_does_not_stop_max(self):
        entered, release = asyncio.Event(), asyncio.Event()
        self.store.accept('tg', [tg(1)], 2, '-100', '99')

        async def handle(request, body):
            if request.path == '/getUpdates':
                entered.set()
                await release.wait()
            return self.normal(request, body)

        self.handler = handle
        stop, task = self.start()
        try:
            await entered.wait()
            await self.until(lambda: self.state() == 'sent' and self.store.get('cursor_max') == '42')
            self.assertFalse(task.done())
        finally:
            release.set()
            stop.set()
            await task

    async def test_lost_publish_response_is_uncertain_and_blocks_only_destination(self):
        self.store.accept('tg', [tg(1), tg(2)], 3, '-100', '99')
        self.store.accept('max', [mx('one')], 1, '-200', '88')

        def handle(request, body):
            if request.path == '/messages':
                request.transport.abort()
                return web.Response()
            return self.normal(request, body)

        self.handler = handle
        stop, task = self.start()
        await self.until(lambda: self.state() == 'uncertain' and self.state(3) == 'sent')
        await asyncio.sleep(0.05)
        self.assertEqual(sum(c[1] == '/messages' for c in self.calls), 1)
        self.assertEqual(self.state(2), 'pending')
        stop.set()
        await task

    async def test_shutdown_during_publication_is_uncertain(self):
        entered, release = asyncio.Event(), asyncio.Event()
        self.store.accept('tg', [tg(1)], 2, '-100', '99')

        async def handle(request, body):
            if request.path == '/messages':
                entered.set()
                await release.wait()
            return self.normal(request, body)

        self.handler = handle
        stop, task = self.start()
        await entered.wait()
        self.assertEqual(self.state(), 'sending')
        stop.set()
        await task
        self.assertEqual(self.state(), 'uncertain')
        release.set()

    async def test_shutdown_during_photo_preparation_stays_pending(self):
        entered = asyncio.Event()
        event = tg(1)
        event['message']['photo'] = [{'file_id': 'file'}]
        self.store.accept('tg', [event], 2, '-100', '99')

        async def prepare(*args):
            entered.set()
            await asyncio.Event().wait()

        self.client.prepare_photo = prepare
        stop, task = self.start()
        await entered.wait()
        stop.set()
        await task
        self.assertEqual(self.state(), 'pending')
        self.assertFalse(any(c[1] == '/messages' for c in self.calls))

    async def test_finishing_preparation_after_stop_does_not_start_publication(self):
        entered, release = asyncio.Event(), asyncio.Event()
        event = tg(1)
        event['message']['photo'] = [{'file_id': 'file'}]
        self.store.accept('tg', [event], 2, '-100', '99')

        async def prepare(*args):
            entered.set()
            await release.wait()
            return {'token': 'image'}

        self.client.prepare_photo = prepare
        stop, task = self.start()
        await entered.wait()
        stop.set()
        release.set()
        await task
        self.assertEqual(self.state(), 'pending')
        self.assertFalse(any(c[1] == '/messages' for c in self.calls))

    async def test_success_without_remote_id_is_uncertain(self):
        self.store.accept('tg', [tg(1)], 2, '-100', '99')

        def handle(request, body):
            if request.path == '/messages':
                return web.json_response({'message': {'body': {'mid': None}}})
            return self.normal(request, body)

        self.handler = handle
        stop, task = self.start()
        await self.until(lambda: self.state() == 'uncertain')
        self.assertEqual(sum(c[1] == '/messages' for c in self.calls), 1)
        stop.set()
        await task

    async def test_429_delays_one_sender_without_stopping_other_workers(self):
        self.store.accept('tg', [tg(1)], 2, '-100', '99')
        self.store.accept('max', [mx('one')], 1, '-200', '88')

        def handle(request, body):
            if request.path == '/messages':
                return web.json_response({'code': 'too.many.requests'}, status=429, headers={'Retry-After': '120'})
            return self.normal(request, body)

        self.handler = handle
        stop, task = self.start()
        await self.until(lambda: self.state(2) == 'sent')
        await self.until(lambda: self.store.db.execute('SELECT ready FROM jobs WHERE id=1').fetchone()[0] > time.time())
        self.assertEqual(self.state(), 'pending')
        self.assertGreater(self.store.db.execute('SELECT ready FROM jobs WHERE id=1').fetchone()[0], time.time() + 100)
        self.assertEqual(sum(c[1] == '/messages' for c in self.calls), 1)
        stop.set()
        await task

    async def test_401_pauses_platform_until_restart_but_not_other_platform(self):
        self.store.accept('max', [mx('one')], 1, '-200', '88')

        def handle(request, body):
            if request.path == '/updates':
                return web.json_response({}, status=401)
            return self.normal(request, body)

        self.handler = handle
        stop, task = self.start()
        await self.until(lambda: 'max' in self.bridge.paused and self.state() == 'sent')
        await asyncio.sleep(0.05)
        self.assertEqual(sum(c[1] == '/updates' for c in self.calls), 1)
        self.assertIsNotNone(self.store.get('last_poll_error_max'))
        self.assertFalse(task.done())
        stop.set()
        await task

    async def test_poll_backoff_grows_to_cap_and_resets_after_success(self):
        moments, outcomes = [], [503, 503, 503, 503, 200, 503, 200]

        def handle(request, body):
            if request.path == '/getUpdates':
                moments.append(time.monotonic())
                status = outcomes[len(moments) - 1] if len(moments) <= len(outcomes) else 200
                if status != 200:
                    return web.json_response({'ok': False, 'error_code': status}, status=status)
            return self.normal(request, body)

        self.handler = handle
        self.bridge.random = lambda: 1.0
        with patch.object(b, 'POLL_BACKOFF', (0.04, 0.16)):
            stop, task = self.start()
            await self.until(lambda: len(moments) >= 8)
            stop.set()
            await task
        gaps = [b - a for a, b in zip(moments, moments[1:])]
        top = 0.16 * (1 + b.BACKOFF_JITTER)
        for gap, minimum in zip(gaps, (0.048, 0.096, top, top)):
            self.assertGreaterEqual(gap, minimum - 0.005, gaps)
        self.assertLess(max(gaps[:4]), top + 0.15, gaps)
        self.assertLess(gaps[5], 0.096, gaps)  # first failure after a success starts from the base again

    async def test_stop_interrupts_a_long_retry_wait(self):
        def handle(request, body):
            if request.path == '/getUpdates':
                return web.json_response({'ok': False, 'error_code': 502}, status=502)
            return self.normal(request, body)

        self.handler = handle
        with patch.object(b, 'POLL_BACKOFF', (60, 60)):
            stop, task = self.start()
            await self.until(lambda: self.store.get('last_poll_failure_tg') is not None)
            started = time.monotonic()
            stop.set()
            await asyncio.wait_for(task, 2)
        self.assertLess(time.monotonic() - started, 1)
        self.assertEqual(sum(c[1] == '/getUpdates' for c in self.calls), 1)

    async def test_max_outage_keeps_telegram_and_heartbeat_then_delivers_once_in_order(self):
        self.store.accept('tg', [tg(1), tg(2), tg(3)], 4, '-100', '99')
        self.store.accept('max', [mx('one')], 1, '-200', '88')
        working, refused = self.client.max_base, []
        real_read = self.client.read

        async def counting_read(platform, url, **kwargs):
            try:
                return await real_read(platform, url, **kwargs)
            except b.ConnectError:
                refused.append(url)
                raise

        self.client.read = counting_read
        self.client.max_base = closed_port_url()
        with patch.object(b, 'SEND_BACKOFF', (0.02, 0.05)), patch.object(b, 'POLL_BACKOFF', (0.02, 0.05)):
            stop, task = self.start()
            await self.until(lambda: self.state(4) == 'sent' and sum('/messages' in u for u in refused) >= 3)
            before = self.store.get('heartbeat')
            await self.until(lambda: self.store.get('heartbeat') != before)
            self.assertEqual([self.state(i) for i in (1, 2, 3)], ['pending'] * 3)
            self.assertEqual(sum(c[1] == '/messages' for c in self.calls), 0)
            self.assertIsNotNone(self.store.get('last_poll_error_max'))
            self.client.max_base = working
            await self.until(lambda: [self.state(i) for i in (1, 2, 3)] == ['sent'] * 3)
            stop.set()
            await task
        self.assertEqual(sum(c[1] == '/messages' for c in self.calls), 3)
        remote = [self.store.db.execute('SELECT remote_id FROM jobs WHERE id=?', (i,)).fetchone()[0] for i in (1, 2, 3)]
        self.assertEqual(remote, sorted(remote, key=lambda r: int(r.split('-')[1])))
        self.assertEqual(self.store.get('cursor_tg'), '4')

    async def test_retry_after_header_delays_the_real_next_publication(self):
        self.store.accept('tg', [tg(1)], 2, '-100', '99')
        moments = []

        def handle(request, body):
            if request.path == '/messages':
                moments.append(time.monotonic())
                if len(moments) == 1:
                    return web.json_response({'code': 'too.many.requests'}, status=429, headers={'Retry-After': '1'})
            return self.normal(request, body)

        self.handler = handle
        with patch.object(b, 'SEND_BACKOFF', (0.01, 0.01)):
            stop, task = self.start()
            await self.until(lambda: self.state() == 'sent')
            stop.set()
            await task
        self.assertEqual(len(moments), 2)
        self.assertGreaterEqual(moments[1] - moments[0], 1.0)

    async def test_certificate_failure_pauses_only_that_platform(self):
        self.store.accept('max', [mx('one')], 1, '-200', '88')
        real_poll, attempts = self.bridge.poll, []

        async def poll(platform):
            if platform == 'max':
                attempts.append(platform)
                raise b.CertificateError('Ошибка проверки TLS-сертификата')
            return await real_poll(platform)

        self.bridge.poll = poll
        stop, task = self.start()
        await self.until(lambda: 'max' in self.bridge.paused and self.state() == 'sent')
        await asyncio.sleep(0.05)
        self.assertEqual(attempts, ['max'])
        self.assertFalse(task.done())
        stop.set()
        await task

    async def test_conflicting_poller_stops_entire_runtime(self):
        self.handler = lambda request, body: web.json_response({}, status=409)
        _, task = self.start()
        with self.assertRaisesRegex(b.BridgeError, 'poller'):
            await task
        self.assertEqual(self.store.get('runtime_state'), 'stopped')

    async def test_programming_error_stops_runtime_instead_of_retrying(self):
        def handle(request, body):
            if request.path == '/getUpdates':
                return web.json_response({'ok': True, 'result': ['PRIVATE_INVALID_ENVELOPE']})
            return self.normal(request, body)

        self.handler = handle
        _, task = self.start()
        with self.assertRaises(TypeError):
            await task
        self.assertEqual(self.store.get('runtime_state'), 'stopped')

    async def test_result_commit_failure_never_republishes(self):
        self.store.accept('tg', [tg(1)], 2, '-100', '99')
        self.store.db.execute("""CREATE TRIGGER fail_sent BEFORE UPDATE ON jobs
            WHEN NEW.state='sent' BEGIN SELECT RAISE(ABORT,'synthetic'); END""")
        _, task = self.start()
        with self.assertRaises(sqlite3.IntegrityError):
            await task
        self.assertEqual(self.state(), 'sending')
        self.store.recover()
        self.assertEqual(self.state(), 'uncertain')
        self.assertEqual(sum(c[1] == '/messages' for c in self.calls), 1)

    async def test_overlapping_attempts_can_publish_queue_head_only_once(self):
        event = tg(1)
        event['message']['photo'] = [{'file_id': 'file'}]
        self.store.accept('tg', [event], 2, '-100', '99')
        entered, release = asyncio.Event(), asyncio.Event()
        preparations = 0

        async def prepare(*args):
            nonlocal preparations
            preparations += 1
            if preparations == 2:
                entered.set()
            await release.wait()
            return {'token': 'image'}

        self.client.prepare_photo = prepare
        tasks = [asyncio.create_task(self.bridge.deliver('max')) for _ in range(2)]
        await entered.wait()
        release.set()
        await asyncio.gather(*tasks)
        self.assertEqual(sum(c[1] == '/messages' for c in self.calls), 1)
        self.assertEqual(self.state(), 'sent')

    async def test_order_is_preserved_for_100_messages_each_way(self):
        self.store.accept('tg', [tg(i, f'item-{i}') for i in range(1, 101)], 101, '-100', '99')
        self.store.accept('max', [mx(str(i)) for i in range(1, 101)], 101, '-200', '88')
        stop, task = self.start()
        await self.until(lambda: self.store.db.execute("SELECT count(*) FROM jobs WHERE state='sent'").fetchone()[0] == 200, seconds=10)
        delivered = [json.loads(c[3])['text'] for c in self.calls if c[1] == '/messages']
        self.assertEqual([text.split('item-')[1] for text in delivered], [str(i) for i in range(1, 101)])
        self.assertEqual(sum(c[1] == '/sendMessage' for c in self.calls), 100)
        stop.set()
        await task

    async def test_database_lock_is_bounded_and_does_not_acknowledge_batch(self):
        self.store.accept('tg', [tg(1)], 2, '-100', '99')
        self.handler = lambda request, body: web.json_response({'ok': True, 'result': [tg(2)]})
        lock = sqlite3.connect(Path(self.tmp.name) / 'bridge.sqlite3')
        lock.execute('BEGIN IMMEDIATE')
        loop = asyncio.get_running_loop()
        previous_slow_threshold = loop.slow_callback_duration
        loop.slow_callback_duration = 0.5  # This test deliberately waits for SQLite's 100ms bound.
        started = time.monotonic()
        try:
            with self.assertRaises(sqlite3.OperationalError):
                await self.bridge.poll('tg')
            self.assertLess(time.monotonic() - started, 0.5)
            self.assertEqual(self.store.get('cursor_tg'), '2')
            self.assertEqual(self.store.db.execute('SELECT count(*) FROM jobs').fetchone()[0], 1)
            self.assertFalse(self.store.db.in_transaction)
        finally:
            lock.rollback()
            lock.close()
            loop.call_soon(setattr, loop, 'slow_callback_duration', previous_slow_threshold)

    async def test_telegram_photo_to_max_keeps_reply_and_awaits_without_transaction(self):
        self.store.accept('tg', [tg(1)], 2, '-100', '99')
        self.store.update(1, 'sent', remote_id='max-parent')
        event = tg(2)
        event['message']['photo'] = [{'file_id': 'file'}]
        event['message']['reply_to_message'] = {'message_id': 1, 'text': 'parent'}
        self.store.accept('tg', [event], 3, '-100', '99')
        self.client.tg_file_base = self.url + '/files'
        self.original_media_check = b.check_media_url

        def handle(request, body):
            self.assertFalse(self.store.db.in_transaction)
            if request.path == '/getFile':
                return web.json_response({'ok': True, 'result': {'file_path': 'image.jpg'}})
            if request.path == '/files/image.jpg':
                return web.Response(body=b'\xff\xd8\xffphoto')
            if request.path == '/uploads':
                return web.json_response({'url': self.url + '/upload'})
            if request.path == '/upload':
                return web.json_response({'token': 'photo-token'})
            return self.normal(request, body)

        self.handler = handle
        with patch.object(b, 'check_media_url', side_effect=self.allow_local_media):
            await self.bridge.deliver('max')
        self.assertEqual(self.state(2), 'sent')
        payload = json.loads(next(c[3] for c in self.calls if c[1] == '/messages'))
        self.assertEqual(payload['link'], {'type': 'reply', 'mid': 'max-parent'})
        self.assertEqual(payload['attachments'][0]['payload'], {'token': 'photo-token'})

    async def test_max_photo_to_telegram_keeps_reply_and_caption(self):
        self.store.accept('max', [mx('1')], 1, '-200', '88')
        self.store.update(1, 'sent', remote_id='75')
        event = mx('2')
        event['message']['body']['attachments'] = [{'type': 'image', 'payload': {'url': self.url + '/image'}}]
        event['message']['link'] = {'type': 'reply', 'message': {'mid': '1', 'text': 'parent'}}
        self.store.accept('max', [event], 2, '-200', '88')
        self.original_media_check = b.check_media_url

        def handle(request, body):
            self.assertFalse(self.store.db.in_transaction)
            if request.path == '/image':
                return web.Response(body=b'\x89PNG\r\n\x1a\nsynthetic')
            return self.normal(request, body)

        self.handler = handle
        with patch.object(b, 'check_media_url', side_effect=self.allow_local_media):
            await self.bridge.deliver('tg')
        self.assertEqual(self.state(2), 'sent')
        payload = next(c[3] for c in self.calls if c[1] == '/sendPhoto')
        self.assertIn(b'"message_id": 75', payload)
        self.assertIn(b'\x89PNG\r\n\x1a\nsynthetic', payload)
        self.assertIn(b'synthetic', payload)


    def posts(self, path):
        return sum(1 for c in self.calls if c[1] == path)

    async def test_http_503_with_retryable_body_after_publication_is_uncertain_once(self):
        self.store.accept('tg', [tg(1)], 2, '-100', '99')

        def handle(request, body):
            if request.path == '/messages':
                return web.json_response({'code': '429'}, status=503, headers={'Retry-After': '1'})
            return self.normal(request, body)

        self.handler = handle
        await self.bridge.deliver('max')
        self.assertEqual((self.state(1), self.posts('/messages')), ('uncertain', 1))
        with self.store.db:
            self.store.db.execute('UPDATE jobs SET ready=0')
        restarted = b.Store(Path(self.tmp.name) / 'bridge.sqlite3')
        self.addCleanup(restarted.db.close)
        bridge = b.Bridge({'TG_CHAT_ID': '-100', 'MAX_CHAT_ID': '-200'}, self.client, restarted,
                          {'tg': '99', 'max': '88'}, send_intervals={'tg': 0, 'max': 0})
        await bridge.deliver('max')
        self.assertEqual((self.state(1), self.posts('/messages')), ('uncertain', 1))

    async def test_photo_download_429_and_404_through_deliver_both_directions(self):
        event = tg(1)
        event['message']['photo'] = [{'file_id': 'file'}]
        self.store.accept('tg', [event], 2, '-100', '99')
        event = mx('2')
        event['message']['body']['attachments'] = [{'type': 'image', 'payload': {'url': self.url + '/image'}}]
        self.store.accept('max', [event], 2, '-200', '88')
        self.client.tg_file_base = self.url + '/files'
        media = {'status': 429}

        def handle(request, body):
            if request.path == '/getFile':
                return web.json_response({'ok': True, 'result': {'file_path': 'image.jpg'}})
            if request.path in ('/files/image.jpg', '/image'):
                return web.Response(status=media['status'], headers={'Retry-After': '60'}, text='synthetic')
            return self.normal(request, body)

        self.handler = handle
        with patch.object(b, 'check_media_url', side_effect=self.allow_local_media):
            for destination, jid in (('max', 1), ('tg', 2)):
                media['status'] = 429
                before = time.time()
                await self.bridge.deliver(destination)
                job = self.store.db.execute('SELECT state, ready FROM jobs WHERE id=?', (jid,)).fetchone()
                self.assertEqual(job[0], 'pending', destination)
                self.assertGreaterEqual(job[1], before + 60, destination)
                with self.store.db:
                    self.store.db.execute('UPDATE jobs SET ready=0')
                media['status'] = 404
                await self.bridge.deliver(destination)
                self.assertEqual(self.state(jid), 'failed', destination)
        self.assertEqual(self.posts('/messages') + self.posts('/sendPhoto'), 0)

    async def test_long_retry_after_on_poll_does_not_stop_the_runtime(self):
        def handle(request, body):
            if request.path == '/getUpdates':
                return web.json_response({'ok': False, 'error_code': 429}, status=429,
                                         headers={'Retry-After': '9' * 5000})
            return self.normal(request, body)

        self.handler = handle
        _, task = self.start()
        await self.until(lambda: self.posts('/getUpdates') >= 1 and self.posts('/updates') >= 5)
        self.assertFalse(task.done())
        self.assertNotIn('tg', self.bridge.paused)


    def json_lines(self, text):
        records = [json.loads(line) for line in text.splitlines() if line.strip()]
        for record in records:
            self.assertTrue({'timestamp', 'level', 'component', 'event', 'run_id'} <= set(record), record)
            self.assertTrue(record['timestamp'].endswith('Z') and isinstance(record['run_id'], str), record)
        return records

    async def test_run_logs_are_safe_json_lines_for_each_outcome(self):
        events = [tg(1, 'PRIVATE_TEXT_1'), tg(2, 'PRIVATE_TEXT_2')]
        for event in events:
            event['message']['from']['first_name'] = 'PRIVATE_AUTHOR'
        self.store.accept('tg', events, 3, '-100', '99')
        event = mx('m1')
        event['message']['body']['text'] = 'PRIVATE_TEXT_3'
        event['message']['sender']['first_name'] = 'PRIVATE_AUTHOR'
        self.store.accept('max', [event], 'k1', '-200', '88')
        replies = iter([web.json_response({'code': '429', 'message': 'PRIVATE_BODY'}, status=429),
                        None, web.json_response({'code': 'PRIVATE_CODE'}, status=400)])

        def handle(request, body):
            if request.path == '/messages':
                return next(replies) or self.normal(request, body)
            if request.path == '/sendMessage':
                return web.json_response({'ok': False, 'description': 'PRIVATE_BODY'}, status=500)
            return self.normal(request, body)

        self.handler = handle
        output = io.StringIO()
        with patch.object(b, 'SEND_BACKOFF', (0.01, 0.02)), redirect_stdout(output):
            stop, task = self.start()
            await self.until(lambda: [self.state(i) for i in (1, 2, 3)] == ['sent', 'failed', 'uncertain'], 5)
            stop.set()
            await task
        records = self.json_lines(output.getvalue())
        events = [r['event'] for r in records]
        for event in ('bridge_started', 'delivery_retry_scheduled', 'delivery_sent', 'delivery_failed',
                      'delivery_uncertain'):
            self.assertIn(event, events)
        retry = next(r for r in records if r['event'] == 'delivery_retry_scheduled')
        self.assertEqual((retry['error_code'], retry['http_status'], retry['direction'], retry['job_id'], retry['state']),
                         ('rate_limited', 429, 'tg>max', 1, 'pending'))
        self.assertTrue(retry['retry_at'] > 0 and retry['delay_seconds'] > 0)
        uncertain = next(r for r in records if r['event'] == 'delivery_uncertain')
        self.assertEqual((uncertain['error_code'], uncertain['phase'], uncertain['direction'], uncertain['job_id']),
                         ('server_error', 'publish', 'max>tg', 3))
        self.assertEqual(next(r for r in records if r['event'] == 'delivery_failed')['error_code'], 'api_rejected')
        self.assertEqual(len({r['run_id'] for r in records}), 1)
        self.assertFalse([e for e in events if e.startswith('poll') or 'heartbeat' in e])
        for marker in ('PRIVATE', '127.0.0.1'):
            self.assertNotIn(marker, output.getvalue())
        error = self.store.db.execute('SELECT error FROM jobs WHERE id=2').fetchone()[0]
        self.assertTrue(error.startswith('api_rejected http=400') and 'PRIVATE' not in error, error)

    async def test_one_stalled_worker_is_reported_while_heartbeat_and_others_continue(self):
        release = asyncio.Event()
        real_poll, real_deliver = self.bridge.poll, self.bridge.deliver

        async def poll(platform):
            if platform == 'max':
                await release.wait()
            return await real_poll(platform)

        async def deliver(destination, stop=None):
            if destination == 'tg':
                await release.wait()
            return await real_deliver(destination, stop)

        self.bridge.poll, self.bridge.deliver = poll, deliver
        self.bridge.budgets = {'poll': 0.05, 'deliver': 0.05}
        self.start()

        def found():
            snapshot = b.health_snapshot(self.store, running=True, stale_after=0.5)
            return snapshot, {(i['code'], i.get('worker')) for i in snapshot['issues']}

        await self.until(lambda: {('worker_stalled', 'poll:max'), ('worker_stalled', 'send:tg')} <= found()[1])
        snapshot, codes = found()
        self.assertEqual({w for c, w in codes if c == 'worker_stalled'}, {'poll:max', 'send:tg'})
        self.assertNotIn('heartbeat_stale', {c for c, _ in codes})
        stalled = next(i for i in snapshot['issues'] if i.get('worker') == 'poll:max')
        self.assertEqual((stalled['platform'], stalled['phase']), ('max', 'poll'))
        self.assertEqual(snapshot['workers']['send:max']['state'], 'ok')
        polls = self.posts('/getUpdates')
        await self.until(lambda: self.posts('/getUpdates') > polls + 3)
        release.set()
        await self.until(lambda: 'worker_stalled' not in {c for c, _ in found()[1]})


    async def test_frozen_event_loop_is_not_reported_as_stalled_workers_after_resume(self):
        # SIGSTOP/host suspend freezes every task; on resume the heartbeat may run before the workers.
        seen, frozen = set(), {'count': 0}
        real_heartbeat = self.store.heartbeat

        def heartbeat(*args):
            real_heartbeat(*args)
            snapshot = b.health_snapshot(self.store, running=True, stale_after=0.3)
            seen.update(i.get('worker') for i in snapshot['issues'] if i['code'] == 'worker_stalled')

        async def freeze():
            while frozen['count'] < 3:
                await asyncio.sleep(0.2)
                time.sleep(1.2)
                frozen['count'] += 1

        self.store.heartbeat = heartbeat
        asyncio.get_running_loop().slow_callback_duration = 5
        with redirect_stdout(io.StringIO()):
            self.start()
            await self.until(lambda: self.store.get('workers'))
            await freeze()
            beats = self.store.get('heartbeat')
            await self.until(lambda: self.store.get('heartbeat') != beats)
        self.assertEqual(seen, set())

    async def test_known_stall_is_not_recovered_by_resuming_a_frozen_loop(self):
        # F2 x F7: SIGCONT of the whole process does not confirm progress of a worker that was already hung.
        release = asyncio.Event()
        real_deliver = self.bridge.deliver

        async def deliver(destination, stop=None):
            if destination == 'max':
                await release.wait()
            return await real_deliver(destination, stop)

        self.bridge.deliver = deliver
        self.bridge.budgets = {'poll': 0.05, 'deliver': 0.05}
        monitor = b.AlertMonitor(':memory:')
        self.addCleanup(monitor.db.close)
        emitted = []

        def observe():
            monitor.observe(b.health_snapshot(self.store, running=True, stale_after=0.3))
            monitor.flush(emitted.append)
            return [a['kind'] for a in emitted if a['issue'].get('worker') == 'send:max']

        asyncio.get_running_loop().slow_callback_duration = 5
        with redirect_stdout(io.StringIO()):
            self.start()
            await self.until(lambda: observe() == ['problem'])
            time.sleep(1.2)
            beats = self.store.get('heartbeat')
            await self.until(lambda: self.store.get('heartbeat') != beats)
            for _ in range(8):
                observe()
                await asyncio.sleep(0.1)
            self.assertEqual(observe(), ['problem'])
            release.set()
            await self.until(lambda: observe() == ['problem', 'recovery'])
            for _ in range(3):
                observe()
                await asyncio.sleep(0.05)
        self.assertEqual(observe(), ['problem', 'recovery'])

    async def test_phase_started_after_resume_keeps_its_own_budget(self):
        # F9: a worker may run before the heartbeat after SIGCONT; its new deadline is not shifted by the freeze.
        release, resumed = asyncio.Event(), {}
        real_deliver = self.bridge.deliver

        async def deliver(destination, stop=None):
            if destination == 'max' and not resumed:
                await asyncio.sleep(0.1)
                time.sleep(1.2)
                resumed['at'] = time.time()
                # The next delivery phase starts in the same task, before the heartbeat can run.
                self.bridge.phase('send:max', 'deliver', time.time() + self.bridge.budgets['deliver'])
                await release.wait()
            return await real_deliver(destination, stop)

        def stalled():
            snapshot = b.health_snapshot(self.store, running=True, stale_after=0.3)
            return snapshot['runtime']['heartbeat_at'] > resumed['at'] and any(
                i.get('worker') == 'send:max' for i in snapshot['issues'] if i['code'] == 'worker_stalled')

        self.bridge.deliver = deliver
        self.bridge.budgets = {'poll': 0.05, 'deliver': 0.05}
        asyncio.get_running_loop().slow_callback_duration = 5
        with redirect_stdout(io.StringIO()):
            self.start()
            await self.until(lambda: resumed)
            await self.until(stalled, 2)
            detected = time.time() - resumed['at']
            release.set()
        self.assertLess(detected, 0.05 + 0.3 + 0.4)

    async def test_resume_shifts_only_deadlines_that_lived_through_the_freeze(self):
        self.bridge.workers = {
            'send:max': {'phase': 'deliver', 'since': 100, 'until': 105, 'progress_at': 100, 'reason': None},
            'send:tg': {'phase': 'deliver', 'since': 119, 'until': 125, 'progress_at': 119, 'reason': None},
            'poll:tg': {'phase': 'poll', 'since': 100, 'until': 150, 'progress_at': 119.5, 'reason': None},
            'poll:max': {'phase': 'paused', 'since': 100, 'until': None, 'progress_at': 100, 'reason': None}}
        self.bridge.resumed(last=101, now=120, interval=2)
        self.assertEqual({k: v['until'] for k, v in self.bridge.workers.items()},
                         {'send:max': 122, 'send:tg': 125, 'poll:tg': 150, 'poll:max': None})
        self.assertEqual(self.bridge.workers['send:max']['since'], 100)

    async def test_untrusted_api_code_does_not_stop_the_bridge(self):
        def handle(request, body):
            if request.path == '/getUpdates':
                return web.json_response({'ok': False, 'error_code': '\u00b2'}, status=429)
            return self.normal(request, body)

        self.handler = handle
        with redirect_stdout(io.StringIO()):
            _, task = self.start()
            await self.until(lambda: self.store.get('last_poll_error_tg'))
            polls = self.posts('/updates')
            await self.until(lambda: self.posts('/updates') > polls + 3)
        self.assertFalse(task.done())
        self.assertEqual(self.store.get('runtime_state'), 'running')
        self.assertTrue(self.store.get('last_poll_error_tg').startswith('rate_limited http=429'))

    async def test_http_5xx_reason_wins_over_attachment_not_ready_body(self):
        self.store.accept('tg', [tg(1)], 2, '-100', '99')
        self.handler = lambda request, body: (web.json_response({'code': 'attachment.not.ready'}, status=503)
                                              if request.path == '/messages' else self.normal(request, body))
        output = io.StringIO()
        with redirect_stdout(output):
            await self.bridge.deliver('max')
        self.assertEqual((self.state(1), self.posts('/messages')), ('uncertain', 1))
        record = json.loads(output.getvalue().splitlines()[-1])
        self.assertEqual((record['event'], record['error_code'], record['http_status']),
                         ('delivery_uncertain', 'server_error', 503))
        snapshot = b.health_snapshot(self.store, running=True)
        self.assertEqual(snapshot['platforms']['max']['delivery']['head']['reason'], 'server_error')

    async def test_slow_but_progressing_photo_delivery_is_not_a_stall(self):
        event = tg(2)
        event['message']['photo'] = [{'file_id': 'file'}]
        self.store.accept('tg', [event], 3, '-100', '99')
        self.client.tg_file_base = self.url + '/files'
        self.original_media_check = b.check_media_url

        async def handle(request, body):
            if request.path in ('/getFile', '/files/image.jpg', '/uploads', '/upload', '/messages'):
                await asyncio.sleep(0.2)
            if request.path == '/getFile':
                return web.json_response({'ok': True, 'result': {'file_path': 'image.jpg'}})
            if request.path == '/files/image.jpg':
                return web.Response(body=b'\xff\xd8\xffphoto')
            if request.path == '/uploads':
                return web.json_response({'url': self.url + '/upload'})
            if request.path == '/upload':
                return web.json_response({'token': 'photo-token'})
            return self.normal(request, body)

        self.handler = handle
        self.bridge.budgets = {'poll': 0.3, 'deliver': 0.3}
        seen = set()
        with patch.object(b, 'check_media_url', side_effect=self.allow_local_media), redirect_stdout(io.StringIO()):
            self.start()

            def sample():
                snapshot = b.health_snapshot(self.store, running=True, stale_after=0.2)
                seen.update(i.get('worker') for i in snapshot['issues'] if i['code'] == 'worker_stalled')
                return self.state(1) == 'sent'

            await self.until(sample, 10)
        self.assertNotIn('send:max', seen)

    async def test_cli_run_failure_has_run_id_in_json(self):
        import sys
        with tempfile.TemporaryDirectory() as state:
            env = {'PATH': os.environ.get('PATH', ''), 'HOME': state, 'TG_BOT_TOKEN': '', 'MAX_BOT_TOKEN': ''}
            runs = [subprocess.run([sys.executable, b.__file__, 'run', '--state-dir', state], env=env,
                                   capture_output=True, text=True, timeout=60) for _ in range(2)]
        ids = []
        for run in runs:
            self.assertEqual(run.returncode, 1, run.stderr)
            records = [json.loads(line) for line in (run.stdout + run.stderr).splitlines() if line.strip()]
            failure = next(r for r in records if r['event'] == 'stopped_with_error')
            self.assertTrue(isinstance(failure['run_id'], str) and failure['run_id'])
            ids.append(failure['run_id'])
        self.assertNotEqual(ids[0], ids[1])


if __name__ == '__main__':
    unittest.main()
