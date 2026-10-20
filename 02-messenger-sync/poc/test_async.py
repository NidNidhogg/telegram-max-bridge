"""Async transport/runtime checks against a real local HTTP server; no live tokens."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

import aiohttp
from aiohttp import web

import bridge as b


class HttpTests(unittest.IsolatedAsyncioTestCase):
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
        api = b.AsyncApi({'TG_BOT_TOKEN': 'PRIVATE_TG', 'MAX_BOT_TOKEN': 'PRIVATE_MAX'},
                         tg_base=self.url, max_base=self.url, timeout=timeout)
        await api.__aenter__()
        self.addAsyncCleanup(api.__aexit__, None, None, None)
        return api

    def allow_local_media(self, url):
        if not url.startswith(self.url + '/'):
            self.original_media_check(url)

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


if __name__ == '__main__':
    unittest.main()
