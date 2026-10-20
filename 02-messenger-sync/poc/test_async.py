"""Async transport/runtime checks against a real local HTTP server; no live tokens."""
import asyncio
import unittest

import aiohttp
from aiohttp import web


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


if __name__ == '__main__':
    unittest.main()
