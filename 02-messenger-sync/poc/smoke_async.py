#!/usr/bin/env python3
"""Real async HTTP smoke check against local servers; no platform credentials."""
import asyncio

from aiohttp import web

import bridge as b


async def transport_smoke():
    entered, release = asyncio.Event(), asyncio.Event()
    publications = []

    async def handle(request):
        await request.read()
        if request.path == '/max/updates':
            entered.set()
            await release.wait()
            return web.json_response({'updates': [], 'marker': 42})
        publications.append(request.path)
        if request.path == '/max/messages':
            request.transport.abort()
            return web.Response()
        return web.json_response({'ok': True, 'result': {'message_id': 123}})

    app = web.Application()
    app.router.add_route('*', '/{path:.*}', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        await web.TCPSite(runner, '127.0.0.1', 0).start()
        base = f'http://127.0.0.1:{runner.addresses[0][1]}'
        cfg = {'TG_BOT_TOKEN': 'synthetic', 'MAX_BOT_TOKEN': 'synthetic'}
        async with b.AsyncApi(cfg, tg_base=base + '/tg', max_base=base + '/max') as api:
            poll = asyncio.create_task(api.request('max', '/updates'))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                result = await asyncio.wait_for(api.request('tg', 'sendMessage', method='POST',
                    body={'text': 'synthetic'}), 2)
                assert result['message_id'] == 123 and not poll.done()
                try:
                    await api.request('max', '/messages', method='POST', body={'text': 'synthetic'})
                except b.TransportError:
                    pass
                else:
                    raise AssertionError('Lost response was accepted as success')
                assert publications == ['/tg/sendMessage', '/max/messages']
            finally:
                release.set()
                await poll
        assert not api.sessions
    finally:
        await runner.cleanup()
    print('Async HTTP OK: held MAX, independent Telegram, one POST after lost reply, closed sessions')


if __name__ == '__main__':
    asyncio.run(transport_smoke())
