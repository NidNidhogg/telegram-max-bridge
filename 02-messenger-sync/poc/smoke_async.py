#!/usr/bin/env python3
"""Real async HTTP smoke check against local servers; no platform credentials."""
import asyncio
import argparse
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time

from aiohttp import web

import bridge as b
from smoke_p0 import tg, mx


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
        async with b.Api(cfg, tg_base=base + '/tg', max_base=base + '/max') as api:
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


async def runtime_smoke(outage_seconds=0):
    """Run the real CLI in a child against HTTP; identity checks use synthetic IDs."""
    tg_events = [tg(i, f'synthetic-{i}') for i in range(1, 21)]
    max_events = [mx(str(i)) for i in range(1, 21)]
    publications = []
    outage = False
    tg_release, tg_entered = asyncio.Event(), asyncio.Event()
    tg_release.set()

    async def handle(request):
        body = await request.json() if request.method == 'POST' else {}
        if outage and request.path.startswith('/max/'):
            if request.path == '/max/messages':
                publications.append(request.path)
            return web.json_response({}, status=503)
        if request.path == '/tg/getUpdates':
            offset = body.get('offset') or 0
            return web.json_response({'ok': True, 'result': [e for e in tg_events if e['update_id'] >= offset]})
        if request.path == '/max/updates':
            marker = int(request.query.get('marker') or 0)
            return web.json_response({'updates': max_events[marker:], 'marker': len(max_events)})
        publications.append(request.path)
        if request.path == '/tg/sendMessage':
            if not tg_release.is_set():
                tg_entered.set()
            await tg_release.wait()
            return web.json_response({'ok': True, 'result': {'message_id': len(publications)}})
        return web.json_response({'message': {'body': {'mid': 'copy-' + str(len(publications))}}})

    app = web.Application()
    app.router.add_route('*', '/{path:.*}', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '127.0.0.1', 0).start()
    base = f'http://127.0.0.1:{runner.addresses[0][1]}'
    with tempfile.TemporaryDirectory(prefix='bridge-async-http-') as temp:
        state = Path(temp)

        def snapshot():
            try:
                with sqlite3.connect(f'file:{state / "bridge.sqlite3"}?mode=ro', uri=True) as db:
                    counts = dict(db.execute('SELECT state,count(*) FROM jobs GROUP BY state'))
                    meta = dict(db.execute('SELECT key,value FROM meta'))
                    return counts, meta
            except sqlite3.OperationalError:
                return {}, {}

        async def until(predicate, seconds=15):
            async with asyncio.timeout(seconds):
                while not predicate():
                    await asyncio.sleep(0.02)

        async def start():
            return await asyncio.create_subprocess_exec(sys.executable, str(Path(__file__).resolve()),
                '--child', str(state), base, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        async def stop(child):
            if child.returncode is None:
                child.terminate()
            try:
                stdout, stderr = await asyncio.wait_for(child.communicate(), 10)
            except TimeoutError:
                child.kill()
                await child.communicate()
                raise AssertionError('Child failed to stop')
            assert child.returncode == 0, (stdout.decode(), stderr.decode())
            assert not stderr, stderr.decode()

        child = await start()
        try:
            await until(lambda: snapshot()[0].get('sent') == 40)
            assert publications.count('/tg/sendMessage') == publications.count('/max/messages') == 20
            if outage_seconds:
                tg_release.clear()
                max_events.append(mx('21'))
                await asyncio.wait_for(tg_entered.wait(), 5)
                outage = True
                tg_events.extend([tg(21), tg(22)])
                tg_release.set()
                await until(lambda: snapshot()[0].get('uncertain') == 1 and snapshot()[0].get('sent') == 41)
                await until(lambda: bool(snapshot()[1].get('last_poll_error_max')))
                deadline = time.monotonic() + outage_seconds
                while time.monotonic() < deadline:
                    counts, meta = snapshot()
                    assert child.returncode is None
                    assert time.time() - float(meta['heartbeat']) < 5
                    assert time.time() - float(meta['last_poll_success_tg']) < 5
                    assert meta['cursor_tg'] == '23' and counts['uncertain'] == 1
                    await asyncio.sleep(min(1, max(0, deadline - time.monotonic())))
                outage = False
                await until(lambda: snapshot()[1].get('last_poll_error_max') == '', seconds=35)
                assert snapshot()[0]['uncertain'] == 1
                assert publications.count('/max/messages') == 21
                print(f'Outage OK: {outage_seconds}s MAX failure, live Telegram/heartbeat, recovery, uncertain not retried')
            await stop(child)
            before = list(publications)
            child = await start()
            await until(lambda: snapshot()[1].get('runtime_state') == 'running')
            await asyncio.sleep(0.2)
            assert publications == before
            await stop(child)
            assert snapshot()[1]['runtime_state'] == 'stopped'
            print('Async CLI OK: 20 texts each way via HTTP, durable restart without duplicates, clean SIGTERM')
        finally:
            tg_release.set()
            if child.returncode is None:
                child.kill()
                await child.communicate()
            await runner.cleanup()


def main():
    if len(sys.argv) == 4 and sys.argv[1] == '--child':
        state, base = Path(sys.argv[2]), sys.argv[3]
        cfg = {'TG_CHAT_ID': '-100', 'MAX_CHAT_ID': '-200',
               'TG_BOT_TOKEN': 'synthetic', 'MAX_BOT_TOKEN': 'synthetic'}
        api_type, bridge_type = b.Api, b.Bridge
        b.config = lambda **kwargs: cfg
        b.Api = lambda cfg: api_type(cfg, tg_base=base + '/tg', max_base=base + '/max')
        b.Bridge = lambda *args: bridge_type(*args, send_intervals={'tg': 0, 'max': 0})

        async def doctor(*args, **kwargs):
            return {'tg': '99', 'max': '88'}

        b.doctor = doctor
        return b.main(['run', '--state-dir', str(state)])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--outage-seconds', type=int, default=0)
    args = parser.parse_args()
    if args.outage_seconds < 0:
        parser.error('outage-seconds must be nonnegative')
    asyncio.run(transport_smoke())
    asyncio.run(runtime_smoke(args.outage_seconds))


if __name__ == '__main__':
    sys.exit(main() or 0)
