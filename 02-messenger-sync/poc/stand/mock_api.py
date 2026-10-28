#!/usr/bin/env python3
"""Synthetic Telegram and MAX Bot API for the container stand; nothing real is contacted."""
import asyncio
import json
import os
import time
from pathlib import Path

from aiohttp import web

BASE = os.environ.get('STAND_PUBLIC_BASE', 'http://mockapi:8080')
DATA = Path(os.environ.get('STAND_MOCK_DATA', '/mockdata')) / 'mock.json'
JPEG = b'\xff\xd8\xff\xe0' + b'synthetic-jpeg' * 16
PNG = b'\x89PNG\r\n\x1a\n' + b'synthetic-png' * 16
TG_CHAT, MAX_CHAT = -100, -200


class Mock:
    def __init__(self):
        saved = json.loads(DATA.read_text()) if DATA.exists() else {}
        self.tg_events = saved.get('tg_events', [])
        self.max_events = saved.get('max_events', [])
        self.publications = saved.get('publications', [])
        self.max_publish = saved.get('max_publish', 'ok')
        self.replay = {'tg': False, 'max': False}
        self.polls = {'tg': 0, 'max': 0}
        self.holds, self.entered = {}, {}
        # D2: scripted failures {route: {status, count, body, headers}} and request timestamps.
        self.faults, self.hits, self.fault_times = {}, {}, {}
        self.max_site, self.max_connections = None, set()

    def save(self):
        DATA.parent.mkdir(parents=True, exist_ok=True)
        tmp = DATA.with_suffix('.tmp')
        tmp.write_text(json.dumps({'tg_events': self.tg_events, 'max_events': self.max_events,
                                   'publications': self.publications, 'max_publish': self.max_publish}))
        tmp.replace(DATA)

    async def gate(self, name):
        hold = self.holds.get(name)
        if hold is not None and not hold.is_set():
            self.entered[name] = self.entered.get(name, 0) + 1
            await hold.wait()

    def publish(self, dest, kind, text, reply, status='ok'):
        remote = len(self.publications) + 1 if dest == 'tg' else f'max-copy-{len(self.publications) + 1}'
        self.publications.append({'dest': dest, 'kind': kind, 'text': text, 'reply': reply,
                                  'remote_id': remote, 'status': status})
        self.save()
        return remote


mock = Mock()


def tg_ok(result):
    return web.json_response({'ok': True, 'result': result})


def route_of(request):
    parts = request.path.strip('/').split('/', 1)
    if parts[0] == 'tg':
        return 'tg:' + parts[1]
    if parts[0] == 'max':
        return 'max:/' + parts[1]
    return parts[0]


@web.middleware
async def faults(request, handler):
    route = route_of(request)
    if route == '_ctl':
        return await handler(request)
    if request.transport is not None and request.transport.get_extra_info('sockname')[1] == 8081:
        mock.max_connections.add(request.transport)
    mock.hits.setdefault(route, []).append(time.time())
    fault = mock.faults.get(route)
    if fault and fault['count'] > 0:
        fault['count'] -= 1
        mock.fault_times.setdefault(route, []).append(time.time())
        await request.read()
        return web.json_response(fault.get('body', {}), status=fault['status'], headers=fault.get('headers', {}))
    return await handler(request)


async def max_listen(on):
    if on and mock.max_site is None:
        mock.max_site = web.TCPSite(runner, '0.0.0.0', 8081)
        await mock.max_site.start()
    elif not on and mock.max_site is not None:
        await mock.max_site.stop()
        mock.max_site = None
        # Close idle keep-alive connections too, so the next request must connect (and be refused).
        for transport in mock.max_connections:
            transport.close()
        mock.max_connections.clear()


async def tg_api(request):
    method = request.match_info['method']
    if method in ('getMe', 'getWebhookInfo', 'getChat', 'getChatMember', 'getFile'):
        query = request.query
        return tg_ok({
            'getMe': {'id': 9001, 'username': 'stand_tg_bot', 'can_read_all_group_messages': True},
            'getWebhookInfo': {'url': ''},
            'getChat': {'id': TG_CHAT, 'type': 'supergroup'},
            'getChatMember': {'status': 'administrator'},
            'getFile': {'file_path': f"photos/{query.get('file_id', 'x')}.jpg", 'file_size': len(JPEG)},
        }[method])
    if method == 'getUpdates':
        body = await request.json()
        mock.polls['tg'] += 1
        offset = 0 if mock.replay['tg'] else int(body.get('offset') or 0)
        mock.replay['tg'] = False
        return tg_ok([e for e in mock.tg_events if e['update_id'] >= offset])
    if method in ('sendMessage', 'sendPhoto'):
        if method == 'sendMessage':
            fields = await request.json()
            text = fields.get('text')
            reply = fields.get('reply_parameters')
        else:
            form = await request.post()
            text = form.get('caption')
            reply = json.loads(form['reply_parameters']) if 'reply_parameters' in form else None
            assert form['photo'].file.read().startswith((JPEG[:4], PNG[:8]))
        remote = mock.publish('tg', 'photo' if method == 'sendPhoto' else 'text', text,
                              reply and reply.get('message_id'))
        await mock.gate('tg_publish')
        return tg_ok({'message_id': remote})
    return web.json_response({'ok': False, 'error_code': 404}, status=404)


async def tg_file(request):
    await mock.gate('tg_file')
    return web.Response(body=JPEG)


async def max_api(request):
    path = '/' + request.match_info['path']
    static = {'/me': {'user_id': 9002, 'username': 'stand_max_bot'},
              '/subscriptions': {'subscriptions': []},
              f'/chats/{MAX_CHAT}': {'chat_id': MAX_CHAT, 'type': 'chat'},
              f'/chats/{MAX_CHAT}/members/me': {'is_admin': True, 'permissions': ['read_all_messages']}}
    if path in static:
        return web.json_response(static[path])
    if path == '/updates':
        mock.polls['max'] += 1
        marker = 0 if mock.replay['max'] else int(request.query.get('marker') or 0)
        mock.replay['max'] = False
        return web.json_response({'updates': mock.max_events[marker:], 'marker': len(mock.max_events)})
    if path == '/uploads':
        return web.json_response({'url': BASE + '/maxupload'})
    if path == '/messages':
        body = await request.json()
        kind = 'photo' if body.get('attachments') else 'text'
        reply = (body.get('link') or {}).get('mid')
        if mock.max_publish in ('error_after', 'drop_after'):
            mock.publish('max', kind, body.get('text'), reply)
            if mock.max_publish == 'drop_after':
                request.transport.abort()
                return web.Response()
            # Contradictory body: HTTP 503 must still win over the retryable code.
            return web.json_response({'code': '429'}, status=503)
        if mock.max_publish == '429':
            mock.publish('max', kind, body.get('text'), reply, status='rejected')
            return web.json_response({'code': '429', 'message': 'synthetic rate limit'}, status=429)
        remote = mock.publish('max', kind, body.get('text'), reply)
        await mock.gate('max_publish')
        return web.json_response({'message': {'body': {'mid': remote}}})
    return web.json_response({'code': 'not.found'}, status=404)


async def max_upload(request):
    await request.read()
    return web.json_response({'token': f'synthetic-token-{len(mock.publications) + 1}'})


async def max_media(request):
    return web.Response(body=PNG)


async def ctl(request):
    action = request.match_info['action']
    body = await request.json() if request.can_read_body else {}
    if action == 'inject':
        (mock.tg_events if body['platform'] == 'tg' else mock.max_events).append(body['event'])
    elif action == 'replay':
        mock.replay[body['platform']] = True
    elif action == 'max_publish':
        mock.max_publish = body['mode']
    elif action == 'hold':
        mock.holds[body['name']] = asyncio.Event()
        mock.entered[body['name']] = 0
    elif action == 'release':
        mock.holds.pop(body['name']).set()
    elif action == 'fault':
        mock.faults[body['route']] = body
    elif action == 'max_listen':
        await max_listen(body['on'])
    elif action != 'state':
        return web.json_response({'error': action}, status=404)
    mock.save()
    return web.json_response({'publications': mock.publications, 'polls': mock.polls,
                              'entered': mock.entered, 'max_publish': mock.max_publish,
                              'hits': {k: len(v) for k, v in mock.hits.items()},
                              'last_hits': {k: v[-30:] for k, v in mock.hits.items()},
                              'fault_times': mock.fault_times,
                              'faults_left': {k: v['count'] for k, v in mock.faults.items()}})


app = web.Application(middlewares=[faults])
app.router.add_route('*', '/tg/{method}', tg_api)
app.router.add_get('/tgfile/{path:.*}', tg_file)
app.router.add_route('*', '/max/{path:.*}', max_api)
app.router.add_post('/maxupload', max_upload)
app.router.add_get('/maxmedia/{name}', max_media)
app.router.add_route('*', '/_ctl/{action}', ctl)

runner = web.AppRunner(app)


async def main():
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', 8080).start()
    # MAX Bot API is also served on 8081, so the stand can refuse MAX connections only.
    await max_listen(True)
    await asyncio.Event().wait()


if __name__ == '__main__':
    asyncio.run(main())
