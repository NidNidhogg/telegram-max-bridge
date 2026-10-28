#!/usr/bin/env python3
"""Scenario steps for the container stand: drive the mock API and check bridge state read-only."""
import json
import os
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bridge as b  # noqa: E402

MOCK = os.environ.get('STAND_API_BASE', 'http://mockapi:8080')
STATE = Path('/state')


def call(action, **body):
    request = urllib.request.Request(f'{MOCK}/_ctl/{action}', data=json.dumps(body).encode(),
                                     headers={'Content-Type': 'application/json'}, method='POST')
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def tg_event(mid, text, reply_to=None, photo=False):
    msg = {'message_id': mid, 'chat': {'id': -100}, 'from': {'id': 10, 'first_name': 'Tester'}}
    if photo:
        msg.update(photo=[{'file_id': f'file-{mid}', 'file_size': 300, 'width': 10}], caption=text)
    else:
        msg['text'] = text
    if reply_to is not None:
        msg['reply_to_message'] = {'message_id': reply_to, 'text': 'quoted'}
    return {'update_id': mid, 'message': msg}


def max_event(mid, text, reply_to=None, photo=False):
    body = {'mid': mid, 'text': text}
    if photo:
        body['attachments'] = [{'type': 'image', 'payload': {'url': f'{MOCK}/maxmedia/{mid}.png'}}]
    msg = {'recipient': {'chat_id': -200}, 'sender': {'user_id': 20, 'first_name': 'Tester'}, 'body': body}
    if reply_to is not None:
        msg['link'] = {'type': 'reply', 'message': {'mid': reply_to, 'text': 'quoted'}}
    return {'update_type': 'message_created', 'message': msg}


def inject(platform, event):
    call('inject', platform=platform, event=event)


def pubs(marker, status='ok'):
    return [p for p in call('state')['publications'] if marker in (p['text'] or '') and p['status'] == status]


def pub(marker):
    found = pubs(marker)
    assert len(found) == 1, (marker, found)
    return found[0]


def until(predicate, what, seconds=60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.3)
    raise AssertionError('timeout waiting for ' + what)


def db():
    return sqlite3.connect(f'file:{STATE / "bridge.sqlite3"}?mode=ro', uri=True)


def job(source, mid):
    with db() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute('SELECT id,state,attempts,remote_id FROM jobs WHERE source=? AND mid=?',
                            (source, str(mid))).fetchall()
    assert len(rows) == 1, (source, mid, [dict(r) for r in rows])
    return dict(rows[0])


def snapshot_data():
    with db() as conn:
        return {'jobs': dict(conn.execute('SELECT state,count(*) FROM jobs GROUP BY state')),
                'inbox': conn.execute('SELECT count(*) FROM inbox').fetchone()[0],
                'mapped': conn.execute('SELECT count(*) FROM jobs WHERE remote_id IS NOT NULL').fetchone()[0],
                'publications': len(call('state')['publications'])}


def step_flows():
    inject('tg', tg_event(1, 'stand-t1'))
    inject('max', max_event('m1', 'stand-m1'))
    inject('tg', tg_event(2, 'stand-t2', photo=True))
    inject('max', max_event('m2', 'stand-m2', photo=True))
    until(lambda: all(pubs(m) for m in ('stand-t1', 'stand-m1', 'stand-t2', 'stand-m2')), 'text/photo')
    assert pub('stand-t1')['dest'] == 'max' and pub('stand-m1')['dest'] == 'tg'
    assert pub('stand-t2')['dest'] == 'max' and pub('stand-t2')['kind'] == 'photo'
    assert pub('stand-m2')['dest'] == 'tg' and pub('stand-m2')['kind'] == 'photo'
    inject('tg', tg_event(3, 'stand-t3', reply_to=pub('stand-m1')['remote_id']))
    inject('max', max_event('m3', 'stand-m3', reply_to=pub('stand-t1')['remote_id']))
    until(lambda: pubs('stand-t3') and pubs('stand-m3'), 'replies')
    assert pub('stand-t3')['reply'] == 'm1', pub('stand-t3')
    assert pub('stand-m3')['reply'] == 1, pub('stand-m3')
    print('flows OK: text, photo and reply in both directions')
    before = call('state')
    call('replay', platform='tg')
    call('replay', platform='max')
    until(lambda: all(call('state')['polls'][p] >= before['polls'][p] + 3 for p in ('tg', 'max')), 'replayed polls')
    after = snapshot_data()
    assert after['publications'] == len(before['publications']) == 6, after
    assert after['jobs'] == {'sent': 6} and after['inbox'] == 6, after
    print('dedup OK: redelivered events of both platforms produced no second delivery')


def step_pending_start():
    call('max_publish', mode='429')
    inject('tg', tg_event(4, 'stand-t4', reply_to=pub('stand-m2')['remote_id']))
    until(lambda: pubs('stand-t4', 'rejected') and job('tg', 4)['attempts'] >= 1, 'rejected attempt')
    print('pending OK: MAX rejects publication with 429, job', job('tg', 4))


def step_pending_check():
    current = job('tg', 4)
    assert current['state'] == 'pending' and current['attempts'] >= 1, current
    assert not pubs('stand-t4'), 'pending job was delivered unexpectedly'
    print('recreate OK: pending job survived container recreation', current)


def step_pending_release():
    call('max_publish', mode='ok')
    until(lambda: pubs('stand-t4'), 'pending job delivery', 120)
    assert pub('stand-t4')['reply'] == 'm2', pub('stand-t4')
    assert job('tg', 4)['state'] == 'sent'
    assert len(pubs('stand-t1')) == len(pubs('stand-m1')) == 1, 'confirmed sends were repeated'
    print('resume OK: pending job delivered once after recreation with reply link to m2')


def step_photo_hold():
    call('hold', name='tg_file')
    inject('tg', tg_event(5, 'stand-t5', photo=True))
    until(lambda: call('state')['entered'].get('tg_file'), 'photo download in progress')
    print('photo preparation is held at download')


def step_photo_check():
    current = job('tg', 5)
    assert current['state'] == 'pending', current
    assert not pubs('stand-t5') and not pubs('stand-t5', 'rejected')
    call('release', name='tg_file')
    print('photo-stop OK: shutdown during photo preparation left the job pending', current)


def step_photo_done():
    until(lambda: pubs('stand-t5'), 'photo delivery after restart', 120)
    assert pub('stand-t5')['kind'] == 'photo' and job('tg', 5)['state'] == 'sent'
    print('photo-resume OK: photo delivered once after restart')


def step_publish_hold():
    call('hold', name='tg_publish')
    inject('max', max_event('m4', 'stand-m4'))
    until(lambda: call('state')['entered'].get('tg_publish'), 'publication in progress')
    print('publication is held after the request reached Telegram')


def step_publish_check():
    current = job('max', 'm4')
    assert current['state'] == 'uncertain', current
    call('release', name='tg_publish')
    assert len(pubs('stand-m4')) == 1
    print('publish-stop OK: shutdown during publication marked the job uncertain', current)


def step_uncertain_stays():
    time.sleep(8)
    assert len(pubs('stand-m4')) == 1, 'uncertain job was retried automatically'
    current = job('max', 'm4')
    assert current['state'] == 'uncertain', current
    inject('tg', tg_event(6, 'stand-t6'))
    until(lambda: pubs('stand-t6'), 'opposite direction delivery')
    print('uncertain OK: not retried after restart; opposite direction still delivers', file=sys.stderr)
    print(current['id'], pub('stand-m4')['remote_id'])


def step_confirmed():
    current = job('max', 'm4')
    assert current['state'] == 'sent' and len(pubs('stand-m4')) == 1, current
    print('confirm OK: operator confirmed uncertain job without a second publication', current)


def step_reply_after_restart():
    inject('max', max_event('m5', 'stand-m5', reply_to=pub('stand-t1')['remote_id']))
    inject('tg', tg_event(7, 'stand-t7', reply_to=pub('stand-m1')['remote_id']))
    until(lambda: pubs('stand-m5') and pubs('stand-t7'), 'replies after restart')
    assert pub('stand-m5')['reply'] == 1 and pub('stand-t7')['reply'] == 'm1'
    print('reply-links OK: reply mapping survived down/up in both directions')


def step_snapshot():
    print(json.dumps(snapshot_data(), sort_keys=True))


def step_health(expected):
    snap = b.read_health(STATE, float(os.environ['HEALTH_STALE_AFTER']), float(os.environ['HEALTH_QUEUE_AGE']))
    codes = sorted({i['code'] for i in snap['issues']})
    if expected == 'ok':
        assert snap['ok'], codes
    else:
        assert not snap['ok'] and expected in codes, codes
    print(f'health OK: expected {expected}, issues {codes}')


def step_alerts():
    events = [json.loads(line) for line in (STATE / 'alerts.jsonl').read_text().splitlines()]
    text = json.dumps(events, ensure_ascii=False)
    for code in ('delivery_blocked', 'heartbeat_stale'):
        assert code in text, code
    assert any('recover' in json.dumps(e) for e in events), 'no recovery events'
    print(f'monitor OK: {len(events)} alert events in shared volume incl. problems and recoveries')


if __name__ == '__main__':
    globals()['step_' + sys.argv[1].replace('-', '_')](*sys.argv[2:])
