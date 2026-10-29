#!/usr/bin/env python3
"""Scenario steps for the container stand: drive the mock API and check bridge state read-only."""
import email.utils
import json
import os
import re
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



# --- D2: temporary API failures, Retry-After, backoff and recovery ---------------------------

def fault(route, status, count, body=None, headers=None):
    call('fault', route=route, status=status, count=count, body=body or {}, headers=headers or {})


def mock_state():
    return call('state')


def meta(key):
    with db() as conn:
        row = conn.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
    return row and row[0]


def row(source, mid):
    with db() as conn:
        conn.row_factory = sqlite3.Row
        found = conn.execute('SELECT * FROM jobs WHERE source=? AND mid=?', (source, str(mid))).fetchone()
    return found and dict(found)


def first_hit_after(route, moment):
    return min(t for t in mock_state()['last_hits'][route] if t > moment)


def order(*markers):
    texts = [p['text'] or '' for p in mock_state()['publications'] if p['status'] == 'ok']
    positions = [next(i for i, t in enumerate(texts) if m in t) for m in markers]
    assert positions == sorted(positions), (markers, positions)


def step_d2_tg_poll_outage():
    fault('tg:getUpdates', 503, 3, {'ok': False, 'error_code': 503, 'description': 'synthetic outage'})
    until(lambda: mock_state()['faults_left']['tg:getUpdates'] < 3, 'first failed Telegram poll')
    beat = meta('heartbeat')
    inject('tg', tg_event(601, 'd2-tp1'))
    inject('tg', tg_event(602, 'd2-tp2'))
    inject('max', max_event('d2m1', 'd2-mp1'))
    until(lambda: pubs('d2-mp1') and meta('heartbeat') != beat, 'MAX delivery and heartbeat during the outage')
    assert mock_state()['faults_left']['tg:getUpdates'] > 0, 'outage ended before the check'
    assert not pubs('d2-tp1'), 'Telegram event delivered during the poll outage'
    until(lambda: pubs('d2-tp1') and pubs('d2-tp2'), 'Telegram events after recovery', 90)
    order('d2-tp1', 'd2-tp2')
    times = mock_state()['fault_times']['tg:getUpdates']
    gaps = [b - a for a, b in zip(times, times[1:])] + [first_hit_after('tg:getUpdates', times[-1]) - times[-1]]
    assert all(g >= m - 0.3 for g, m in zip(gaps, (2, 4, 8))) and gaps == sorted(gaps), gaps
    assert len(pubs('d2-tp1')) == len(pubs('d2-tp2')) == len(pubs('d2-mp1')) == 1
    print('D2 Telegram poll OK: 3x 503, backoff gaps', [round(g, 1) for g in gaps],
          '; MAX delivery and heartbeat continued; events delivered once in order')


def step_d2_max_connect_outage():
    call('max_listen', on=False)
    time.sleep(1.5)
    reached = mock_state()['hits'].get('max:/messages', 0)
    beat = meta('heartbeat')
    for i, marker in enumerate(('d2-c1', 'd2-c2', 'd2-c3')):
        inject('tg', tg_event(611 + i, marker))
    until(lambda: (row('tg', 611) or {}).get('attempts', 0) >= 3, 'three refused MAX publications', 90)
    head = row('tg', 611)
    assert head['state'] == 'pending' and 'не установлено' in head['error'], head
    assert [row('tg', m)['state'] for m in (612, 613)] == ['pending'] * 2
    assert mock_state()['hits'].get('max:/messages', 0) == reached, 'a refused request reached MAX'
    assert meta('heartbeat') != beat and meta('cursor_tg'), 'Telegram polling/heartbeat stopped'
    call('max_listen', on=True)
    until(lambda: all(pubs(m) for m in ('d2-c1', 'd2-c2', 'd2-c3')), 'MAX delivery after reconnect', 90)
    order('d2-c1', 'd2-c2', 'd2-c3')
    assert all(len(pubs(m)) == 1 for m in ('d2-c1', 'd2-c2', 'd2-c3'))
    print(f"D2 MAX connect OK: {head['attempts']} refused attempts left jobs pending; one copy each, in order")


def step_d2_rate_limit():
    fault('max:/messages', 429, 1, {'code': 'too.many.requests'}, {'Retry-After': '4'})
    inject('tg', tg_event(621, 'd2-r1'))
    until(lambda: pubs('d2-r1'), 'MAX publication after Retry-After')
    moment = mock_state()['fault_times']['max:/messages'][-1]
    max_gap = first_hit_after('max:/messages', moment) - moment
    fault('tg:sendMessage', 429, 1, {'ok': False, 'error_code': 429, 'parameters': {'retry_after': 2}},
          {'Retry-After': email.utils.formatdate(time.time() + 6, usegmt=True)})
    inject('max', max_event('d2r2', 'd2-r2'))
    until(lambda: pubs('d2-r2'), 'Telegram publication after Retry-After')
    moment = mock_state()['fault_times']['tg:sendMessage'][-1]
    tg_gap = first_hit_after('tg:sendMessage', moment) - moment
    assert max_gap >= 4 and tg_gap >= 4, (max_gap, tg_gap)
    assert len(pubs('d2-r1')) == len(pubs('d2-r2')) == 1
    print(f'D2 429 OK: MAX Retry-After 4s -> next try after {max_gap:.1f}s; Telegram HTTP-date over '
          f'retry_after -> {tg_gap:.1f}s; one copy each')


def step_d2_photo_retry():
    inject('tg', tg_event(631, 'd2-pbase'))
    inject('max', max_event('d2pb', 'd2-mbase'))
    until(lambda: pubs('d2-pbase') and pubs('d2-mbase'), 'reply targets')
    fault('tgfile', 503, 3)
    fault('maxmedia', 503, 3)
    inject('tg', tg_event(632, 'd2-photo-tg', reply_to=pub('d2-mbase')['remote_id'], photo=True))
    inject('max', max_event('d2p2', 'd2-photo-max', reply_to=pub('d2-pbase')['remote_id'], photo=True))
    until(lambda: pubs('d2-photo-tg') and pubs('d2-photo-max'), 'photos after download failures', 90)
    left = mock_state()['faults_left']
    assert left['tgfile'] == left['maxmedia'] == 0, left
    to_max, to_tg = pub('d2-photo-tg'), pub('d2-photo-max')
    assert (to_max['dest'], to_max['kind'], to_max['reply']) == ('max', 'photo', 'd2pb'), to_max
    assert (to_tg['dest'], to_tg['kind'], to_tg['reply']) == ('tg', 'photo', 631), to_tg
    uploads = mock_state()['hits'].get('maxupload', 0)
    fault('max:/messages', 400, 2, {'code': 'attachment.not.ready', 'message': 'synthetic'})
    inject('tg', tg_event(633, 'd2-notready', reply_to=pub('d2-mbase')['remote_id'], photo=True))
    until(lambda: pubs('d2-notready'), 'photo after attachment.not.ready', 90)
    state = mock_state()
    assert state['faults_left']['max:/messages'] == 0 and state['hits']['maxupload'] - uploads == 1, state['hits']
    assert pub('d2-notready')['reply'] == 'd2pb' and len(pubs('d2-notready')) == 1
    print('D2 photo OK: 3x download failure each way, then photo+caption+reply once; '
          'attachment.not.ready x2 reused one upload and published once')


def step_d2_uncertain(mode, marker, mid):
    call('max_publish', mode=mode)
    inject('tg', tg_event(int(mid), marker))
    until(lambda: (row('tg', mid) or {}).get('state') == 'uncertain', f'{mode} -> uncertain')
    call('max_publish', mode='ok')
    assert len(pubs(marker)) == 1, pubs(marker)
    print(f'D2 {mode} OK: possible publication marked uncertain after one request', file=sys.stderr)
    print(row('tg', mid)['id'], pub(marker)['remote_id'])


def step_d2_uncertain_stays(marker, mid):
    time.sleep(6)
    current = row('tg', mid)
    assert current['state'] == 'uncertain' and len(pubs(marker)) == 1, (current, pubs(marker))
    print(f'D2 uncertain OK: {marker} not retried after container recreation')


def step_d2_wait_start():
    fault('max:/messages', 429, 1, {'code': 'too.many.requests'}, {'Retry-After': '25'})
    inject('tg', tg_event(651, 'd2-wait', reply_to=pub('d2-mbase')['remote_id']))
    until(lambda: (row('tg', 651) or {}).get('error'), 'rate-limited job')
    current = row('tg', 651)
    assert current['state'] == 'pending' and current['ready'] > time.time() + 15, current
    print(repr(current['ready']))


def step_d2_wait_check(ready):
    current = row('tg', 651)
    assert current['state'] == 'pending' and current['ready'] == float(ready), (current, ready)
    until(lambda: pubs('d2-wait'), 'delivery after the persisted retry time', 90)
    moment = mock_state()['fault_times']['max:/messages'][-1]
    gap = first_hit_after('max:/messages', moment) - moment
    assert gap >= 25 and time.time() >= float(ready), gap
    assert pub('d2-wait')['reply'] == 'd2pb' and len(pubs('d2-wait')) == 1
    assert all(len(pubs(m)) == 1 for m in ('d2-c1', 'd2-tp1', 'd2-photo-tg', 'stand-t1')), 'confirmed sends repeated'
    print(f'D2 recreate OK: retry time kept across recreation, retried after {gap:.1f}s with reply link; '
          'confirmed sends not repeated')


# --- D3: crash points, deduplication, echo and owner lock ----------------------------------

def long_text(marker, tail=''):
    return marker + ' ' + 'слово ' * 700 + tail  # > 3500 chars: two parts


def parts(source, mid):
    with db() as conn:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute('SELECT * FROM jobs WHERE source=? AND mid=? ORDER BY part',
                                              (source, str(mid)))]


def quarantined():
    with db() as conn:
        return conn.execute('SELECT count(*) FROM quarantine').fetchone()[0]


def total_pubs():
    return len(mock_state()['publications'])


def step_d3_accept_start():
    bad = {'update_id': 703, 'message': {'message_id': 703, 'chat': {'id': -100}, 'from': {'id': 10},
                                         'photo': [{'file_size': 1}]}}
    print(meta('cursor_tg'), quarantined(), total_pubs())
    call('inject_many', platform='tg', events=[tg_event(701, 'd3-a1'), tg_event(702, long_text('d3-a2')),
                                               bad, tg_event(704, 'd3-a4 d3-freeze-accept')])


def step_d3_accept_frozen(cursor, quarantine, total):
    assert str(meta('cursor_tg')) == cursor, (meta('cursor_tg'), cursor)
    assert not any(parts('tg', m) for m in (701, 702, 704)) and quarantined() == int(quarantine)
    assert total_pubs() == int(total)
    print('D3 accept frozen: batch inside the open transaction; cursor, jobs and quarantine not committed')


def step_d3_accept_check(cursor, quarantine, total):
    until(lambda: all(pubs(m) for m in ('d3-a1', 'd3-a2', 'd3-a4')), 'redelivered batch')
    long = parts('tg', 702)
    until(lambda: all(p['state'] == 'sent' for p in parts('tg', 702)), 'all parts of the long message')
    time.sleep(3)
    assert len(long) == 2 and all(len(pubs(m)) == 1 for m in ('d3-a1', 'd3-a2', 'd3-a4'))
    assert total_pubs() == int(total) + 4, (total_pubs(), total)
    assert quarantined() == int(quarantine) + 1 and meta('cursor_tg') == '705', (quarantined(), meta('cursor_tg'))
    order('d3-a1', 'd3-a2', 'd3-a4')
    print(f'D3 accept OK: after SIGKILL inside the accept transaction (cursor {cursor}) the redelivered '
          'batch gave 3 messages (one in 2 parts) once, in order, and 1 quarantine entry')


def step_d3_echo():
    own_tg = tg_event(705, 'd3-echo-tg')
    own_tg['message']['from'] = {'id': 9001, 'is_bot': True, 'first_name': 'stand'}
    own_max = max_event('d3echo', 'd3-echo-max')
    own_max['message']['sender'] = {'user_id': 9002, 'is_bot': True, 'name': 'stand'}
    other = tg_event(706, 'd3-otherbot')
    other['message']['from'] = {'id': 555, 'is_bot': True, 'first_name': 'Other'}
    for event in (own_tg, other, tg_event(707, 'd3-same'), tg_event(708, 'd3-same')):
        inject('tg', event)
    inject('max', own_max)
    until(lambda: pubs('d3-otherbot') and len(pubs('d3-same')) == 2, 'other bot and identical texts')
    time.sleep(3)
    assert not pubs('d3-echo-tg') and not pubs('d3-echo-max')
    assert not parts('tg', 705) and not parts('max', 'd3echo') and len(pubs('d3-same')) == 2
    print('D3 echo OK: own-bot echoes ignored both ways; another bot and two identical human texts delivered')


def step_d3_photo_start():
    call('hold', name='tg_file')
    call('hold', name='max_media')
    inject('tg', tg_event(711, 'd3-p1', reply_to=pub('stand-m1')['remote_id'], photo=True))
    inject('tg', tg_event(712, 'd3-p2'))
    inject('max', max_event('d3pm1', 'd3-pm1', reply_to=pub('stand-t1')['remote_id'], photo=True))
    inject('max', max_event('d3pm2', 'd3-pm2'))
    until(lambda: mock_state()['entered'].get('tg_file') and mock_state()['entered'].get('max_media')
          and parts('tg', 712) and parts('max', 'd3pm2'), 'photo downloads held in both directions')
    keys = (('tg', 711), ('tg', 712), ('max', 'd3pm1'), ('max', 'd3pm2'))
    assert [parts(s, m)[0]['state'] for s, m in keys] == ['pending'] * 4
    assert not any(pubs(m) for m in ('d3-p1', 'd3-p2', 'd3-pm1', 'd3-pm2'))
    print('D3 photo: both photo downloads in progress, photos and texts behind them pending')


def step_d3_photo_release():
    call('release', name='tg_file')
    call('release', name='max_media')


def step_d3_photo_check():
    until(lambda: all(pubs(m) for m in ('d3-p1', 'd3-p2', 'd3-pm1', 'd3-pm2')), 'photos after recreation')
    time.sleep(2)
    assert all(len(pubs(m)) == 1 for m in ('d3-p1', 'd3-p2', 'd3-pm1', 'd3-pm2'))
    assert pub('d3-p1')['kind'] == 'photo' and pub('d3-p1')['reply'] == 'm1', pub('d3-p1')
    assert pub('d3-pm1')['kind'] == 'photo' and pub('d3-pm1')['reply'] == 1, pub('d3-pm1')
    order('d3-p1', 'd3-p2')
    order('d3-pm1', 'd3-pm2')
    print('D3 photo OK: SIGKILL during photo preparation both ways; photo+caption+reply and the text '
          'behind it delivered once, in order')


def step_d3_inject(platform, mid, text, tail=''):
    if text == 'long':
        text = long_text(mid, tail)
    reply = pub('stand-m1')['remote_id'] if platform == 'tg' else None
    inject(platform, tg_event(int(mid[2:]), text, reply_to=reply) if platform == 'tg' else max_event(mid, text))


def check_parts(source, mid, states, marker, count):
    current = parts(source, mid)
    assert [p['state'] for p in current] == states.split(','), (mid, current)
    assert len(pubs(marker)) == int(count), (marker, pubs(marker))
    remote = {str(p['remote_id']) for p in mock_state()['publications']}
    assert all(str(p['remote_id']) in remote for p in current if p['state'] == 'sent'), current
    assert len({p['remote_id'] for p in current if p['remote_id']}) == len([p for p in current if p['remote_id']])
    return current


def step_d3_frozen(source, mid, states, marker, count):
    check_parts(source, mid, states, marker, count)
    print(f'D3 crash point: {source} {mid} parts {states}, {count} publication(s) of {marker}')


def step_d3_kept(source, mid, states, marker, count, replay=''):
    until(lambda: [p['state'] for p in parts(source, mid)] == states.split(','), f'{mid} -> {states}')
    if replay:
        polls = mock_state()['polls'][replay]
        call('replay', platform=replay)
        until(lambda: mock_state()['polls'][replay] >= polls + 3, 'replayed polls')
    time.sleep(4)
    current = check_parts(source, mid, states, marker, count)
    assert (STATE / 'process.lock').exists()
    print(f'D3 recovery OK: {source} {mid} parts {states}, still {count} publication(s) of {marker}'
          + (f', {replay} event redelivered without new jobs' if replay else ''), file=sys.stderr)
    print(current[-1]['id'], (pubs(marker) or [{'remote_id': '-'}])[0]['remote_id'])


def step_d3_sent_reply():
    inject('max', max_event('d3c2', 'd3-c2', reply_to=pub('tg741')['remote_id']))
    until(lambda: pubs('d3-c2'), 'reply to the copy sent before the crash')
    assert pub('d3-c2')['reply'] == 741 and pub('tg741')['reply'] == 'm1', (pub('d3-c2'), pub('tg741'))
    print('D3 sent OK: remote ID and reply links kept after SIGKILL right after the sent COMMIT')


def step_d3_other_direction():
    inject('max', max_event('d3x1', 'd3-x1'))
    until(lambda: pubs('d3-x1'), 'opposite direction while a part is uncertain')
    print('D3 multipart OK: the uncertain part blocks only its direction')


def logical_snapshot(path):
    """Digest of user_version, schema and every row in one read transaction.

    WAL/SHM sidecar files and checkpoints may change on a normal read; the data may not."""
    import hashlib
    conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    try:
        conn.execute('BEGIN')
        digest = hashlib.sha256(b'user_version=%d\n' % conn.execute('PRAGMA user_version').fetchone()[0])
        for line in conn.iterdump():
            digest.update(line.encode() + b'\n')
        conn.rollback()
    finally:
        conn.close()
    return digest.hexdigest()[:16]


def step_d3_snapshot():
    print(logical_snapshot(STATE / 'bridge.sqlite3'))


def step_d3_confirm_reply():
    assert row('max', 'd3u1')['state'] == 'sent' and len(pubs('maxd3u1')) == 1
    inject('tg', tg_event(791, 'd3-r1', reply_to=pub('maxd3u1')['remote_id']))
    until(lambda: pubs('d3-r1'), 'reply to the confirmed copy')
    assert pub('d3-r1')['reply'] == 'd3u1', pub('d3-r1')
    print('D3 confirm OK: operator confirmation restored the reply link without a new publication')


def step_d3_health_only(code):
    # The quarantined input of the accept batch stays open by design; nothing else may be wrong.
    def codes():
        snap = b.read_health(STATE, float(os.environ['HEALTH_STALE_AFTER']), float(os.environ['HEALTH_QUEUE_AGE']))
        return sorted({i['code'] for i in snap['issues']})
    until(lambda: codes() == [code], f'health issues == [{code}]')
    print(f'health OK: only {code} (open quarantine entry from the D3 accept batch)')


def step_d3_hits(route):
    print(mock_state()['hits'].get(route, 0))


# --- D4: one stalled worker while the heartbeat and the other workers continue -------------

def health_issues():
    snap = b.read_health(STATE, float(os.environ['HEALTH_STALE_AFTER']), float(os.environ['HEALTH_QUEUE_AGE']))
    return snap, snap['issues']


def alerts(kind, worker):
    path = STATE / 'alerts.jsonl'
    events = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
    return [e for e in events if e['kind'] == kind and e['issue'].get('code') == 'worker_stalled'
            and e['issue'].get('worker') == worker]


D4_SEQ = {'n': 0}


def step_d4_stalled(worker, seq):
    started = time.monotonic()
    until(lambda: any(i['code'] == 'worker_stalled' and i.get('worker') == worker for i in health_issues()[1]),
          f'worker_stalled {worker}', 120)
    took = time.monotonic() - started
    snap, issues = health_issues()
    issue = next(i for i in issues if i['code'] == 'worker_stalled')
    kind, platform = worker.split(':')
    assert issue['platform'] == platform and issue['phase'] == ('poll' if kind == 'poll' else 'deliver'), issue
    assert [i for i in issues if i['code'] == 'worker_stalled'] == [issue], issues
    assert 'heartbeat_stale' not in {i['code'] for i in issues}, issues
    assert all(w['state'] == 'ok' for n, w in snap['workers'].items() if n != worker), snap['workers']
    # An independent direction keeps delivering: it needs neither the frozen poller nor sender.
    source = 'max' if worker in ('poll:tg', 'send:max') else 'tg'
    marker = f'd4-{worker.replace(":", "-")}'
    inject(source, tg_event(800 + int(seq), marker) if source == 'tg' else max_event('d4' + seq, marker))
    until(lambda: pubs(marker), f'{source} delivery while {worker} is stalled')
    until(lambda: alerts('problem', worker), f'monitor problem for {worker}')
    print(f'D4 stalled OK: {worker} reported (platform {platform}, phase {issue["phase"]}) after {took:.0f}s '
          f'with a fresh heartbeat; {source} direction delivered; monitor problem event written')


def step_d4_recovered(worker):
    until(lambda: not any(i['code'] == 'worker_stalled' for i in health_issues()[1]), f'{worker} recovered', 60)
    until(lambda: alerts('recovery', worker), f'monitor recovery for {worker}', 60)
    assert len(alerts('problem', worker)) == 1 and len(alerts('recovery', worker)) == 1, worker
    print(f'D4 recovered OK: {worker} healthy in the new container; exactly one problem and one recovery event')

LOG_MARKERS = re.compile(r'synthetic-(tg|max)-token|Authorization|Tester|stand-[tm][0-9]|d[234]-|mockapi|tgfile|maxmedia')
# Events the stand scenarios produce; delivery_failed (TLS, 4xx) is covered by test_async only.
LOG_EVENTS = ('preflight_ok', 'bridge_started', 'bridge_stopped', 'delivery_sent', 'delivery_retry_scheduled',
              'delivery_uncertain', 'poll_failed', 'poll_recovered', 'monitor_started')


def check_logs(lines, required=()):
    """Every application line is a JSON object with the D4 fields; only stand wrapper lines are text."""
    records = []
    for number, line in enumerate(lines, 1):
        line = line.rstrip('\n')
        if LOG_MARKERS.search(line):
            raise AssertionError(f'log line {number}: private marker')
        if line.startswith('stand: '):
            continue
        try:
            record = json.loads(line)
        except ValueError:
            raise AssertionError(f'log line {number}: not JSON') from None
        if not isinstance(record, dict):
            raise AssertionError(f'log line {number}: not a JSON object')
        for key in ('timestamp', 'component', 'event', 'run_id'):
            if not isinstance(record.get(key), str) or not record[key]:
                raise AssertionError(f'log line {number}: {key} missing or empty')
        if record.get('level') not in ('debug', 'info', 'warning', 'error') or \
                record['component'] not in ('bridge', 'monitor'):
            raise AssertionError(f'log line {number}: bad level or component')
        records.append(record)
    missing = sorted(set(required) - {r['event'] for r in records})
    if missing:
        raise AssertionError(f'log events missing: {missing}')
    return records


def step_d4_logs():
    records = check_logs(sys.stdin.readlines(), LOG_EVENTS)
    runs = {r['run_id'] for r in records}
    print(f'logs OK: {len(records)} JSON records from {len(runs)} runs, no synthetic tokens, texts, names or URLs')


def step_d5_thresholds():
    disk = os.statvfs(STATE)
    free = disk.f_bavail * disk.f_frsize
    assert free > 96 * 1024**2, 'D5 pressure filesystem needs >96 MiB free to reserve and release 64 MiB safely'
    print(free - 16 * 1024**2, free - 32 * 1024**2)


def step_d5_pressure():
    pressure = STATE / 'd5-owned-disk-pressure.bin'
    assert not pressure.exists(), 'refuse to overwrite an existing stand fixture'

    def disk_events(kind):
        conn = sqlite3.connect(f'file:{STATE / "monitor.sqlite3"}?mode=ro', uri=True)
        try:
            values = [json.loads(r[0]) for r in conn.execute('SELECT payload FROM alerts WHERE delivered=1')]
            return [v for v in values if v['kind'] == kind and v['issue']['key'] == 'storage:free'
                    and v['issue'].get('severity') == 'critical']
        finally:
            conn.close()

    def paused():
        view, issues = health_issues()
        return (any(i['code'] == 'storage_free' for i in issues)
                and all(view['workers'].get('poll:' + p, {}).get('phase') == 'storage_wait' for p in ('tg', 'max')))

    until(lambda: all('poll:' + p in health_issues()[0]['workers'] for p in ('tg', 'max')), 'new pollers ready')
    try:
        # Allocated bytes on the real state volume, not a sparse file or a stubbed disk measurement.
        with pressure.open('xb') as stream:
            for _ in range(64):
                stream.write(b'x' * 1024**2)
            stream.flush()
            os.fsync(stream.fileno())
        until(paused, 'storage critical and both pollers paused', 30)
        until(lambda: len(disk_events('problem')) == 1, 'one disk problem')
        before = snapshot_data()
        heartbeat = health_issues()[0]['runtime']['heartbeat_at']
        polls = call('state')['polls']
        inject('tg', tg_event(9001, 'd5-storage-tg', reply_to=pub('stand-m1')['remote_id']))
        inject('max', max_event('d5storage', 'd5-storage-max', reply_to=pub('stand-t1')['remote_id']))
        time.sleep(3)
        assert paused()
        assert call('state')['polls'] == polls, 'poll requests continued during storage pressure'
        assert snapshot_data() == before, 'queue or publications changed during intake pause'
        assert health_issues()[0]['runtime']['heartbeat_at'] > heartbeat, 'heartbeat stopped'
        assert len(disk_events('problem')) == 1 and not disk_events('recovery')
    finally:
        pressure.unlink(missing_ok=True)
    until(lambda: pubs('d5-storage-tg') and pubs('d5-storage-max'), 'automatic resume and delivery', 30)
    until(lambda: len(disk_events('recovery')) == 1, 'confirmed storage recovery', 30)
    assert len(disk_events('problem')) == len(disk_events('recovery')) == 1
    assert pub('d5-storage-tg')['reply'] == 'm1'
    assert pub('d5-storage-max')['reply'] == 1
    assert job('tg', 9001)['state'] == job('max', 'd5storage')['state'] == 'sent'
    print('D5 storage OK: real 64 MiB pressure, paused intake, live heartbeat, automatic resume, replies, one problem/recovery')


def step_d5_rotation():
    # Reuse production sink/outbox regression tests inside an isolated container. Includes real SIGKILL.
    import unittest

    from test_storage import RotationTests
    result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(RotationTests))
    assert result.wasSuccessful(), 'D5 rotation regression failed'
    print('D5 rotation OK: bounded JSON files, expiration, safe paths, partial-line repair, durable outbox and real SIGKILL')


if __name__ == '__main__':
    globals()['step_' + sys.argv[1].replace('-', '_')](*sys.argv[2:])
