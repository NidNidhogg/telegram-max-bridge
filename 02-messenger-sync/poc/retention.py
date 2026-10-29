"""Single-owner, bounded SQLite retention. No API calls and no schema migration."""
import json
import math
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass

CLEARED = '{"content_cleared":true}'
CLOSED = ('sent', 'skipped')
DEFAULTS = {'RETENTION_CONTENT_ENABLED': 1, 'RETENTION_CONTENT_DAYS': 30,
            'RETENTION_HISTORY_DAYS': 90, 'MAINTENANCE_INTERVAL': 60,
            'MAINTENANCE_BATCH_SIZE': 100, 'MAINTENANCE_BUDGET_MS': 50}


@dataclass(frozen=True)
class Settings:
    content_enabled: int = 1
    content_days: int = 30
    history_days: int = 90
    interval: int = 60
    batch_size: int = 100
    budget_ms: int = 50

    @classmethod
    def load(cls, values):
        parsed = []
        bounds = [(0, 1), (1, 3650), (1, 3650), (1, 3600), (1, 1000), (5, 200)]
        for (key, default), (low, high) in zip(DEFAULTS.items(), bounds):
            value = str(values.get(key, default)).strip()
            if not value.isascii() or not value.isdecimal() or len(value) > 10 or not low <= int(value) <= high:
                raise ValueError(f'{key}: значение вне допустимого диапазона')
            parsed.append(int(value))
        result = cls(*parsed)
        if result.history_days < result.content_days:
            raise ValueError('RETENTION: история должна храниться не меньше содержимого')
        return result


def stamp(value, now):
    return (not isinstance(value, bool) and isinstance(value, (int, float))
            and math.isfinite(value) and 0 < value <= now)


def expired(row, cutoff, now):
    return (stamp(row['created'], now) and stamp(row['updated'], now)
            and row['created'] <= row['updated'] <= cutoff)


def group_closed(db, job, cutoff, now):
    rows = db.execute('SELECT state,created,updated FROM jobs WHERE source=? AND chat=? AND mid=?',
                      (job['source'], job['chat'], job['mid'])).fetchall()
    return bool(rows) and all(row['state'] in CLOSED and expired(row, cutoff, now) for row in rows)


def load_report(db, kind):
    table = 'meta' if kind == 'bridge' else 'monitor_state'
    row = db.execute(f"SELECT value FROM {table} WHERE key='maintenance'").fetchone()
    if not row:
        return {}
    value = json.loads(row[0])
    if not isinstance(value, dict):
        raise ValueError('Invalid maintenance state')
    cursors = value.get('cursors', {})
    names = ('jobs', 'quarantine', 'audit') if kind == 'bridge' else ('alerts',)
    if (not isinstance(cursors, dict) or any(key not in names or isinstance(cursor, bool)
            or not isinstance(cursor, int) or cursor < 0 for key, cursor in cursors.items())):
        raise ValueError('Invalid maintenance cursor')
    return value


def metrics(db):
    page_size = db.execute('PRAGMA page_size').fetchone()[0]
    return {'page_size': page_size, 'pages': db.execute('PRAGMA page_count').fetchone()[0],
            'reusable_pages': db.execute('PRAGMA freelist_count').fetchone()[0]}


@contextmanager
def bounded(db, budget_ms, stopping):
    deadline = time.monotonic() + budget_ms / 1000
    previous = db.execute('PRAGMA busy_timeout').fetchone()[0]
    db.execute('PRAGMA busy_timeout=0')
    db.set_progress_handler(lambda: int(stopping() or time.monotonic() >= deadline), 100)
    try:
        yield
    finally:
        db.set_progress_handler(None, 0)
        db.execute(f'PRAGMA busy_timeout={previous}')


class Maintainer:
    def __init__(self, db, kind, policy):
        if kind not in ('bridge', 'monitor'):
            raise ValueError('Unknown maintenance owner')
        self.db, self.kind, self.policy = db, kind, policy

    def page(self, table, cursor):
        # Fixed table names only. Walk by primary key: completed tombstones never force a full scan.
        assert table in ('jobs', 'quarantine', 'audit', 'alerts')
        rows = self.db.execute(f'SELECT * FROM {table} WHERE id>? ORDER BY id LIMIT ?',
                               (cursor, self.policy.batch_size)).fetchall()
        return rows, rows[-1]['id'] if rows else 0

    def bridge_plan(self, report, now):
        db, policy = self.db, self.policy
        content_cutoff, history_cutoff = now - policy.content_days * 86400, now - policy.history_days * 86400
        actions = {'jobs': [], 'quarantine': [], 'audit': []}
        cursors = report.get('cursors', {})
        next_cursors, scanned = {}, 0
        for table in actions:
            rows, next_cursors[table] = self.page(table, cursors.get(table, 0))
            scanned += len(rows)
            for row in rows:
                if table == 'jobs':
                    if (policy.content_enabled and row['payload'] != CLEARED and row['state'] in CLOSED
                            and group_closed(db, row, content_cutoff, now)):
                        actions[table].append(row['id'])
                elif table == 'quarantine':
                    if (policy.content_enabled and row['state'] == 'resolved' and row['payload'] != CLEARED
                            and expired(row, content_cutoff, now)):
                        # A resolved event must not be scrubbed while the corresponding direction
                        # still has unresolved work: TG event_key is update_id, not message_id.
                        active = db.execute("SELECT 1 FROM jobs WHERE source=? AND chat=? AND state NOT IN ('sent','skipped') LIMIT 1",
                                            (row['platform'], row['chat'])).fetchone()
                        if not active:
                            actions[table].append(row['id'])
                elif stamp(row['created'], now) and row['created'] <= history_cutoff:
                    if row['scope'] == 'job':
                        job = db.execute('SELECT * FROM jobs WHERE id=?', (row['target_id'],)).fetchone()
                        eligible = job is not None and group_closed(db, job, history_cutoff, now)
                    elif row['scope'] == 'quarantine':
                        item = db.execute('SELECT * FROM quarantine WHERE id=?', (row['target_id'],)).fetchone()
                        eligible = item is not None and item['state'] == 'resolved' and expired(item, history_cutoff, now)
                        if eligible:
                            eligible = not db.execute("SELECT 1 FROM jobs WHERE source=? AND chat=? AND state NOT IN ('sent','skipped') LIMIT 1",
                                                      (item['platform'], item['chat'])).fetchone()
                    else:
                        eligible = False
                    if eligible:
                        actions[table].append(row['id'])
        return actions, next_cursors, scanned

    def monitor_plan(self, report, now):
        actions = {'alerts': [], 'legacy': []}
        protected = set()
        for key in ('issues', 'absent'):
            row = self.db.execute('SELECT value FROM monitor_state WHERE key=?', (key,)).fetchone()
            if key == 'issues' and row is None and self.db.execute('SELECT 1 FROM alerts LIMIT 1').fetchone():
                raise ValueError('Current monitor state is unknown')
            known = json.loads(row[0]) if row else {}
            if not isinstance(known, dict):
                raise ValueError('Invalid monitor state')
            protected.update(known)
        rows, cursor = self.page('alerts', report.get('cursors', {}).get('alerts', 0))
        for row in rows:
            if row['delivered'] != 1:
                continue
            try:
                payload = json.loads(row['payload'])
                key = payload['issue']['key']
                if (not isinstance(key, str) or not key or not stamp(payload['detected_at'], now)
                        or payload.get('kind') not in ('problem', 'recovery') or payload.get('event_id') != row['event_id']):
                    continue
                since = payload.get('retention_since')
                if since is None:
                    # Older versions did not record ACK time. Start a conservative new 90-day
                    # retention window at the first observation, never infer an old ACK.
                    payload['retention_since'] = now
                    actions['legacy'].append((json.dumps(payload, ensure_ascii=False, sort_keys=True), row['id']))
                elif (stamp(since, now) and payload['detected_at'] <= since <= now - self.policy.history_days * 86400
                      and key not in protected):
                    actions['alerts'].append(row['id'])
            except (ValueError, KeyError, TypeError):
                continue  # Unknown dates/payloads are not proof of expiry.
        return actions, {'alerts': cursor}, len(rows)

    def plan(self, previous, now):
        return self.bridge_plan(previous, now) if self.kind == 'bridge' else self.monitor_plan(previous, now)

    def run(self, *, now=None, apply=True, stopping=lambda: False):
        now = time.time() if now is None else now
        if not stamp(now, now):
            raise ValueError('Invalid maintenance time')
        if self.db.in_transaction:
            raise ValueError('Maintenance requires an idle connection')
        start = time.monotonic()
        result = {'owner': self.kind, 'at': now, 'state': 'ok', 'reason': None, 'applied': apply,
                  'counts': {}, 'scanned': 0, 'cursors': {}}
        try:
            with bounded(self.db, self.policy.budget_ms, stopping):
                self.db.execute('BEGIN IMMEDIATE' if apply else 'BEGIN')
                previous = load_report(self.db, self.kind)
                actions, cursors, scanned = self.plan(previous, now)
                result.update(counts={key: len(value) for key, value in actions.items()}, cursors=cursors, scanned=scanned)
                if apply:
                    if self.kind == 'bridge':
                        self.db.executemany('UPDATE jobs SET payload=? WHERE id=?', [(CLEARED, jid) for jid in actions['jobs']])
                        self.db.executemany("UPDATE quarantine SET payload=?,error='content_cleared' WHERE id=?",
                                            [(CLEARED, qid) for qid in actions['quarantine']])
                        self.db.executemany('DELETE FROM audit WHERE id=?', [(jid,) for jid in actions['audit']])
                    else:
                        self.db.executemany('DELETE FROM alerts WHERE id=?', [(jid,) for jid in actions['alerts']])
                        self.db.executemany('UPDATE alerts SET payload=? WHERE id=?', actions['legacy'])
                    if stopping():
                        raise sqlite3.OperationalError('interrupted')
                    self.save(result)
                    self.db.commit()
                else:
                    self.db.rollback()
        except (sqlite3.Error, ValueError) as exc:
            self.db.rollback()
            code = getattr(exc, 'sqlite_errorcode', 0) & 255
            reason = ('busy' if code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED) else
                      'budget' if code == sqlite3.SQLITE_INTERRUPT else
                      'stopping' if stopping() else 'storage_error')
            result.update(state='deferred' if reason in ('busy', 'budget', 'stopping') else 'error', reason=reason,
                          applied=False, counts={}, scanned=0, cursors={})
            if apply and not stopping():
                # Persist only diagnostics; never advance a cursor after rollback. BUSY/full may
                # also reject this write: propagate that error to the existing runtime safety path.
                try:
                    previous = load_report(self.db, self.kind)
                    result['cursors'] = previous.get('cursors', {})
                    with self.db:
                        self.save(result)
                except sqlite3.Error:
                    raise exc
        result['duration_ms'] = round((time.monotonic() - start) * 1000, 2)
        result['space'] = metrics(self.db)
        return result

    def save(self, result):
        table = 'meta' if self.kind == 'bridge' else 'monitor_state'
        self.db.execute(f"INSERT OR REPLACE INTO {table}(key,value) VALUES('maintenance',?)",
                        (json.dumps(result, sort_keys=True),))


def checkpoint(db):
    """Called by the existing writer only; PASSIVE never waits for a long reader."""
    busy, frames, copied = db.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
    return {'state': 'deferred' if busy or copied < frames else 'ok', 'frames': frames, 'copied': copied}
