"""Bounded, read-only storage measurements and single-owner alert file rotation."""
import json
import os
import stat
import time
from dataclasses import dataclass, fields
from pathlib import Path

MAX_ARCHIVES = 32
DEFAULTS = {
    'STORAGE_WARN_BYTES': 256 * 1024**2,
    'STORAGE_CRITICAL_BYTES': 512 * 1024**2,
    'STORAGE_FREE_WARN_BYTES': 256 * 1024**2,
    'STORAGE_FREE_CRITICAL_BYTES': 128 * 1024**2,
    'STORAGE_CHECK_INTERVAL': 5,
    'ALERTS_MAX_BYTES': 10 * 1024**2,
    'ALERTS_BACKUP_COUNT': 4,
    'ALERTS_RETENTION_DAYS': 90,
}


@dataclass(frozen=True)
class Settings:
    warn_bytes: int = DEFAULTS['STORAGE_WARN_BYTES']
    critical_bytes: int = DEFAULTS['STORAGE_CRITICAL_BYTES']
    free_warn_bytes: int = DEFAULTS['STORAGE_FREE_WARN_BYTES']
    free_critical_bytes: int = DEFAULTS['STORAGE_FREE_CRITICAL_BYTES']
    check_interval: int = DEFAULTS['STORAGE_CHECK_INTERVAL']
    alerts_max_bytes: int = DEFAULTS['ALERTS_MAX_BYTES']
    alerts_backup_count: int = DEFAULTS['ALERTS_BACKUP_COUNT']
    alerts_retention_days: int = DEFAULTS['ALERTS_RETENTION_DAYS']

    @classmethod
    def load(cls, values):
        parsed = []
        for key, default in DEFAULTS.items():
            raw = str(values.get(key, default)).strip()
            if not raw.isascii() or not raw.isdecimal() or len(raw) > 18:
                raise ValueError(f'{key}: требуется целое число в допустимом диапазоне')
            number = int(raw)
            if number < (0 if key == 'ALERTS_BACKUP_COUNT' else 1):
                raise ValueError(f'{key}: значение вне допустимого диапазона')
            parsed.append(number)
        policy = cls(*parsed)
        if policy.warn_bytes >= policy.critical_bytes or policy.free_warn_bytes <= policy.free_critical_bytes:
            raise ValueError('STORAGE: пороги предупреждения и критического уровня противоречат друг другу')
        if policy.check_interval > 60 or policy.alerts_backup_count > MAX_ARCHIVES or policy.alerts_retention_days > 3650:
            raise ValueError('STORAGE/ALERTS: интервал или число архивов/дней вне допустимого диапазона')
        if policy.alerts_max_bytes < 1024:
            raise ValueError('ALERTS_MAX_BYTES: требуется не менее 1024 байт')
        return policy

    def as_dict(self):
        return {f.name: getattr(self, f.name) for f in fields(self)}


def directory(path):
    return os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)


def file_stat(fd, name):
    try:
        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
    except FileNotFoundError:
        return None  # WAL/SHM may disappear during a normal checkpoint/close.
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_dev != os.fstat(fd).st_dev:
        raise OSError('Unsupported managed file')
    return info


def snapshot(state, policy):
    """No SQLite connection, checkpoint, recursive scan, writes or private file names."""
    result = {'state': 'unknown', 'managed_bytes': None, 'free_bytes': None, 'files': {},
              'thresholds': policy.as_dict(), 'issues': [], 'warnings': []}
    fd = None
    try:
        fd = directory(state)
        names = [base + suffix for base in ('bridge.sqlite3', 'monitor.sqlite3') for suffix in ('', '-wal', '-shm')]
        names += ['alerts.jsonl'] + [f'alerts.jsonl.{i}' for i in range(1, MAX_ARCHIVES + 1)]
        for name in names:
            info = file_stat(fd, name)
            result['files'][name] = info.st_size if info else 0
        result['managed_bytes'] = sum(result['files'].values())
        disk = os.fstatvfs(fd)
        result['free_bytes'] = disk.f_bavail * disk.f_frsize
        result['state'] = 'ok'
        for key, code, level in (
            ('storage:size', 'storage_size', 'critical' if result['managed_bytes'] >= policy.critical_bytes else
             'warning' if result['managed_bytes'] >= policy.warn_bytes else None),
            ('storage:free', 'storage_free', 'critical' if result['free_bytes'] < policy.free_critical_bytes else
             'warning' if result['free_bytes'] < policy.free_warn_bytes else None),
        ):
            if level:
                # Live byte counters stay in metrics, not in an alert's identity.
                result['issues' if level == 'critical' else 'warnings'].append(
                    {'key': key, 'code': code, 'severity': level})
        if result['issues']:
            result['state'] = 'critical'
        elif result['warnings']:
            result['state'] = 'warning'
    except OSError:
        result['issues'] = [{'key': 'storage:measurement', 'code': 'storage_unavailable', 'severity': 'critical'}]
    finally:
        if fd is not None:
            os.close(fd)
    return result


class Gate:
    """Pause new polls on pressure/unknown measurement; two fresh samples clear a latch."""
    def __init__(self, state, policy):
        self.state, self.policy = state, policy
        self.checked_at, self.current = None, None
        self.blocked, self.clean = False, 0

    def allow_poll(self, now=None):
        now = time.monotonic() if now is None else now
        if self.checked_at is None or now - self.checked_at >= self.policy.check_interval:
            self.checked_at = now
            self.current = snapshot(self.state, self.policy)
            if self.current['state'] in ('unknown', 'critical'):
                self.blocked, self.clean = True, 0
            elif self.blocked:
                size_safe = self.current['managed_bytes'] < (self.policy.warn_bytes + self.policy.critical_bytes) // 2
                free_safe = self.current['free_bytes'] >= (self.policy.free_warn_bytes + self.policy.free_critical_bytes) // 2
                self.clean = self.clean + 1 if size_safe and free_safe else 0
                if self.clean >= 2:
                    self.blocked = False
        return not self.blocked


class AlertFile:
    """Only monitor owns the sink; renames are atomic, acknowledgement follows file+directory fsync."""
    def __init__(self, path, policy):
        self.path, self.policy = Path(path), policy

    def _old(self, fd, name, now):
        info = file_stat(fd, name)
        if info is None:
            return False
        handle = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        try:
            if os.fstat(handle).st_ino != info.st_ino:
                raise OSError('Managed file changed')
            # First record is the oldest retained batch; pruning a whole file may shorten history.
            first = os.read(handle, min(info.st_size, 65536)).split(b'\n', 1)[0]
            if not first:
                oldest = info.st_mtime
            else:
                oldest = json.loads(first)['detected_at']
                if isinstance(oldest, bool) or not isinstance(oldest, (int, float)) or not (0 <= oldest < float('inf')):
                    raise ValueError('Invalid alert time')
        except (ValueError, KeyError, TypeError, UnicodeError):
            raise OSError('Invalid alert archive') from None
        finally:
            os.close(handle)
        return now - oldest >= self.policy.alerts_retention_days * 86400

    def _maintain(self, fd, now):
        changed = False
        # Only this exact family of files is owned; no glob, symlink or backup traversal.
        for i in range(MAX_ARCHIVES, -1, -1):
            name = self.path.name + (f'.{i}' if i else '')
            info = file_stat(fd, name)
            if info is not None and (i > self.policy.alerts_backup_count or info.st_size > self.policy.alerts_max_bytes
                                     or self._old(fd, name, now)):
                os.unlink(name, dir_fd=fd)
                changed = True
        if changed:
            os.fsync(fd)

    def maintain(self, now=None):
        fd = directory(self.path.parent)
        try:
            self._repair_tail(fd)
            self._maintain(fd, time.time() if now is None else now)
        finally:
            os.close(fd)

    def _repair_tail(self, fd):
        info = file_stat(fd, self.path.name)
        if info is None or not info.st_size or info.st_size > self.policy.alerts_max_bytes:
            return
        handle = os.open(self.path.name, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        try:
            if os.fstat(handle).st_ino != info.st_ino:
                raise OSError('Managed file changed')
            if os.pread(handle, 1, info.st_size - 1) == b'\n':
                return
            # An incomplete last line was never durably acknowledged. Outbox will replay it.
            end, cut = info.st_size, 0
            while end:
                start = max(0, end - 65536)
                chunk = os.pread(handle, end - start, start)
                found = chunk.rfind(b'\n')
                if found >= 0:
                    cut = start + found + 1
                    break
                end = start
            os.ftruncate(handle, cut)
            os.fsync(handle)
        finally:
            os.close(handle)

    def __call__(self, alert):
        raw = (json.dumps(alert, ensure_ascii=False, sort_keys=True) + '\n').encode()
        if len(raw) > self.policy.alerts_max_bytes:
            raise OSError('Alert exceeds configured file limit')
        fd = directory(self.path.parent)
        try:
            self._repair_tail(fd)
            self._maintain(fd, time.time())
            info = file_stat(fd, self.path.name)
            if info and info.st_size + len(raw) > self.policy.alerts_max_bytes:
                for i in range(self.policy.alerts_backup_count, 0, -1):
                    target = f'{self.path.name}.{i}'
                    source = self.path.name + (f'.{i-1}' if i > 1 else '')
                    file_stat(fd, target)  # Reject links before overwrite as well as before reading.
                    if file_stat(fd, source) is not None:
                        os.replace(source, target, src_dir_fd=fd, dst_dir_fd=fd)
                if not self.policy.alerts_backup_count:
                    os.unlink(self.path.name, dir_fd=fd)
                os.fsync(fd)
            handle = os.open(self.path.name, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK,
                             0o600, dir_fd=fd)
            try:
                checked = os.fstat(handle)
                if not stat.S_ISREG(checked.st_mode) or checked.st_nlink != 1:
                    raise OSError('Unsupported alert file')
                with os.fdopen(handle, 'ab', closefd=False) as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(handle)
            finally:
                os.close(handle)
            os.fsync(fd)
        finally:
            os.close(fd)
