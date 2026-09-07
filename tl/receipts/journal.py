"""Local OS locking and recoverable, append-only metric ledger batches."""

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import uuid

from tl.stream.events import ValidationError, canonical

LOCK_HEADER = b'tokenledger-os-lock/v1\n'


def sha(content):
    return hashlib.sha256(content).hexdigest()


@contextmanager
def ledger_lock(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.with_suffix(path.suffix + '.lock').exists():
        raise ValidationError('legacy lock exists; stop legacy writers and preserve the lock for explicit migration')
    lock = path.with_suffix(path.suffix + '.oslock')
    fd = os.open(lock, os.O_CREAT | os.O_RDWR | getattr(os,'O_BINARY',0), 0o600)
    acquired = False
    try:
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except OSError as exc:
            raise ValidationError('metric ledger is locked by an active writer') from exc
        header = os.read(fd, len(LOCK_HEADER))
        if not header:
            # Safe after a crash during creation: only this protocol uses .oslock.
            os.write(fd, LOCK_HEADER)
            os.fsync(fd)
        elif header != LOCK_HEADER:
            raise ValidationError('unrecognized OS lock header; preserve the lock for explicit migration')
        yield
    finally:
        if acquired:
            os.lseek(fd, 0, os.SEEK_SET)
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
        # Keep the same inode/path. Unlinking permits two distinct locks on Unix.


def pending_path(path):
    return Path(str(path) + '.pending.json')


def _finish(path, pending):
    """Recover only an exact prefix of an already verified append intent."""
    from tl.receipts.metrics import parse_receipts
    intent = json.loads(pending.read_bytes())
    if intent.get('schema_version') != 'tokenledger-append/v1':
        raise ValidationError('unsupported ledger recovery intent')
    payload = intent['payload'].encode('utf-8')
    current = path.read_bytes() if path.exists() else b''
    size = intent['base_size']
    if (type(size) is not int or size < 0 or len(current) < size
            or sha(current[:size]) != intent['base_sha256']
            or sha(payload) != intent['payload_sha256']
            or not payload.startswith(current[size:])):
        raise ValidationError('ledger recovery mismatch; preserve ledger and append intent')
    parse_receipts(current[:size] + payload)
    remaining = payload[len(current) - size:]
    with path.open('ab') as handle:
        handle.write(remaining)
        handle.flush()
        os.fsync(handle.fileno())
    audit = Path(str(path) + '.batches')
    audit.mkdir(exist_ok=True)
    completed = audit / (sha(pending.read_bytes()) + '.json')
    if completed.exists():
        raise ValidationError('append audit already exists; preserve pending intent')
    pending.rename(completed)
    return dict(status='recovered', appended_bytes=len(remaining), ledger_sha256=sha(path.read_bytes()),
                intent_sha256=completed.stem, evidence=str(completed))


def append_batch(path, records):
    from tl.receipts.metrics import parse_receipts
    path = Path(path)
    payload = (''.join(canonical(record) + '\n' for record in records)).encode('utf-8')
    if not payload:
        raise ValidationError('empty metric receipt batch')
    with ledger_lock(path):
        pending = pending_path(path)
        if pending.exists():
            raise ValidationError('ledger recovery required; run tl ledger recover with the same artifact root')
        previous = path.read_bytes() if path.exists() else b''
        parse_receipts(previous + payload)
        intent = dict(schema_version='tokenledger-append/v1', base_size=len(previous),
                      base_sha256=sha(previous), payload_sha256=sha(payload), payload=payload.decode('utf-8'))
        temporary = pending.with_name(pending.name + '.' + uuid.uuid4().hex + '.tmp')
        with temporary.open('xb') as handle:
            handle.write((canonical(intent) + '\n').encode('utf-8'))
            handle.flush()
            os.fsync(handle.fileno())
        temporary.rename(pending)
        _finish(path, pending)


def recover(path):
    path = Path(path)
    with ledger_lock(path):
        pending = pending_path(path)
        if not pending.exists():
            from tl.receipts.metrics import parse_receipts
            content = path.read_bytes() if path.exists() else b''
            parse_receipts(content)
            return dict(status='clean', ledger_sha256=sha(content))
        return _finish(path, pending)
