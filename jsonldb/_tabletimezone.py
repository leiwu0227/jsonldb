"""Private table timezone interpretation and mutation preflight."""
import datetime as dt
import re
import os

from . import metaslot


_TIMESTAMP = re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}')
_SUFFIX = re.compile(r'(?:[.,][0-9]+)?([+-].*|[Zz].*)')
_OFFSET = re.compile(r'([+-])([0-9]{2})(?::?([0-9]{2}))?(?::?([0-9]{2})(?:[.,][0-9]+)?)?')


def inspect(path):
    try:
        return metaslot.inspect_file(path)
    except FileNotFoundError:
        return None


def read(path):
    info = inspect(path)
    return info.timezone if info is not None else None


def _offset_text(key):
    if isinstance(key, str) and _TIMESTAMP.match(key) is not None:
        match = _SUFFIX.fullmatch(key[19:])
        if match:
            return match.group(1)
    return None


def normalize(key, timezone, timespec, serialize):
    """Interpret declared-table keys without collapsing equal serialized keys."""
    stamp = key
    suffix = _offset_text(key)
    if suffix is not None:
        if suffix != 'Z':
            match = _OFFSET.fullmatch(suffix)
            if (match is None or int(match[2]) >= 24
                    or any(int(value) >= 60 for value in match.groups()[2:] if value is not None)):
                raise ValueError('invalid timezone-bearing timestamp key')
            if re.search(r'[.,][0-9]*[1-9][0-9]*$', suffix):
                raise ValueError('datetime key offset conflicts with table timezone ' + timezone)
        try:
            # Python 3.8 does not accept Z in fromisoformat.
            stamp = dt.datetime.fromisoformat(key[:-1] + '+00:00' if key.endswith('Z') else key)
        except ValueError:
            raise ValueError('invalid timezone-bearing timestamp key') from None
    if isinstance(stamp, dt.datetime):
        offset = stamp.utcoffset()
        if offset is not None:
            sign = -1 if timezone[0] == '-' else 1
            expected = dt.timedelta(minutes=sign * (int(timezone[1:3])*60 + int(timezone[4:])))
            if offset != expected:
                raise ValueError('datetime key offset conflicts with table timezone ' + timezone)
            stamp = stamp.replace(tzinfo=None)
        return serialize(stamp, timespec)
    return serialize(key, timespec)


def bound(path, value, timespec, serialize):
    """Naive read keys need no slot I/O; aware keys consult the declaration."""
    if ((isinstance(value, dt.datetime) and value.tzinfo is not None)
            or _offset_text(value)):
        timezone = read(path)
        if timezone is not None:
            return normalize(value, timezone, timespec, serialize)
    return value


def key(path, value, timespec, serialize):
    return serialize(bound(path, value, timespec, serialize), timespec)


def prepare(rows, timezone, timespec, serialize):
    prepared = []
    for key_value, record in rows.items():
        if not isinstance(record, dict):
            raise TypeError('JSONL record values must be dictionaries')
        text = normalize(key_value, timezone, timespec, serialize)
        if text == metaslot.META_KEY:
            raise ValueError("'_meta' is reserved for table metadata")
        prepared.append((text, record))
    return lambda: iter(prepared)


def save_slots(info, meta, slot_bytes):
    """Fit both publications; the initial slot already declares interpretation."""
    if meta is not None:
        metaslot._check_metadata_version(info)
    timezone = info.timezone if info is not None else None
    existing = info if info is not None and info.is_slot else None
    width = existing.width if slot_bytes is None and existing else slot_bytes
    if width is None:
        if meta is not None:
            raise ValueError('metadata slot is not enabled for this file')
        return None, None
    placeholder = metaslot._encode_slot(None, width, timezone)
    if meta is not None:
        final = metaslot._encode_slot(meta, width, timezone)
    elif existing is not None and slot_bytes is None:
        final = existing.raw_line
    else:
        final = metaslot._preserve_slot(existing, width)
    return placeholder, final


def metadata_bytes(path, meta):
    info = metaslot.inspect_file(path)
    if not info.is_slot:
        raise ValueError('metadata slot is not enabled for this file')
    metaslot._check_metadata_version(info)
    return metaslot._encode_slot(meta, info.width, info.timezone)


def configure_bytes(path, timezone):
    """Preflight without trusting an index or modifying any file."""
    if timezone is not None and not isinstance(timezone, str):
        raise TypeError('timezone must be a string or None')
    target = metaslot._normalize_timezone(timezone) if timezone is not None else None
    info = metaslot.inspect_file(path)
    if not info.is_slot or type(info.version) is not int or info.version != metaslot.CURRENT_VERSION:
        raise ValueError('timezone requires an existing known-version metadata slot')
    current = info.timezone
    if current == target:
        return None
    with open(path, 'rb') as source:
        source.readline()
        if any(line.strip() for line in source):
            raise ValueError('changing timezone on a populated table requires explicit migration')
    return metaslot._encode_slot(info.record, info.width, target)


def publish(path, encoded, load_index):
    """Publish preflighted slot bytes and mark the derived index fresh last."""
    load_index(path)
    with open(path, 'rb+') as stream:
        stream.write(encoded)
        stream.flush()
    os.utime(path + '.idx', None)


def validate(rows, info, timespec, legacy_validate, serialize):
    timezone = info.timezone if info is not None else None
    if timezone is None:
        return legacy_validate(rows, timespec, reuse=True)
    return prepare(rows, timezone, timespec, serialize)
