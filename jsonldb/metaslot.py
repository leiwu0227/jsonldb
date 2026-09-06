"""Fixed-width line-one metadata slots for JSONL table files."""

from typing import Any, NamedTuple, Optional
import re

import orjson


META_KEY = "_meta"
CURRENT_VERSION = 1
DEFAULT_SLOT_BYTES = 4096


class SlotInfo(NamedTuple):
    """Classification and decoded contents of one physical first line."""

    is_slot: bool
    width: int
    record: Optional[Any]
    version: Optional[int]
    raw_line: bytes

    @property
    def is_unknown_version(self) -> bool:
        return (self.is_slot and type(self.version) is int
                and self.version != CURRENT_VERSION)

    @property
    def timezone(self):
        """Return a canonical known offset; never guess damaged declarations."""
        if not self.is_slot or self.is_unknown_version:
            return None
        try:
            envelope = orjson.loads(self.raw_line)[META_KEY]
            if type(envelope) is dict and 'timezone' in envelope:
                if type(self.version) is not int or self.version != CURRENT_VERSION:
                    raise ValueError('invalid timezone envelope')
                return _normalize_timezone(envelope['timezone'])
        except (orjson.JSONDecodeError, KeyError, TypeError):
            if b'"timezone"' in self.raw_line:
                raise ValueError('damaged timezone declaration') from None
        return None


def _normalize_timezone(value):
    """Normalize fixed offsets, including the explicitly supported UTC alias."""
    if not isinstance(value, str):
        raise ValueError('timezone declaration must be a fixed-offset string')
    if value == 'UTC':
        return '+00:00'
    match = re.fullmatch(r'([+-])([0-9]{1,2}):([0-9]{2})', value)
    if match:
        sign, hours, minutes = match.groups()
        hours, minutes = int(hours), int(minutes)
        if hours < 24 and minutes < 60:
            return '%s%02d:%02d' % (sign if hours or minutes else '+', hours, minutes)
    raise ValueError('timezone must be UTC or a signed offset less than 24 hours')


def _looks_like_torn_slot(line: bytes) -> bool:
    """Recognize the stable reserved-key prefix when the envelope is torn."""
    stripped = line.lstrip()
    prefix = b'{"_meta"'
    if not stripped.startswith(prefix):
        return False
    return stripped[len(prefix):].lstrip().startswith(b':')


def classify_line(line: bytes) -> SlotInfo:
    """Classify a physical first line without consulting folder settings."""
    width = len(line)
    try:
        value = orjson.loads(line)
    except (orjson.JSONDecodeError, TypeError):
        is_slot = _looks_like_torn_slot(line)
        return SlotInfo(is_slot, width if is_slot else 0, None, None, line)

    if not isinstance(value, dict) or META_KEY not in value:
        return SlotInfo(False, 0, None, None, line)

    if len(value) != 1:
        return SlotInfo(True, width, None, None, line)

    envelope = value[META_KEY]
    if not isinstance(envelope, dict):
        return SlotInfo(True, width, None, None, line)
    version = envelope.get("v")
    if type(version) is not int or version != CURRENT_VERSION:
        return SlotInfo(True, width, None, version, line)
    return SlotInfo(True, width, envelope.get("data"), version, line)


def encode_slot(record: Optional[dict], width: int) -> bytes:
    """Encode a version-1 envelope padded to exactly ``width`` bytes."""
    return _encode_slot(record, width)


def _encode_slot(record, width, timezone=None):
    """Encode library properties independently of the opaque consumer record."""
    if type(width) is not int or width <= 0:
        raise ValueError("metadata slot width must be a positive integer")
    if record is not None and not isinstance(record, dict):
        raise TypeError("metadata record must be a dict or None")

    envelope = {"v": CURRENT_VERSION}
    if timezone is not None:
        envelope['timezone'] = _normalize_timezone(timezone)
    if record is not None:
        envelope["data"] = record
    serialized = orjson.dumps(
        {META_KEY: envelope}, option=orjson.OPT_SERIALIZE_NUMPY
    ) + b'\n'
    required = len(serialized)
    if required > width:
        raise ValueError(
            "metadata record requires %d bytes but slot is %d bytes"
            % (required, width)
        )
    return serialized[:-1] + b' ' * (width - required) + b'\n'


def inspect_file(file_path: str) -> SlotInfo:
    """Read and classify line one, returning a legacy result for an empty file."""
    with open(file_path, 'rb') as f:
        line = f.readline()
    if not line:
        return SlotInfo(False, 0, None, None, b'')
    return classify_line(line)


def _preserve_slot(info: Optional[SlotInfo], width: int) -> bytes:
    """Fit an existing envelope without interpreting an unknown version."""
    if type(width) is not int or width <= 0:
        raise ValueError("metadata slot width must be a positive integer")
    if info is not None and info.is_unknown_version:
        if width == info.width and info.raw_line.endswith(b'\n'):
            return info.raw_line
        content = info.raw_line.rstrip()
        required = len(content) + 1
        if required > width:
            raise ValueError(
                "metadata envelope requires %d bytes but slot is %d bytes"
                % (required, width))
        return content + b' ' * (width - required) + b'\n'
    record = info.record if info is not None and info.is_slot else None
    return _encode_slot(record, width, info.timezone if info is not None else None)


def lint_slot(info: Optional[SlotInfo], width: Optional[int] = None) -> Optional[bytes]:
    """Choose lint's slot bytes, preserving opaque unknown-version envelopes."""
    if info is not None:
        info.timezone  # Refuse recognizable damage before repair can discard it.
    if width is not None:
        return _preserve_slot(info, width)
    if info is not None and info.is_slot:
        if info.is_unknown_version or (type(info.version) is int and info.version == CURRENT_VERSION):
            return info.raw_line if info.raw_line.endswith(b'\n') else info.raw_line + b'\n'
        try:
            return encode_slot(None, info.width)
        except ValueError:
            pass
    return None


def read_slot(file_path: str) -> Optional[Any]:
    """Return a valid known-version record, or ``None`` for all other states."""
    return inspect_file(file_path).record


def _check_metadata_version(info: Optional[SlotInfo]) -> None:
    if info is not None and info.is_unknown_version:
        raise ValueError('cannot edit metadata with unsupported envelope version %s' % info.version)


def write_slot(file_path: str, record: Optional[dict]) -> None:
    """Fit-check and overwrite an existing slot without changing its width."""
    info = inspect_file(file_path)
    if not info.is_slot:
        raise ValueError("metadata slot is not enabled for this file")
    _check_metadata_version(info)
    encoded = _encode_slot(record, info.width, info.timezone)
    with open(file_path, 'rb+') as f:
        f.seek(0)
        f.write(encoded)
        f.flush()
