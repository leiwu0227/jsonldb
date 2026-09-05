"""Scoped logger capture and bounded persistent integrity reports."""

import logging
import os
import re
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone


MAX_FINDINGS = 100
MAX_REMOVED_CHARS = 160
_INVALID = re.compile(r"invalid JSON line in (.+?) at byte (\d+): (.*)")
_REMOVED = re.compile(r"lint removed (\d+) bytes from (.+) at byte (\d+)")
_INDEX = re.compile(r"rebuilt (empty|corrupt|missing|stale) index (.+)")
_MOVE = re.compile(r"could not move (?:invalid file|valid file|file) (.+?): (.*)")
_MISSING = re.compile(r"file not found: (.+)")


def _one_line(value):
    return " ".join(str(value).replace("|", "\\|").splitlines())


def _finding(record):
    message = _one_line(record.getMessage())
    if getattr(record, "jsonldb_kind", None) == "invalid_json":
        return "kind=invalid_json | file=%s | offset=%s | detail=skipped" % (
            _one_line(record.jsonldb_file), record.jsonldb_offset)
    removed = getattr(record, "jsonldb_removed", None)
    match = _REMOVED.fullmatch(message)
    if match:
        count, path, offset = match.groups()
        detail = "removed %s bytes" % count
        suffix = " | removed=%s" % _one_line(
            removed.decode("utf-8", errors="replace")[:MAX_REMOVED_CHARS]
        ) if removed is not None else ""
        return ("kind=lint_removed | file=%s | offset=%s | detail=%s%s" %
                (_one_line(path), offset, detail, suffix))
    match = _INVALID.fullmatch(message)
    if match:
        path, offset, _ = match.groups()
        return "kind=invalid_json | file=%s | offset=%s | detail=skipped" % (
            _one_line(path), offset)
    match = _INDEX.fullmatch(message)
    if match:
        reason, path = match.groups()
        return ("kind=index_rebuilt | file=%s | offset=- | detail=%s" %
                (_one_line(path), reason))
    path = getattr(record, "jsonldb_file", "-")
    kind = getattr(record, "jsonldb_kind", record.levelname.lower())
    return "kind=%s | file=%s | offset=- | detail=%s" % (
        _one_line(kind), _one_line(path), message)


def _record_path(record):
    """Prefer structured ownership; decode only known legacy message formats."""
    path = getattr(record, "jsonldb_file", None)
    if path is not None:
        return path
    message = record.getMessage()
    for pattern, group in ((_INVALID, 1), (_REMOVED, 2), (_INDEX, 2),
                           (_MOVE, 1), (_MISSING, 1)):
        match = pattern.fullmatch(message)
        if match:
            return match[group]
    return None


class _Capture(logging.Handler):
    def __init__(self, folder):
        super().__init__(logging.WARNING)
        self._folder = os.path.normcase(os.path.abspath(folder))
        self.findings = []
        self.total = 0

    def emit(self, record):
        path = _record_path(record)
        if path is None:
            return
        try:
            absolute = os.path.normcase(os.path.abspath(path))
            if os.path.commonpath((self._folder, absolute)) != self._folder:
                return
        except (TypeError, ValueError, OSError):
            return  # An unusable diagnostic path must not break recovery.
        self.total += 1
        if len(self.findings) < MAX_FINDINGS:
            self.findings.append(_finding(record))


def _write(folder, operation, filename, capture):
    report_dir = os.path.join(folder, ".jsonldb")
    os.makedirs(report_dir, exist_ok=True)
    lines = ["timestamp=%s | operation=%s" % (
        datetime.now(timezone.utc).isoformat(), operation)]
    lines.extend(capture.findings)
    omitted = capture.total - len(capture.findings)
    if omitted:
        lines.append("omitted=%d" % omitted)
    target = os.path.join(report_dir, filename)
    fd, temporary = tempfile.mkstemp(dir=report_dir, prefix="." + filename)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write("\n".join(lines) + "\n")
        os.replace(temporary, target)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


@contextmanager
def capture_report(folder, operation, filename):
    """Capture scoped package warnings and replace one operation report."""
    capture = _Capture(folder)
    package_logger = logging.getLogger("jsonldb")
    package_logger.addHandler(capture)
    try:
        yield
    finally:
        package_logger.removeHandler(capture)
        _write(folder, operation, filename, capture)
