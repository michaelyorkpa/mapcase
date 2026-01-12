from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import re
from typing import Optional, Mapping


_MAX_AGE_RE = re.compile(r"max-age=(\d+)")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_http_datetime(value: str) -> Optional[datetime]:
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def compute_expires_at(headers: Mapping[str, str], default_ttl_seconds: int) -> datetime:
    """
    Prefer Cache-Control: max-age; fallback to Expires header; fallback to default TTL.
    """
    now = utcnow()

    cache_control = headers.get("cache-control") or headers.get("Cache-Control") or ""
    m = _MAX_AGE_RE.search(cache_control)
    if m:
        return now + timedelta(seconds=int(m.group(1)))

    expires = headers.get("expires") or headers.get("Expires")
    if expires:
        dt = parse_http_datetime(expires)
        if dt:
            return dt

    return now + timedelta(seconds=default_ttl_seconds)


def get_etag(headers: Mapping[str, str]) -> Optional[str]:
    return headers.get("etag") or headers.get("ETag")


def get_last_modified(headers: Mapping[str, str]) -> Optional[datetime]:
    lm = headers.get("last-modified") or headers.get("Last-Modified")
    return parse_http_datetime(lm) if lm else None
