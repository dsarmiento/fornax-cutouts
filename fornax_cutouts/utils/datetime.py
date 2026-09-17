from __future__ import annotations

from datetime import datetime

from vo_models.voresource.types import UTCTimestamp


def utc_timestamp(value: datetime | str) -> UTCTimestamp:
    if isinstance(value, UTCTimestamp):
        return value
    if isinstance(value, str):
        return UTCTimestamp.fromisoformat(value)
    return UTCTimestamp.fromisoformat(value.isoformat())


def format_utc_timestamp(value: datetime, *, timespec: str = "seconds") -> str:
    return utc_timestamp(value).isoformat(timespec=timespec)
