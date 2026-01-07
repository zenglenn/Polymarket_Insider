from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from dateutil import parser


def local_today(timezone: str) -> date:
    return datetime.now(ZoneInfo(timezone)).date()


def parse_datetime(value: str | int | float | None) -> datetime | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    try:
        return parser.isoparse(value)
    except (ValueError, TypeError):
        return None
