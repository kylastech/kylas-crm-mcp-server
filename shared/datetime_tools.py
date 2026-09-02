"""
Date/datetime conversion helpers shared across every entity: the default
timezone fallback, converting a *.search filter's date value(s) from the
user's local timezone to the UTC ISO string Kylas expects, computing an
"N days ago" threshold for idle-record searches, and the standalone
datetime.parse_to_utc registry id (create/update field_values need a UTC
ISO string; users think and speak in their own timezone).
"""

from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from dateutil import parser as dateutil_parser

from shared.app import mcp


def _get_default_timezone() -> str:
    """
    Default timezone for date/datetime filters. Used when the user doesn't pass timeZone in a filter.
    Fixed to Asia/Calcutta for now.
    """
    return "Asia/Calcutta"


DEFAULT_TIMEZONE = _get_default_timezone()


def _threshold_iso_days_ago(days: int, time_zone: str) -> str:
    """Return (now - days) in the given timezone as ISO string (UTC with Z)."""
    try:
        tz = ZoneInfo(time_zone)
    except Exception:
        tz = ZoneInfo("UTC")
    now = datetime.now(tz)
    threshold = now - timedelta(days=days)
    return threshold.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _convert_date_value_to_utc(value: Any, timezone_str: str) -> Any:
    """
    Convert date/datetime filter value(s) from user's local timezone to UTC.
    Handles single ISO string, list of ISO strings (for between operator), or None.
    Strips trailing 'Z' before parsing so the value is treated as local time.
    """
    if value is None:
        return value

    try:
        tz = ZoneInfo(timezone_str)
    except Exception:
        tz = ZoneInfo("UTC")
    utc = ZoneInfo("UTC")

    def _convert_single(v: str) -> str:
        if not isinstance(v, str):
            return v
        # Strip trailing Z so we treat it as naive local time
        clean = v.rstrip("Z").rstrip("z")
        try:
            # Try parsing with milliseconds first
            try:
                dt = datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S.%f")
            except ValueError:
                dt = datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S")
            # Attach the user's timezone, then convert to UTC
            local_dt = dt.replace(tzinfo=tz)
            utc_dt = local_dt.astimezone(utc)
            return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc_dt.microsecond // 1000:03d}Z"
        except Exception:
            return v  # Return as-is if parsing fails

    if isinstance(value, list):
        return [_convert_single(v) for v in value]
    return _convert_single(value)


# ---------------------------------------------------------------------------
# Tool 3a: Parse datetime in user timezone to UTC ISO (for create_lead datetime fields)
# ---------------------------------------------------------------------------

def parse_datetime_to_utc_iso(local_datetime: str, timezone: str) -> str:
    """
    Parse a datetime string as given in the user's local timezone and return UTC ISO string for the Kylas API.
    Use when creating a lead with a date/datetime field: the user says e.g. "11th Feb 2026 at 7:30 AM" in their timezone;
    call get_current_user to get timezone, then call this with (user's datetime string, user's timezone) and put the result in field_values.
    """
    try:
        tz = ZoneInfo(timezone)
    except Exception:
        tz = ZoneInfo("UTC")
    dt = dateutil_parser.parse(local_datetime)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    utc_dt = dt.astimezone(ZoneInfo("UTC"))
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


@mcp.tool()
def parse_datetime_to_utc_iso_tool(local_datetime: str, timezone: str) -> str:
    """
    Parse a datetime string in the user's timezone and return UTC ISO string for the Kylas API.
    Call get_current_user first to get the user's timezone. Use the returned string in create_lead field_values for date/datetime fields.
    Example: user says "create lead with follow-up 11th Feb 2026 at 7:30 AM" → get_current_user → timezone Asia/Calcutta → parse_datetime_to_utc_iso_tool("11 Feb 2026 7:30 AM", "Asia/Calcutta") → use result in field_values.
    local_datetime: Datetime as the user said it (e.g. "11 Feb 2026 7:30 AM", "11th Feb 2026 at 7:30 am").
    timezone: IANA timezone from get_current_user (e.g. Asia/Calcutta).
    """
    try:
        return parse_datetime_to_utc_iso(local_datetime, timezone)
    except Exception as e:
        return f"Error: {e}"
