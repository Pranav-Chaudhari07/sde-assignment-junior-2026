"""Value normalisation for Assignment A.

Implements the spec in assignment-a-data/README.md exactly:
header matching, phone, date, decimal and enum rules.
Pure functions, no I/O, so they are easy to test.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

IST_OFFSET_HOURS = 5.5


def normalize_header(value: str) -> str:
    """Lower-case, trim, replace every run of '_' or whitespace with one space."""
    return re.sub(r"[_\s]+", " ", value.strip().lower())


_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def normalize_phone(raw: str | None) -> str | None:
    """Return canonical '+91XXXXXXXXXX' or None if invalid.

    Empty / missing input returns None too -- callers must distinguish
    missing (empty after trim) from invalid (present but bad) themselves.
    """
    if raw is None:
        return None
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) == 10 and digits[0] in "6789":
        return "+91" + digits
    return None


def parse_date(raw: str | None) -> str | None:
    """Parse the four accepted day-first formats, return ISO 'YYYY-MM-DD'.

    Returns None for missing input and for impossible / unrecognised dates.
    """
    if raw is None:
        return None
    s = raw.strip()
    if not s:
        return None
    # DD-Mon-YYYY, month name case-insensitive
    m = re.fullmatch(r"(\d{1,2})-([A-Za-z]{3})-(\d{4})", s)
    if m:
        day, mon, year = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        month = _MONTHS.get(mon)
        if month is None:
            return None
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


_DECIMAL_PREFIX = re.compile(r"^(?:₹|rs\.?|inr)\s*", re.IGNORECASE)


def parse_decimal(raw: str | None) -> Decimal | None:
    """Parse an optional currency-prefixed decimal, >= 0, rounded to 2 places.

    Returns None for missing input and for invalid / negative values.
    """
    if raw is None:
        return None
    s = raw.strip()
    if not s:
        return None
    s = _DECIMAL_PREFIX.sub("", s)
    s = s.replace(",", "").strip()
    if not s:
        return None
    try:
        value = Decimal(s)
    except InvalidOperation:
        return None
    if value < 0 or not value.is_finite():
        return None
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def parse_enum(raw: str | None, values: list[str]) -> str | None:
    """Case-insensitive enum match, stored lower-case; None if no match."""
    if raw is None:
        return None
    s = raw.strip()
    if not s:
        return None
    lowered = s.lower()
    if lowered in [v.lower() for v in values]:
        return lowered
    return None


def parse_timestamp(raw) -> datetime | None:
    """Parse the three timestamp forms. Naive means IST.

    Returns an aware datetime (in UTC), or None if unparsable / missing.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s[:-1] + "+00:00")
        else:
            dt = datetime.fromisoformat(s)
    except ValueError:
        # Naive 'YYYY-MM-DD HH:MM:SS' is also fromisoformat-parseable in
        # 3.11+, so reaching here means genuinely unparsable.
        return None
    if dt.tzinfo is None:
        from datetime import timezone, timedelta

        dt = dt.replace(tzinfo=timezone(timedelta(hours=5, minutes=30)))
    from datetime import timezone

    return dt.astimezone(timezone.utc)


def ist_iso(dt_utc: datetime | None) -> str | None:
    """Aware UTC datetime -> ISO-8601 string in IST (+05:30)."""
    if dt_utc is None:
        return None
    from datetime import timezone, timedelta

    ist = timezone(timedelta(hours=5, minutes=30))
    return dt_utc.astimezone(ist).isoformat(timespec="seconds")
