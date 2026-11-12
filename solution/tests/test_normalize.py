"""Unit tests for the value rules most likely to break."""
from datetime import timezone
from decimal import Decimal

from solution.pipeline.normalize import (
    ist_iso,
    normalize_header,
    normalize_phone,
    parse_date,
    parse_decimal,
    parse_enum,
    parse_timestamp,
)


def test_header_normalisation():
    assert normalize_header("  Policy No ") == "policy no"
    assert normalize_header("policy_number") == "policy number"
    assert normalize_header("Mobile__No") == "mobile no"
    assert normalize_header("CUSTOMER  NAME") == "customer name"


def test_phone_formats():
    cases = {
        "9876543210": "+919876543210",
        "+91 98765 43210": "+919876543210",
        "91-98765-43210": "+919876543210",
        "09876543210": "+919876543210",
        "(+91) 9876543210": "+919876543210",
        "+919876543210": "+919876543210",
        "6789012345": "+916789012345",
    }
    for raw, expected in cases.items():
        assert normalize_phone(raw) == expected, raw


def test_phone_invalid():
    for raw in ["987654321", "5876543210", "022-1234-5678", "N/A",
                "+1 415 555 0134", "98765432101", ""]:
        assert normalize_phone(raw) is None, raw


def test_dates():
    assert parse_date("2026-12-01") == "2026-12-01"
    assert parse_date("01/12/2026") == "2026-12-01"  # day-first
    assert parse_date("01-12-2026") == "2026-12-01"
    assert parse_date("01-Dec-2026") == "2026-12-01"
    assert parse_date("01-dec-2026") == "2026-12-01"
    assert parse_date("01-DEC-2026") == "2026-12-01"


def test_dates_impossible():
    for raw in ["31/02/2026", "2026-13-01", "soon", "TBD", "32/01/2026"]:
        assert parse_date(raw) is None, raw


def test_decimals():
    assert parse_decimal("₹27,554") == Decimal("27554.00")
    assert parse_decimal("Rs. 1000") == Decimal("1000.00")
    assert parse_decimal("INR 25301.4") == Decimal("25301.40")
    assert parse_decimal("6850") == Decimal("6850.00")
    assert parse_decimal("12.345") == Decimal("12.35")  # rounds to 2dp
    assert parse_decimal("rs 500") == Decimal("500.00")


def test_decimals_invalid():
    for raw in ["N/A", "-", "twelve thousand", "-500"]:
        assert parse_decimal(raw) is None, raw


def test_enum():
    assert parse_enum("HI", ["en", "hi", "ta"]) == "hi"
    assert parse_enum(" en ", ["en", "hi"]) == "en"
    assert parse_enum("Hindi", ["en", "hi"]) is None
    assert parse_enum("english", ["en", "hi"]) is None


def test_timestamps_all_forms_same_instant():
    utc = parse_timestamp("2026-10-02T21:08:23Z")
    offset = parse_timestamp("2026-10-03T02:38:23+05:30")
    naive = parse_timestamp("2026-10-03 02:38:23")
    assert utc == offset == naive
    assert ist_iso(utc) == "2026-10-03T02:38:23+05:30"
    assert utc.tzinfo == timezone.utc
