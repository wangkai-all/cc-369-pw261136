"""Typed scalar values with explicit parse failures.

Every parse returns either a typed value or a `ParseFailure` carrying the
positional error path and the raw text. Nothing is coerced to a string as a
fallback: an unparseable value stays unparseable and visible.
"""

from __future__ import annotations

import datetime as _dt
import decimal
import re
from dataclasses import dataclass
from typing import Optional, Union


class TypedValue:
    kind: str

    def as_json(self) -> object:
        raise NotImplementedError

    def comparable(self) -> object:
        """The value used when comparing across rows."""
        raise NotImplementedError


@dataclass(frozen=True)
class DateValue(TypedValue):
    value: _dt.date
    kind = "date"

    def as_json(self) -> str:
        return self.value.isoformat()

    def comparable(self) -> _dt.date:
        return self.value


@dataclass(frozen=True)
class MoneyValue(TypedValue):
    value: decimal.Decimal
    kind = "money"

    def as_json(self) -> str:
        return f"{self.value:.2f}"

    def comparable(self) -> decimal.Decimal:
        return self.value


@dataclass(frozen=True)
class EnumValue(TypedValue):
    value: str
    kind = "enum"

    def as_json(self) -> str:
        return self.value

    def comparable(self) -> str:
        return self.value


Value = Union[DateValue, MoneyValue, EnumValue]


@dataclass(frozen=True)
class ParseFailure:
    """A value that could not be typed, with a precise location."""

    batch_id: str
    row_index: int
    field: str
    raw: str
    reason: str
    rule_version: int

    @property
    def location(self) -> str:
        return f"batch={self.batch_id} row={self.row_index} field={self.field}"

    def as_json(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "row_index": self.row_index,
            "field": self.field,
            "raw": self.raw,
            "reason": self.reason,
            "rule_version": self.rule_version,
            "location": self.location,
        }


Parsed = Union[Value, ParseFailure]


def _fail(ctx: "ParseContext", raw: str, reason: str) -> ParseFailure:
    return ParseFailure(
        batch_id=ctx.batch_id,
        row_index=ctx.row_index,
        field=ctx.field,
        raw=raw,
        reason=reason,
        rule_version=ctx.rule_version,
    )


@dataclass(frozen=True)
class ParseContext:
    batch_id: str
    row_index: int
    field: str
    rule_version: int


DATE_FORMATS = ("%Y-%m-%d", "%Y/%m/%d", "%d.%m.%Y", "%m/%d/%Y", "%Y%m%d")


def parse_date(raw: str, ctx: ParseContext) -> Parsed:
    text = raw.strip()
    if not text:
        return _fail(ctx, raw, "empty value is not a date")
    for fmt in DATE_FORMATS:
        try:
            return DateValue(_dt.datetime.strptime(text, fmt).date())
        except ValueError:
            continue
    return _fail(ctx, raw, f"unrecognized date format (tried {len(DATE_FORMATS)} layouts)")


_MONEY_RE = re.compile(r"^(?P<sign>-)?(?P<cur>[$€£])?(?P<body>\d{1,3}(?:,\d{3})*|\d+)(?:\.(?P<cents>\d{1,2}))?$")


def parse_money(raw: str, ctx: ParseContext) -> Parsed:
    text = raw.strip()
    if not text:
        return _fail(ctx, raw, "empty value is not an amount")
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]
    m = _MONEY_RE.match(text)
    if not m:
        return _fail(ctx, raw, "unrecognized money format")
    digits = m.group("body").replace(",", "")
    cents = (m.group("cents") or "").ljust(2, "0")
    amount = decimal.Decimal(f"{digits}.{cents}")
    if m.group("sign"):
        amount = -amount
    return MoneyValue(amount)


def parse_enum(raw: str, ctx: ParseContext, allowed: tuple[str, ...], aliases: dict[str, str]) -> Parsed:
    text = raw.strip()
    if not text:
        return _fail(ctx, raw, "empty value is not a member of the enum")
    key = text.casefold()
    if key in aliases:
        return EnumValue(aliases[key])
    if key in allowed:
        return EnumValue(key)
    return _fail(ctx, raw, f"value not in controlled set {sorted(allowed)}")


def render(parsed: Parsed) -> Optional[str]:
    """Render a typed value for the shadow column; failures render as NULL.

    This is the *only* place a failure becomes NULL, and the failure itself is
    already recorded in the quarantine/inspection tables with its location.
    """
    if isinstance(parsed, ParseFailure):
        return None
    return parsed.as_json()
