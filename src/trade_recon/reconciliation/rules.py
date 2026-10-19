from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class FieldRule:
    break_type: str
    internal_column: str
    external_column: str
    field_name: str


DEFAULT_PRICE_TOLERANCE = Decimal("0.01")
DEFAULT_MISSING_SIDE_SLA_MINUTES = 30

FIELD_RULES = (
    FieldRule(
        "INSTRUMENT_MISMATCH",
        "internal_instrument_id",
        "external_instrument_id",
        "instrument_id",
    ),
    FieldRule("SIDE_MISMATCH", "internal_side", "external_side", "side"),
    FieldRule(
        "QUANTITY_MISMATCH",
        "internal_quantity",
        "external_quantity",
        "quantity",
    ),
    FieldRule(
        "CURRENCY_MISMATCH",
        "internal_currency",
        "external_currency",
        "currency",
    ),
    FieldRule(
        "BROKER_MISMATCH",
        "internal_broker_id",
        "external_broker_id",
        "broker_id",
    ),
    FieldRule(
        "ACCOUNT_MISMATCH",
        "internal_account_id",
        "external_account_id",
        "account_id",
    ),
    FieldRule(
        "TRADE_DATE_MISMATCH",
        "internal_trade_date",
        "external_trade_date",
        "trade_date",
    ),
    FieldRule(
        "SETTLEMENT_DATE_MISMATCH",
        "internal_settlement_date",
        "external_settlement_date",
        "settlement_date",
    ),
)

REQUIRED_ACTIVE_FIELDS = (
    "instrument_id",
    "side",
    "quantity",
    "price",
    "currency",
    "broker_id",
    "account_id",
    "trade_date",
    "settlement_date",
)

RESOLUTION_REASON_BY_BREAK = {
    "MISSING_CONFIRMATION": "LATE_CONFIRMATION",
    "MISSING_TRADE": "LATE_TRADE",
}


def validate_runtime_config(price_tolerance: Decimal, sla_minutes: int) -> None:
    if price_tolerance < 0:
        raise ValueError("price_tolerance must be zero or greater")
    if sla_minutes <= 0:
        raise ValueError("sla_minutes must be greater than zero")
