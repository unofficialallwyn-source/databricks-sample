"""Synthetic trade-data generator scaffolding for TR-016.

This module intentionally contains method signatures only.
Implement the generator logic hands-on and replace each NotImplementedError.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from random import Random
from typing import Any, Mapping, Sequence

GeneratorConfig = Mapping[str, Any]
ScenarioConfig = Mapping[str, Any]
SyntheticTrade = Mapping[str, Any]
OmsEvent = Mapping[str, Any]
BrokerEvent = Mapping[str, Any]
DeliveryBatch = Mapping[str, Any]
GenerationManifest = Mapping[str, Any]
ExpectedResult = Mapping[str, Any]


def generate_dataset(config: GeneratorConfig) -> GenerationManifest:
    """Generate a complete deterministic synthetic dataset and return its manifest."""
    raise NotImplementedError("TR-016: implement generate_dataset")


def generate_base_trade(
    trade_index: int,
    business_date: date,
    rng: Random,
) -> SyntheticTrade:
    """Generate one valid base trade before scenario-specific mutations."""
    if trade_index < 1:
        raise ValueError("trade_index must be greater than zero")

    instruments = ("AAPL", "MSFT", "GOOG", "AMZN")
    venues = ("XNAS", "XNYS")

    instrument_id = rng.choice(instruments)
    side = rng.choice(("BUY", "SELL"))
    quantity = rng.randrange(1, 501) * 10
    price_cents = rng.randrange(1_000, 50_001)
    price = (Decimal(price_cents) / Decimal("100")).quantize(Decimal("0.0000000001"))

    execution_time = datetime.combine(
        business_date,
        time(hour=14, minute=30),
        tzinfo=timezone.utc,
    ) + timedelta(seconds=rng.randrange(0, 6 * 60 * 60))

    event_time = execution_time + timedelta(seconds=rng.randrange(0, 5))
    published_at = event_time + timedelta(seconds=rng.randrange(1, 31))

    synthetic_trade: dict[str, Any] = {
        "event_id": f"OMS-EVT-{trade_index:08d}-V001",
        "trade_id": f"TRD{trade_index:08d}",
        "trade_version": 1,
        "event_type": "NEW",
        "event_time": event_time.isoformat(),
        "schema_version": "1.0",
        "instrument_id": instrument_id,
        "instrument_type": "EQUITY",
        "side": side,
        "quantity": f"{quantity:.6f}",
        "price": format(price, "f"),
        "currency": "USD",
        "account_id": f"ACC{((trade_index - 1) % 1000) + 1:06d}",
        "portfolio_id": f"PORT{((trade_index - 1) % 100) + 1:04d}",
        "broker_id": "BROKER_A",
        "venue_id": rng.choice(venues),
        "trade_date": business_date.isoformat(),
        "execution_timestamp": execution_time.isoformat(),
        "settlement_date": (business_date + timedelta(days=2)).isoformat(),
        "published_at": published_at.isoformat(),
    }
    return synthetic_trade


def generate_scenario(
    trade: SyntheticTrade,
    scenario: ScenarioConfig,
    rng: Random,
) -> tuple[list[OmsEvent], list[BrokerEvent], ExpectedResult]:
    """Apply one synthetic scenario and return source events plus the external test oracle."""
    raise NotImplementedError("TR-016: implement generate_scenario")


def generate_oms_events(
    trade: SyntheticTrade,
    scenario: ScenarioConfig,
    rng: Random,
) -> list[OmsEvent]:
    """Generate OMS JSONL-compatible source events for one synthetic trade."""
    raise NotImplementedError("TR-016: implement generate_oms_events")


def generate_broker_events(
    trade: SyntheticTrade,
    scenario: ScenarioConfig,
    rng: Random,
) -> list[BrokerEvent]:
    """Generate Broker A CSV-compatible source events for one synthetic trade."""
    raise NotImplementedError("TR-016: implement generate_broker_events")


def assign_delivery_batches(
    oms_events: Sequence[OmsEvent],
    broker_events: Sequence[BrokerEvent],
    scenario: ScenarioConfig,
) -> list[DeliveryBatch]:
    """Assign generated source events to delivery phases/files independently of source version order."""
    raise NotImplementedError("TR-016: implement assign_delivery_batches")


def write_oms_jsonl(
    events: Sequence[OmsEvent],
    output_path: str | Path,
    records_per_file: int,
) -> list[Path]:
    """Write immutable OMS JSONL files and return generated file paths."""
    raise NotImplementedError("TR-016: implement write_oms_jsonl")


def write_broker_csv(
    events: Sequence[BrokerEvent],
    output_path: str | Path,
    records_per_file: int,
) -> list[Path]:
    """Write immutable Broker A CSV files and return generated file paths."""
    raise NotImplementedError("TR-016: implement write_broker_csv")


def write_manifest(
    manifest: GenerationManifest,
    output_path: str | Path,
) -> Path:
    """Persist generation metadata and test-oracle information separately from source payloads."""
    raise NotImplementedError("TR-016: implement write_manifest")


def validate_generated_dataset(
    manifest: GenerationManifest,
) -> None:
    """Validate generator invariants and raise when generated data is internally inconsistent."""
    raise NotImplementedError("TR-016: implement validate_generated_dataset")


def apply_price_mismatch(
    trade: SyntheticTrade,
    scenario: ScenarioConfig,
) -> SyntheticTrade:
    """Return a trade/scenario variant whose price difference exceeds configured tolerance."""
    raise NotImplementedError("TR-016: implement apply_price_mismatch")


def apply_quantity_mismatch(
    trade: SyntheticTrade,
    scenario: ScenarioConfig,
) -> SyntheticTrade:
    """Return a trade/scenario variant with a quantity mismatch."""
    raise NotImplementedError("TR-016: implement apply_quantity_mismatch")


def apply_oms_amendment(
    trade: SyntheticTrade,
    scenario: ScenarioConfig,
) -> list[OmsEvent]:
    """Create source-local OMS amendment versions for the trade."""
    raise NotImplementedError("TR-016: implement apply_oms_amendment")


def apply_broker_correction(
    trade: SyntheticTrade,
    scenario: ScenarioConfig,
) -> list[BrokerEvent]:
    """Create source-local Broker correction versions for the trade."""
    raise NotImplementedError("TR-016: implement apply_broker_correction")


def apply_cancellation(
    trade: SyntheticTrade,
    scenario: ScenarioConfig,
) -> tuple[list[OmsEvent], list[BrokerEvent]]:
    """Create configured OMS/Broker cancellation lifecycle events."""
    raise NotImplementedError("TR-016: implement apply_cancellation")


def duplicate_event(event: Mapping[str, Any]) -> dict[str, Any]:
    """Create an exact redelivery with the same event ID and payload."""
    raise NotImplementedError("TR-016: implement duplicate_event")


def create_conflicting_redelivery(
    event: Mapping[str, Any],
    field_name: str,
    conflicting_value: Any,
) -> dict[str, Any]:
    """Reuse an event ID while changing payload content to test CONFLICTING_EVENT_ID handling."""
    raise NotImplementedError("TR-016: implement create_conflicting_redelivery")
