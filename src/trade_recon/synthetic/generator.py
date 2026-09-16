"""Deterministic synthetic trade-data generator for TR-016.

The generator creates source-faithful OMS and Broker A payloads, delivery batches,
expected reconciliation outcomes, and a portable generation manifest. It models
source event time independently from delivery timing so late/out-of-order cases
can be exercised without corrupting source semantics.
"""

from __future__ import annotations

import copy
import csv
import json
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
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

SCENARIO_NAMES: dict[str, str] = {
    "S-001": "EXACT_MATCH",
    "S-002": "PRICE_MISMATCH",
    "S-003": "PRICE_WITHIN_TOLERANCE",
    "S-004": "QUANTITY_MISMATCH",
    "S-005": "MULTI_FIELD_MISMATCH",
    "S-006": "MISSING_CONFIRMATION",
    "S-007": "LATE_CONFIRMATION",
    "S-008": "MISSING_TRADE",
    "S-009": "LATE_TRADE",
    "S-010": "MATCH_KEY_MISSING",
    "S-011": "OMS_AMENDMENT",
    "S-012": "BROKER_CORRECTION",
    "S-013": "OUT_OF_ORDER_OMS",
    "S-014": "OUT_OF_ORDER_BROKER",
    "S-015": "DUPLICATE_OMS_EVENT",
    "S-016": "DUPLICATE_BROKER_EVENT",
    "S-017": "CONFLICTING_OMS_EVENT",
    "S-018": "CONFLICTING_BROKER_EVENT",
    "S-019": "DUPLICATE_INTERNAL_CANDIDATE",
    "S-020": "DUPLICATE_EXTERNAL_CANDIDATE",
    "S-021": "OMS_CANCEL_BROKER_ACTIVE",
    "S-022": "BROKER_CANCEL_OMS_ACTIVE",
    "S-023": "BOTH_CANCELLED",
    "S-024": "LATE_CANCEL",
    "S-025": "REQUIRED_VALUE_MISSING",
    "S-026": "INVALID_SCHEMA",
    "S-027": "INVALID_DATA_TYPE",
    "S-028": "INVALID_ENUM",
    "S-029": "BROKER_MISMATCH",
    "S-030": "ACCOUNT_MISMATCH",
    "S-031": "OMS_VERSION_AFTER_MISSING_BREAK",
}

CURRENT_BREAK_TYPES: dict[str, list[str]] = {
    "S-002": ["PRICE_MISMATCH"],
    "S-004": ["QUANTITY_MISMATCH"],
    "S-005": ["PRICE_MISMATCH", "QUANTITY_MISMATCH"],
    "S-006": ["MISSING_CONFIRMATION"],
    "S-008": ["MISSING_TRADE"],
    "S-010": ["MATCH_KEY_MISSING"],
    "S-019": ["DUPLICATE_INTERNAL_CANDIDATE"],
    "S-020": ["DUPLICATE_EXTERNAL_CANDIDATE"],
    "S-021": ["OMS_CANCEL_BROKER_ACTIVE"],
    "S-022": ["BROKER_CANCEL_OMS_ACTIVE"],
    "S-024": ["OMS_CANCEL_BROKER_ACTIVE"],
    "S-029": ["BROKER_MISMATCH"],
    "S-030": ["ACCOUNT_MISMATCH"],
}

BREAK_HISTORY: dict[str, list[str]] = {
    "S-007": ["MISSING_CONFIRMATION"],
    "S-009": ["MISSING_TRADE"],
    "S-012": ["PRICE_MISMATCH"],
    "S-031": ["MISSING_CONFIRMATION"],
}

VALIDATION_ERRORS: dict[str, list[str]] = {
    "S-017": ["CONFLICTING_EVENT_ID"],
    "S-018": ["CONFLICTING_EVENT_ID"],
    "S-025": ["REQUIRED_VALUE_MISSING"],
    "S-026": ["INVALID_SCHEMA"],
    "S-027": ["INVALID_DATA_TYPE"],
    "S-028": ["INVALID_ENUM"],
}

# Backward-compatible aliases retained for earlier milestones/tests.
ScenarioNameMap = SCENARIO_NAMES
BreakTypeMap = CURRENT_BREAK_TYPES
ExpectedBreakHistoryMap = BREAK_HISTORY

MATCHED_SCENARIOS = {
    "S-001",
    "S-003",
    "S-007",
    "S-009",
    "S-011",
    "S-012",
    "S-013",
    "S-014",
    "S-015",
    "S-016",
    "S-017",
    "S-018",
    "S-023",
    "S-031",
}
QUARANTINED_SCENARIOS = {"S-025", "S-026", "S-027", "S-028"}

PRICE_QUANTUM = Decimal("0.0000000001")
QUANTITY_QUANTUM = Decimal("0.000001")
DEFAULT_PRICE_TOLERANCE = Decimal("0.01")
DEFAULT_PRICE_DELTA = Decimal("0.02")
DEFAULT_QUANTITY_DELTA = Decimal("10.000000")
DEFAULT_SLA_MINUTES = 30
DEFAULT_LATE_BY_MINUTES = 5


def generate_dataset(config: GeneratorConfig) -> GenerationManifest:
    """Generate a deterministic dataset, write staged source files, and return its manifest."""
    seed = int(config.get("seed", 12345))
    business_date = _parse_business_date(config.get("business_date"))
    scenario_ids = list(config.get("scenario_ids", SCENARIO_NAMES.keys()))
    if not scenario_ids:
        raise ValueError("scenario_ids must not be empty")
    _validate_scenario_ids(scenario_ids)

    trade_count = int(config.get("trade_count", len(scenario_ids)))
    records_per_file = int(config.get("records_per_file", 1000))
    if trade_count <= 0:
        raise ValueError("trade_count must be positive")
    if records_per_file <= 0:
        raise ValueError("records_per_file must be positive")

    output_dir = Path(config.get("output_path", "data/synthetic/generated"))
    output_dir.mkdir(parents=True, exist_ok=True)
    generator_version = str(config.get("generator_version", "1.0.0"))
    overrides = config.get("scenario_overrides", {})
    if not isinstance(overrides, Mapping):
        raise ValueError("scenario_overrides must be a mapping")

    rng = Random(seed)
    expected_results: list[dict[str, Any]] = []
    batch_groups: dict[int, dict[str, list[Mapping[str, Any]]]] = defaultdict(
        lambda: {"oms_events": [], "broker_events": []}
    )
    scenario_counter: Counter[str] = Counter()
    total_oms = 0
    total_broker = 0

    for trade_index in range(1, trade_count + 1):
        scenario_id = scenario_ids[(trade_index - 1) % len(scenario_ids)]
        scenario = default_scenario_config(scenario_id)
        scenario_override = overrides.get(scenario_id, {})
        if not isinstance(scenario_override, Mapping):
            raise ValueError(f"scenario override for {scenario_id} must be a mapping")
        scenario.update(scenario_override)

        trade = generate_base_trade(trade_index, business_date, rng)
        oms_events, broker_events, expected_result = generate_scenario(trade, scenario, rng)
        batches = assign_delivery_batches(oms_events, broker_events, scenario)

        oracle = dict(expected_result)
        oracle["generated_oms_event_count"] = len(oms_events)
        oracle["generated_broker_event_count"] = len(broker_events)
        oracle["delivery_offsets_minutes"] = [
            int(batch["delivery_offset_minutes"]) for batch in batches
        ]
        expected_results.append(oracle)

        total_oms += len(oms_events)
        total_broker += len(broker_events)
        scenario_counter[scenario_id] += 1

        for batch in batches:
            offset = int(batch["delivery_offset_minutes"])
            batch_groups[offset]["oms_events"].extend(batch["oms_events"])
            batch_groups[offset]["broker_events"].extend(batch["broker_events"])

    source_files: list[dict[str, Any]] = []
    for batch_index, offset in enumerate(sorted(batch_groups), start=1):
        phase_dir = output_dir / f"batch_{batch_index:03d}_offset_{offset:05d}m"
        oms_dir = phase_dir / "oms"
        broker_dir = phase_dir / "broker_a"
        oms_paths = write_oms_jsonl(batch_groups[offset]["oms_events"], oms_dir, records_per_file)
        broker_paths = write_broker_csv(
            batch_groups[offset]["broker_events"], broker_dir, records_per_file
        )
        source_files.append(
            {
                "batch_id": f"BATCH_{batch_index:03d}",
                "delivery_offset_minutes": offset,
                "oms_files": [_relative_posix(path, output_dir) for path in oms_paths],
                "broker_files": [_relative_posix(path, output_dir) for path in broker_paths],
                "oms_event_count": len(batch_groups[offset]["oms_events"]),
                "broker_event_count": len(batch_groups[offset]["broker_events"]),
            }
        )

    manifest: dict[str, Any] = {
        "generator_version": generator_version,
        "seed": seed,
        "business_date": business_date.isoformat(),
        "trade_count": trade_count,
        "scenario_ids": scenario_ids,
        "scenario_counts": dict(sorted(scenario_counter.items())),
        "oms_event_count": total_oms,
        "broker_event_count": total_broker,
        "source_event_count": total_oms + total_broker,
        "expected_results": expected_results,
        "source_files": source_files,
    }
    validate_generated_dataset(manifest)
    write_manifest(manifest, output_dir)
    return manifest


def default_scenario_config(scenario_id: str) -> dict[str, Any]:
    """Return deterministic default parameters for a scenario."""
    if scenario_id not in SCENARIO_NAMES:
        raise ValueError(f"Unsupported scenario_id: {scenario_id}")
    config: dict[str, Any] = {
        "scenario_id": scenario_id,
        "scenario_name": SCENARIO_NAMES[scenario_id],
    }
    if scenario_id in {"S-002", "S-003", "S-005", "S-012", "S-014"}:
        config.update(
            price_tolerance=str(DEFAULT_PRICE_TOLERANCE),
            price_delta=("0.01" if scenario_id == "S-003" else str(DEFAULT_PRICE_DELTA)),
        )
    if scenario_id in {"S-004", "S-005"}:
        config["quantity_delta"] = str(DEFAULT_QUANTITY_DELTA)
    if scenario_id in {"S-007", "S-009", "S-031"}:
        config.update(
            missing_counterparty_sla_minutes=DEFAULT_SLA_MINUTES,
            late_by_minutes=DEFAULT_LATE_BY_MINUTES,
        )
    if scenario_id == "S-024":
        config.update(cancel_sla_minutes=DEFAULT_SLA_MINUTES, late_by_minutes=DEFAULT_LATE_BY_MINUTES)
    if scenario_id in {"S-011", "S-013", "S-031"}:
        config["amend_price_delta"] = str(DEFAULT_PRICE_DELTA)
    if scenario_id in {"S-012", "S-014"}:
        config["correction_price_delta"] = str(DEFAULT_PRICE_DELTA)
    return config


def generate_base_trade(trade_index: int, business_date: date, rng: Random) -> SyntheticTrade:
    """Generate source-neutral trade economics before OMS/Broker wrapping."""
    if trade_index < 1:
        raise ValueError("trade_index must be greater than zero")
    instruments = ("AAPL", "MSFT", "GOOG", "AMZN")
    venues = ("XNAS", "XNYS")
    quantity = rng.randrange(1, 501) * 10
    price_cents = rng.randrange(1_000, 50_001)
    price = (Decimal(price_cents) / Decimal("100")).quantize(PRICE_QUANTUM)
    execution_time = datetime.combine(
        business_date, time(hour=14, minute=30), tzinfo=timezone.utc
    ) + timedelta(seconds=rng.randrange(0, 6 * 60 * 60))
    return {
        "business_trade_id": f"TRD{trade_index:08d}",
        "instrument_id": rng.choice(instruments),
        "instrument_type": "EQUITY",
        "side": rng.choice(("BUY", "SELL")),
        "quantity": f"{Decimal(quantity):.6f}",
        "price": format(price, "f"),
        "currency": "USD",
        "account_id": f"ACC{((trade_index - 1) % 1000) + 1:06d}",
        "portfolio_id": f"PORT{((trade_index - 1) % 100) + 1:04d}",
        "broker_id": "BROKER_A",
        "venue_id": rng.choice(venues),
        "trade_date": business_date.isoformat(),
        "execution_timestamp": execution_time.isoformat(),
        "settlement_date": (business_date + timedelta(days=2)).isoformat(),
    }


def generate_scenario(
    trade: SyntheticTrade,
    scenario: ScenarioConfig,
    rng: Random,
) -> tuple[list[OmsEvent], list[BrokerEvent], ExpectedResult]:
    """Apply one scenario and return source events plus the external test oracle."""
    scenario_id = str(scenario.get("scenario_id", ""))
    if not scenario_id:
        raise ValueError("scenario_id is required in scenario config")
    _validate_scenario_ids([scenario_id])

    original = copy.deepcopy(trade)
    oms_events: list[OmsEvent]
    broker_events: list[BrokerEvent]
    expected_oms_version: int | None = 1
    expected_broker_version: int | None = 1

    if scenario_id == "S-001":
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_events = generate_broker_events(trade, scenario, rng)
    elif scenario_id == "S-002":
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_events = generate_broker_events(apply_price_mismatch(trade, scenario), scenario, rng)
    elif scenario_id == "S-003":
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_events = generate_broker_events(apply_price_within_tolerance(trade, scenario), scenario, rng)
    elif scenario_id == "S-004":
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_events = generate_broker_events(apply_quantity_mismatch(trade, scenario), scenario, rng)
    elif scenario_id == "S-005":
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_events = generate_broker_events(apply_multi_field_mismatch(trade, scenario), scenario, rng)
    elif scenario_id == "S-006":
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_events = []
        expected_broker_version = None
    elif scenario_id == "S-007":
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_events = generate_broker_events(trade, scenario, rng)
    elif scenario_id == "S-008":
        oms_events = []
        broker_events = generate_broker_events(trade, scenario, rng)
        expected_oms_version = None
    elif scenario_id == "S-009":
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_events = generate_broker_events(trade, scenario, rng)
    elif scenario_id == "S-010":
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_events = generate_broker_events(trade, scenario, rng)
        broker_events[0]["client_trade_id"] = None
    elif scenario_id in {"S-011", "S-013"}:
        oms_events = apply_oms_amendment(trade, scenario)
        amended_trade = _trade_from_oms_event(oms_events[-1], trade)
        broker_events = generate_broker_events(amended_trade, scenario, rng)
        expected_oms_version = 2
    elif scenario_id in {"S-012", "S-014"}:
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_events = apply_broker_correction(trade, scenario)
        expected_broker_version = 2
    elif scenario_id == "S-015":
        oms_events = generate_oms_events(trade, scenario, rng)
        oms_events.append(duplicate_event(oms_events[0]))
        broker_events = generate_broker_events(trade, scenario, rng)
    elif scenario_id == "S-016":
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_events = generate_broker_events(trade, scenario, rng)
        broker_events.append(duplicate_event(broker_events[0]))
    elif scenario_id == "S-017":
        oms_events = generate_oms_events(trade, scenario, rng)
        conflicting_price = str((Decimal(trade["price"]) + Decimal("1.00")).quantize(PRICE_QUANTUM))
        oms_events.append(create_conflicting_redelivery(oms_events[0], "price", conflicting_price))
        broker_events = generate_broker_events(trade, scenario, rng)
    elif scenario_id == "S-018":
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_events = generate_broker_events(trade, scenario, rng)
        conflicting_price = str((Decimal(trade["price"]) + Decimal("1.00")).quantize(PRICE_QUANTUM))
        broker_events.append(
            create_conflicting_redelivery(broker_events[0], "price", conflicting_price)
        )
    elif scenario_id == "S-019":
        oms_events = generate_oms_events(trade, scenario, rng)
        candidate = copy.deepcopy(oms_events[0])
        candidate["event_id"] = f"OMS-EVT-{trade['business_trade_id']}-CANDIDATE-002"
        oms_events.append(candidate)
        broker_events = generate_broker_events(trade, scenario, rng)
    elif scenario_id == "S-020":
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_events = generate_broker_events(trade, scenario, rng)
        candidate = copy.deepcopy(broker_events[0])
        candidate["confirmation_event_id"] = f"BRK-EVT-{trade['business_trade_id']}-CANDIDATE-002"
        candidate["broker_trade_id"] = f"BRK2-{trade['business_trade_id']}"
        broker_events.append(candidate)
    elif scenario_id in {"S-021", "S-022", "S-023", "S-024"}:
        cancel_side = {
            "S-021": "OMS",
            "S-022": "BROKER",
            "S-023": "BOTH",
            "S-024": "OMS",
        }[scenario_id]
        cancel_scenario = dict(scenario)
        cancel_scenario["cancel_side"] = cancel_side
        oms_events, broker_events = apply_cancellation(trade, cancel_scenario)
        expected_oms_version = 2 if cancel_side in {"OMS", "BOTH"} else 1
        expected_broker_version = 2 if cancel_side in {"BROKER", "BOTH"} else 1
    elif scenario_id in QUARANTINED_SCENARIOS:
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_events = []
        expected_broker_version = None
        if scenario_id == "S-025":
            oms_events[0]["instrument_id"] = None
        elif scenario_id == "S-026":
            oms_events[0]["schema_version"] = "999.0"
        elif scenario_id == "S-027":
            oms_events[0]["quantity"] = "NOT_A_NUMBER"
        elif scenario_id == "S-028":
            oms_events[0]["side"] = "HOLD"
    elif scenario_id == "S-029":
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_trade = copy.deepcopy(trade)
        broker_trade["broker_id"] = "BROKER_B"
        broker_events = generate_broker_events(broker_trade, scenario, rng)
    elif scenario_id == "S-030":
        oms_events = generate_oms_events(trade, scenario, rng)
        broker_trade = copy.deepcopy(trade)
        broker_trade["account_id"] = "ACC999999"
        broker_events = generate_broker_events(broker_trade, scenario, rng)
    elif scenario_id == "S-031":
        oms_events = apply_oms_amendment(trade, scenario)
        amended_trade = _trade_from_oms_event(oms_events[-1], trade)
        broker_events = generate_broker_events(amended_trade, scenario, rng)
        broker_events[0]["confirmation_version"] = 2
        broker_events[0]["confirmation_type"] = "CORRECT"
        broker_events[0]["confirmation_event_id"] = f"BRK-EVT-{trade['business_trade_id']}-V002"
        expected_oms_version = 2
        expected_broker_version = 2
    else:
        raise AssertionError(f"Unhandled scenario {scenario_id}")

    if trade != original:
        raise AssertionError(f"scenario {scenario_id} mutated source-neutral base trade")

    status = (
        "QUARANTINED"
        if scenario_id in QUARANTINED_SCENARIOS
        else "MATCHED"
        if scenario_id in MATCHED_SCENARIOS
        else "BREAK"
    )
    return (
        oms_events,
        broker_events,
        get_expected_result(
            scenario_id=scenario_id,
            trade_id=str(trade["business_trade_id"]),
            scenario_name=SCENARIO_NAMES[scenario_id],
            status=status,
            expected_oms_version=expected_oms_version,
            expected_broker_version=expected_broker_version,
        ),
    )


def get_expected_result(
    scenario_id: str,
    trade_id: str,
    scenario_name: str,
    status: str,
    expected_oms_version: int | None = 1,
    expected_broker_version: int | None = 1,
) -> dict[str, Any]:
    """Return the expected final state plus historical/validation expectations."""
    break_history = list(BREAK_HISTORY.get(scenario_id, []))
    break_occurrences = dict(Counter(break_history))
    return {
        "scenario_id": scenario_id,
        "scenario_name": scenario_name,
        "business_trade_id": trade_id,
        "expected_reconciliation_status": status,
        "expected_break_types": list(CURRENT_BREAK_TYPES.get(scenario_id, [])),
        "expected_break_history": break_history,
        "expected_break_occurrences": break_occurrences,
        "expected_validation_errors": list(VALIDATION_ERRORS.get(scenario_id, [])),
        "expected_oms_version": expected_oms_version,
        "expected_broker_version": expected_broker_version,
    }


def generate_oms_events(
    trade: SyntheticTrade, scenario: ScenarioConfig, rng: Random
) -> list[OmsEvent]:
    """Generate one OMS NEW event from a source-neutral trade."""
    return [_build_oms_event(trade, 1, "NEW", rng)]


def generate_broker_events(
    trade: SyntheticTrade, scenario: ScenarioConfig, rng: Random
) -> list[BrokerEvent]:
    """Generate one Broker A CONFIRM event from a source-neutral trade."""
    return [_build_broker_event(trade, 1, "CONFIRM", rng)]


def assign_delivery_batches(
    oms_events: Sequence[OmsEvent],
    broker_events: Sequence[BrokerEvent],
    scenario: ScenarioConfig,
) -> list[DeliveryBatch]:
    """Assign events to deterministic delivery phases independently of source version order."""
    scenario_id = str(scenario.get("scenario_id", ""))
    _validate_scenario_ids([scenario_id])

    if scenario_id in {"S-007", "S-009", "S-031"}:
        sla = _positive_int(scenario.get("missing_counterparty_sla_minutes"), "missing_counterparty_sla_minutes")
        late_by = _positive_int(scenario.get("late_by_minutes"), "late_by_minutes")
    else:
        sla = DEFAULT_SLA_MINUTES
        late_by = DEFAULT_LATE_BY_MINUTES

    if scenario_id == "S-007":
        return [
            _batch(1, 0, oms_events, []),
            _batch(2, sla + late_by, [], broker_events),
        ]
    if scenario_id == "S-009":
        return [
            _batch(1, 0, [], broker_events),
            _batch(2, sla + late_by, oms_events, []),
        ]
    if scenario_id == "S-013":
        if len(oms_events) < 2:
            raise ValueError("S-013 requires OMS v1 and v2")
        return [
            _batch(1, 0, [oms_events[-1]], broker_events),
            _batch(2, 1, [oms_events[0]], []),
        ]
    if scenario_id == "S-014":
        if len(broker_events) < 2:
            raise ValueError("S-014 requires Broker v1 and v2")
        return [
            _batch(1, 0, oms_events, [broker_events[-1]]),
            _batch(2, 1, [], [broker_events[0]]),
        ]
    if scenario_id == "S-024":
        cancel_sla = _positive_int(scenario.get("cancel_sla_minutes", DEFAULT_SLA_MINUTES), "cancel_sla_minutes")
        cancel_late = _positive_int(scenario.get("late_by_minutes", DEFAULT_LATE_BY_MINUTES), "late_by_minutes")
        return [
            _batch(1, 0, oms_events[:1], broker_events),
            _batch(2, cancel_sla + cancel_late, oms_events[1:], []),
        ]
    if scenario_id == "S-031":
        if len(oms_events) < 2 or len(broker_events) != 1:
            raise ValueError("S-031 requires OMS v1/v2 and one late Broker v2")
        return [
            _batch(1, 0, [oms_events[0]], []),
            _batch(2, sla + late_by, [oms_events[1]], []),
            _batch(3, sla + late_by + 1, [], broker_events),
        ]
    return [_batch(1, 0, oms_events, broker_events)]


def write_oms_jsonl(
    events: Sequence[OmsEvent], output_path: str | Path, records_per_file: int
) -> list[Path]:
    """Write immutable OMS JSONL chunks and return file paths."""
    return _write_chunks(events, output_path, records_per_file, "oms_part", "jsonl")


def write_broker_csv(
    events: Sequence[BrokerEvent], output_path: str | Path, records_per_file: int
) -> list[Path]:
    """Write immutable Broker A CSV chunks and return file paths."""
    if records_per_file <= 0:
        raise ValueError("records_per_file must be positive")
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    if not events:
        return []
    headers = [
        "confirmation_event_id",
        "broker_trade_id",
        "client_trade_id",
        "confirmation_version",
        "confirmation_type",
        "confirmation_time",
        "schema_version",
        "instrument_code",
        "instrument_type",
        "side",
        "quantity",
        "price",
        "currency",
        "client_account",
        "broker_id",
        "venue",
        "trade_date",
        "execution_timestamp",
        "settlement_date",
        "published_at",
    ]
    paths: list[Path] = []
    for index, chunk in enumerate(_chunks(events, records_per_file), start=1):
        path = output_dir / f"broker_a_part_{index:05d}.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            writer.writerows(chunk)
        paths.append(path)
    return paths


def write_manifest(manifest: GenerationManifest, output_path: str | Path) -> Path:
    """Persist deterministic generation metadata and oracle information."""
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "generation_manifest.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return path


def validate_generated_dataset(manifest: GenerationManifest) -> None:
    """Validate manifest counts and scenario invariants; raise on internal inconsistency."""
    required = {
        "generator_version",
        "seed",
        "business_date",
        "trade_count",
        "scenario_ids",
        "scenario_counts",
        "oms_event_count",
        "broker_event_count",
        "source_event_count",
        "expected_results",
        "source_files",
    }
    missing = required - set(manifest)
    if missing:
        raise ValueError(f"manifest missing required keys: {sorted(missing)}")

    trade_count = int(manifest["trade_count"])
    expected_results = manifest["expected_results"]
    if len(expected_results) != trade_count:
        raise ValueError("expected_results count must equal trade_count")

    oms_count = int(manifest["oms_event_count"])
    broker_count = int(manifest["broker_event_count"])
    if int(manifest["source_event_count"]) != oms_count + broker_count:
        raise ValueError("source_event_count does not equal OMS + Broker counts")

    file_oms = sum(int(item["oms_event_count"]) for item in manifest["source_files"])
    file_broker = sum(int(item["broker_event_count"]) for item in manifest["source_files"])
    if file_oms != oms_count or file_broker != broker_count:
        raise ValueError("source_files event counts do not match manifest totals")

    seen_ids: set[str] = set()
    scenario_counts: Counter[str] = Counter()
    for result in expected_results:
        scenario_id = result["scenario_id"]
        _validate_scenario_ids([scenario_id])
        business_trade_id = result["business_trade_id"]
        if business_trade_id in seen_ids:
            raise ValueError(f"duplicate business_trade_id in oracle: {business_trade_id}")
        seen_ids.add(business_trade_id)
        scenario_counts[scenario_id] += 1

        if result["expected_reconciliation_status"] == "MATCHED" and result["expected_break_types"]:
            raise ValueError("MATCHED result cannot have current break types")
        if scenario_id == "S-006" and result["generated_broker_event_count"] != 0:
            raise ValueError("S-006 must not generate Broker events")
        if scenario_id == "S-008" and result["generated_oms_event_count"] != 0:
            raise ValueError("S-008 must not generate OMS events")
        if scenario_id == "S-031" and result["expected_break_occurrences"].get("MISSING_CONFIRMATION") != 1:
            raise ValueError("S-031 must contain exactly one MISSING_CONFIRMATION episode")

    if dict(sorted(scenario_counts.items())) != dict(manifest["scenario_counts"]):
        raise ValueError("scenario_counts do not match expected_results")


def apply_price_mismatch(trade: SyntheticTrade, scenario: ScenarioConfig) -> SyntheticTrade:
    """Return a copy whose price difference strictly exceeds tolerance."""
    tolerance = _decimal_config(scenario, "price_tolerance", DEFAULT_PRICE_TOLERANCE)
    delta = _decimal_config(scenario, "price_delta", DEFAULT_PRICE_DELTA)
    if tolerance < 0 or delta <= 0:
        raise ValueError("price_tolerance must be non-negative and price_delta must be positive")
    if delta <= tolerance:
        raise ValueError("price_delta must exceed price_tolerance")
    result = copy.deepcopy(trade)
    result["price"] = str((Decimal(str(trade["price"])) + delta).quantize(PRICE_QUANTUM))
    return result


def apply_price_within_tolerance(trade: SyntheticTrade, scenario: ScenarioConfig) -> SyntheticTrade:
    """Return a copy whose positive price difference is within tolerance."""
    tolerance = _decimal_config(scenario, "price_tolerance", DEFAULT_PRICE_TOLERANCE)
    delta = _decimal_config(scenario, "price_delta", DEFAULT_PRICE_TOLERANCE)
    if tolerance <= 0 or delta <= 0:
        raise ValueError("price_tolerance and price_delta must be positive")
    if delta > tolerance:
        raise ValueError("price_delta must be within price_tolerance")
    result = copy.deepcopy(trade)
    result["price"] = str((Decimal(str(trade["price"])) + delta).quantize(PRICE_QUANTUM))
    return result


def apply_quantity_mismatch(trade: SyntheticTrade, scenario: ScenarioConfig) -> SyntheticTrade:
    """Return a copy with an observable six-decimal quantity mismatch."""
    delta = _decimal_config(scenario, "quantity_delta", DEFAULT_QUANTITY_DELTA)
    if delta < QUANTITY_QUANTUM:
        raise ValueError("quantity_delta must be at least 0.000001")
    result = copy.deepcopy(trade)
    quantity = Decimal(str(trade["quantity"])) + delta
    if quantity <= 0:
        raise ValueError("resulting quantity must be positive")
    result["quantity"] = f"{quantity:.6f}"
    return result


def apply_multi_field_mismatch(trade: SyntheticTrade, scenario: ScenarioConfig) -> SyntheticTrade:
    """Return a copy with both price and quantity mismatches."""
    return apply_quantity_mismatch(apply_price_mismatch(trade, scenario), scenario)


def apply_oms_amendment(trade: SyntheticTrade, scenario: ScenarioConfig) -> list[OmsEvent]:
    """Create OMS v1 NEW and v2 AMEND using source-local versioning."""
    rng = Random(int(scenario.get("seed", 11011)))
    v1 = _build_oms_event(trade, 1, "NEW", rng)
    amended_trade = copy.deepcopy(trade)
    delta = _decimal_config(scenario, "amend_price_delta", DEFAULT_PRICE_DELTA)
    if delta <= 0:
        raise ValueError("amend_price_delta must be positive")
    amended_trade["price"] = str((Decimal(str(trade["price"])) + delta).quantize(PRICE_QUANTUM))
    v2 = _build_oms_event(amended_trade, 2, "AMEND", rng)
    _ensure_later_timestamp(v2, "event_time", v1["event_time"])
    _ensure_later_timestamp(v2, "published_at", v1["published_at"])
    return [v1, v2]


def apply_broker_correction(trade: SyntheticTrade, scenario: ScenarioConfig) -> list[BrokerEvent]:
    """Create mismatching Broker v1 CONFIRM followed by matching v2 CORRECT."""
    rng = Random(int(scenario.get("seed", 12012)))
    mismatch_scenario = dict(scenario)
    mismatch_scenario.setdefault("price_tolerance", str(DEFAULT_PRICE_TOLERANCE))
    mismatch_scenario["price_delta"] = str(
        _decimal_config(scenario, "correction_price_delta", DEFAULT_PRICE_DELTA)
    )
    v1_trade = apply_price_mismatch(trade, mismatch_scenario)
    v1 = _build_broker_event(v1_trade, 1, "CONFIRM", rng)
    v2 = _build_broker_event(trade, 2, "CORRECT", rng)
    _ensure_later_timestamp(v2, "confirmation_time", v1["confirmation_time"])
    _ensure_later_timestamp(v2, "published_at", v1["published_at"])
    return [v1, v2]


def apply_cancellation(
    trade: SyntheticTrade, scenario: ScenarioConfig
) -> tuple[list[OmsEvent], list[BrokerEvent]]:
    """Create source-local cancellation lifecycles for OMS, Broker, or both."""
    cancel_side = str(scenario.get("cancel_side", "BOTH")).upper()
    if cancel_side not in {"OMS", "BROKER", "BOTH"}:
        raise ValueError("cancel_side must be OMS, BROKER, or BOTH")
    rng = Random(int(scenario.get("seed", 21021)))
    oms = [_build_oms_event(trade, 1, "NEW", rng)]
    broker = [_build_broker_event(trade, 1, "CONFIRM", rng)]
    if cancel_side in {"OMS", "BOTH"}:
        cancel = _build_oms_event(trade, 2, "CANCEL", rng)
        _ensure_later_timestamp(cancel, "event_time", oms[0]["event_time"])
        _ensure_later_timestamp(cancel, "published_at", oms[0]["published_at"])
        oms.append(cancel)
    if cancel_side in {"BROKER", "BOTH"}:
        cancel = _build_broker_event(trade, 2, "CANCEL", rng)
        _ensure_later_timestamp(cancel, "confirmation_time", broker[0]["confirmation_time"])
        _ensure_later_timestamp(cancel, "published_at", broker[0]["published_at"])
        broker.append(cancel)
    return oms, broker


def duplicate_event(event: Mapping[str, Any]) -> dict[str, Any]:
    """Create an exact redelivery with the same event ID and payload."""
    return copy.deepcopy(dict(event))


def create_conflicting_redelivery(
    event: Mapping[str, Any], field_name: str, conflicting_value: Any
) -> dict[str, Any]:
    """Reuse the source event ID while changing one payload field."""
    if field_name not in event:
        raise ValueError(f"field_name not present in event: {field_name}")
    result = copy.deepcopy(dict(event))
    if result[field_name] == conflicting_value:
        raise ValueError("conflicting_value must differ from the original value")
    result[field_name] = conflicting_value
    return result


def _build_oms_event(
    trade: SyntheticTrade, version: int, event_type: str, rng: Random
) -> dict[str, Any]:
    event_time = datetime.fromisoformat(str(trade["execution_timestamp"])) + timedelta(
        seconds=rng.randrange(1, 3600)
    )
    published_at = event_time + timedelta(seconds=rng.randrange(1, 600))
    return {
        "event_id": f"OMS-EVT-{trade['business_trade_id']}-V{version:03d}",
        "trade_id": trade["business_trade_id"],
        "trade_version": version,
        "event_type": event_type,
        "event_time": event_time.isoformat(),
        "schema_version": "1.0",
        "instrument_id": trade.get("instrument_id"),
        "instrument_type": trade.get("instrument_type"),
        "side": trade["side"],
        "quantity": trade["quantity"],
        "price": trade["price"],
        "currency": trade["currency"],
        "account_id": trade.get("account_id"),
        "portfolio_id": trade.get("portfolio_id"),
        "broker_id": trade["broker_id"],
        "venue_id": trade.get("venue_id"),
        "trade_date": trade["trade_date"],
        "execution_timestamp": trade["execution_timestamp"],
        "settlement_date": trade["settlement_date"],
        "published_at": published_at.isoformat(),
    }


def _build_broker_event(
    trade: SyntheticTrade, version: int, confirmation_type: str, rng: Random
) -> dict[str, Any]:
    confirmation_time = datetime.fromisoformat(str(trade["execution_timestamp"])) + timedelta(
        seconds=rng.randrange(1, 3600)
    )
    published_at = confirmation_time + timedelta(seconds=rng.randrange(1, 600))
    return {
        "confirmation_event_id": f"BRK-EVT-{trade['business_trade_id']}-V{version:03d}",
        "broker_trade_id": f"BRK-{trade['business_trade_id']}",
        "client_trade_id": trade["business_trade_id"],
        "confirmation_version": version,
        "confirmation_type": confirmation_type,
        "confirmation_time": confirmation_time.isoformat(),
        "schema_version": "1.0",
        "instrument_code": trade.get("instrument_id"),
        "instrument_type": trade.get("instrument_type"),
        "side": trade["side"],
        "quantity": trade["quantity"],
        "price": trade["price"],
        "currency": trade["currency"],
        "client_account": trade.get("account_id"),
        "broker_id": trade["broker_id"],
        "venue": trade.get("venue_id"),
        "trade_date": trade["trade_date"],
        "execution_timestamp": trade["execution_timestamp"],
        "settlement_date": trade["settlement_date"],
        "published_at": published_at.isoformat(),
    }


def _trade_from_oms_event(event: Mapping[str, Any], template: SyntheticTrade) -> dict[str, Any]:
    result = copy.deepcopy(dict(template))
    result.update(
        business_trade_id=event["trade_id"],
        instrument_id=event["instrument_id"],
        instrument_type=event["instrument_type"],
        side=event["side"],
        quantity=event["quantity"],
        price=event["price"],
        currency=event["currency"],
        account_id=event["account_id"],
        portfolio_id=event["portfolio_id"],
        broker_id=event["broker_id"],
        venue_id=event["venue_id"],
        trade_date=event["trade_date"],
        execution_timestamp=event["execution_timestamp"],
        settlement_date=event["settlement_date"],
    )
    return result


def _batch(
    phase: int,
    offset_minutes: int,
    oms_events: Sequence[OmsEvent],
    broker_events: Sequence[BrokerEvent],
) -> dict[str, Any]:
    return {
        "batch_id": f"BATCH_{phase:03d}",
        "delivery_phase": phase,
        "delivery_offset_minutes": int(offset_minutes),
        "oms_events": list(oms_events),
        "broker_events": list(broker_events),
    }


def _chunks(items: Sequence[Mapping[str, Any]], size: int) -> list[list[Mapping[str, Any]]]:
    return [list(items[index : index + size]) for index in range(0, len(items), size)]


def _write_chunks(
    events: Sequence[OmsEvent],
    output_path: str | Path,
    records_per_file: int,
    prefix: str,
    extension: str,
) -> list[Path]:
    if records_per_file <= 0:
        raise ValueError("records_per_file must be positive")
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for index, chunk in enumerate(_chunks(events, records_per_file), start=1):
        path = output_dir / f"{prefix}_{index:05d}.{extension}"
        with path.open("w", encoding="utf-8") as handle:
            for event in chunk:
                handle.write(json.dumps(event, sort_keys=True))
                handle.write("\n")
        paths.append(path)
    return paths


def _decimal_config(
    scenario: ScenarioConfig, key: str, default: Decimal
) -> Decimal:
    try:
        return Decimal(str(scenario.get(key, default)))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError(f"{key} must be decimal-compatible") from exc


def _positive_int(value: Any, name: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if result <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return result


def _parse_business_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise ValueError("business_date is required and must be YYYY-MM-DD or date")


def _validate_scenario_ids(scenario_ids: Sequence[str]) -> None:
    unsupported = [scenario_id for scenario_id in scenario_ids if scenario_id not in SCENARIO_NAMES]
    if unsupported:
        raise ValueError(f"Unsupported scenario_id(s): {unsupported}")


def _ensure_later_timestamp(event: dict[str, Any], field: str, reference: str) -> None:
    value = datetime.fromisoformat(str(event[field]))
    reference_dt = datetime.fromisoformat(reference)
    if value <= reference_dt:
        event[field] = (reference_dt + timedelta(seconds=1)).isoformat()


def _relative_posix(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()
