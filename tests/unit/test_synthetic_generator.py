from __future__ import annotations

import csv
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from random import Random

import pytest

from src.trade_recon.synthetic.generator import (
    BreakTypeMap,
    ExpectedBreakHistoryMap,
    SCENARIO_NAMES,
    ScenarioNameMap,
    apply_broker_correction,
    apply_cancellation,
    apply_oms_amendment,
    apply_price_mismatch,
    apply_price_within_tolerance,
    apply_quantity_mismatch,
    assign_delivery_batches,
    create_conflicting_redelivery,
    default_scenario_config,
    duplicate_event,
    generate_base_trade,
    generate_dataset,
    generate_scenario,
    validate_generated_dataset,
    write_broker_csv,
    write_manifest,
    write_oms_jsonl,
)

BUSINESS_DATE = date(2026, 9, 7)


def _base_trade(index: int = 1):
    return generate_base_trade(index, BUSINESS_DATE, Random(12345))


def _scenario(scenario_id: str, **overrides):
    scenario = default_scenario_config(scenario_id)
    scenario.update(overrides)
    return scenario


def _generate(scenario_id: str, **overrides):
    trade = _base_trade()
    original = dict(trade)
    result = generate_scenario(trade, _scenario(scenario_id, **overrides), Random(12345))
    assert trade == original
    return trade, *result


def test_all_31_scenarios_are_registered() -> None:
    assert list(SCENARIO_NAMES) == [f"S-{i:03d}" for i in range(1, 32)]


def test_backward_compatible_scenario_aliases_are_available() -> None:
    assert ScenarioNameMap is SCENARIO_NAMES
    assert BreakTypeMap["S-006"] == ["MISSING_CONFIRMATION"]
    assert ExpectedBreakHistoryMap["S-007"] == ["MISSING_CONFIRMATION"]


def test_generate_base_trade_is_deterministic_and_source_neutral() -> None:
    first = generate_base_trade(7, BUSINESS_DATE, Random(12345))
    second = generate_base_trade(7, BUSINESS_DATE, Random(12345))
    assert first == second
    assert first["business_trade_id"] == "TRD00000007"
    assert first["trade_date"] == "2026-09-07"
    assert "event_id" not in first
    assert "confirmation_event_id" not in first


def test_exact_match_and_tolerance_scenarios_match() -> None:
    for scenario_id in ("S-001", "S-003"):
        _, oms, broker, expected = _generate(scenario_id)
        assert expected["expected_reconciliation_status"] == "MATCHED"
        assert expected["expected_break_types"] == []
        assert oms[0]["trade_id"] == broker[0]["client_trade_id"]
        assert oms[0]["quantity"] == broker[0]["quantity"]
        if scenario_id == "S-001":
            assert oms[0]["price"] == broker[0]["price"]
        else:
            assert abs(Decimal(oms[0]["price"]) - Decimal(broker[0]["price"])) <= Decimal("0.01")


def test_price_quantity_and_multi_field_breaks() -> None:
    expectations = {
        "S-002": ["PRICE_MISMATCH"],
        "S-004": ["QUANTITY_MISMATCH"],
        "S-005": ["PRICE_MISMATCH", "QUANTITY_MISMATCH"],
    }
    for scenario_id, break_types in expectations.items():
        _, oms, broker, expected = _generate(scenario_id)
        assert expected["expected_reconciliation_status"] == "BREAK"
        assert expected["expected_break_types"] == break_types
        if "PRICE_MISMATCH" in break_types:
            assert oms[0]["price"] != broker[0]["price"]
        if "QUANTITY_MISMATCH" in break_types:
            assert oms[0]["quantity"] != broker[0]["quantity"]


def test_price_and_quantity_validation_boundaries() -> None:
    trade = _base_trade()
    with pytest.raises(ValueError):
        apply_price_mismatch(trade, {"price_tolerance": "0.01", "price_delta": "0.01"})
    with pytest.raises(ValueError):
        apply_price_within_tolerance(trade, {"price_tolerance": "0.01", "price_delta": "0.02"})
    with pytest.raises(ValueError):
        apply_quantity_mismatch(trade, {"quantity_delta": "0.0000001"})
    assert apply_quantity_mismatch(trade, {"quantity_delta": "0.000001"})["quantity"] != trade["quantity"]


def test_missing_confirmation_and_missing_trade_are_source_absence_not_placeholders() -> None:
    _, oms, broker, expected = _generate("S-006")
    assert len(oms) == 1
    assert broker == []
    assert expected["expected_break_types"] == ["MISSING_CONFIRMATION"]
    assert expected["expected_broker_version"] is None

    _, oms, broker, expected = _generate("S-008")
    assert oms == []
    assert len(broker) == 1
    assert expected["expected_break_types"] == ["MISSING_TRADE"]
    assert expected["expected_oms_version"] is None


def test_late_counterpart_scenarios_resolve_historical_missing_breaks() -> None:
    for scenario_id, history in (
        ("S-007", ["MISSING_CONFIRMATION"]),
        ("S-009", ["MISSING_TRADE"]),
    ):
        _, oms, broker, expected = _generate(scenario_id)
        batches = assign_delivery_batches(oms, broker, _scenario(scenario_id))
        assert expected["expected_reconciliation_status"] == "MATCHED"
        assert expected["expected_break_types"] == []
        assert expected["expected_break_history"] == history
        assert len(batches) == 2
        assert batches[1]["delivery_offset_minutes"] > 30


def test_late_confirmation_and_late_trade_have_opposite_arrival_order() -> None:
    _, oms, broker, _ = _generate("S-007")
    batches = assign_delivery_batches(oms, broker, _scenario("S-007"))
    assert batches[0]["oms_events"] == oms
    assert batches[0]["broker_events"] == []
    assert batches[1]["broker_events"] == broker

    _, oms, broker, _ = _generate("S-009")
    batches = assign_delivery_batches(oms, broker, _scenario("S-009"))
    assert batches[0]["broker_events"] == broker
    assert batches[0]["oms_events"] == []
    assert batches[1]["oms_events"] == oms


@pytest.mark.parametrize(
    "field,value",
    [
        ("missing_counterparty_sla_minutes", 0),
        ("late_by_minutes", 0),
        ("missing_counterparty_sla_minutes", -1),
        ("late_by_minutes", -1),
    ],
)
def test_late_delivery_rejects_non_positive_timing(field, value) -> None:
    scenario = _scenario("S-007")
    scenario[field] = value
    _, oms, broker, _ = _generate("S-007")
    with pytest.raises(ValueError):
        assign_delivery_batches(oms, broker, scenario)


def test_match_key_missing_is_a_reconciliation_break() -> None:
    _, oms, broker, expected = _generate("S-010")
    assert oms[0]["trade_id"]
    assert broker[0]["client_trade_id"] is None
    assert expected["expected_reconciliation_status"] == "BREAK"
    assert expected["expected_break_types"] == ["MATCH_KEY_MISSING"]


def test_oms_amendment_uses_local_versions_and_broker_matches_latest() -> None:
    trade, oms, broker, expected = _generate("S-011")
    assert [event["trade_version"] for event in oms] == [1, 2]
    assert [event["event_type"] for event in oms] == ["NEW", "AMEND"]
    assert broker[0]["confirmation_version"] == 1
    assert broker[0]["price"] == oms[-1]["price"]
    assert oms[-1]["price"] != trade["price"]
    assert expected["expected_oms_version"] == 2
    assert expected["expected_broker_version"] == 1


def test_broker_correction_creates_break_then_resolves_to_match() -> None:
    _, oms, broker, expected = _generate("S-012")
    assert [event["confirmation_version"] for event in broker] == [1, 2]
    assert [event["confirmation_type"] for event in broker] == ["CONFIRM", "CORRECT"]
    assert broker[0]["price"] != oms[0]["price"]
    assert broker[1]["price"] == oms[0]["price"]
    assert expected["expected_break_history"] == ["PRICE_MISMATCH"]
    assert expected["expected_reconciliation_status"] == "MATCHED"


def test_out_of_order_versions_are_delivered_newest_first_but_versions_are_preserved() -> None:
    _, oms, broker, expected = _generate("S-013")
    batches = assign_delivery_batches(oms, broker, _scenario("S-013"))
    assert [event["trade_version"] for event in oms] == [1, 2]
    assert batches[0]["oms_events"][0]["trade_version"] == 2
    assert batches[1]["oms_events"][0]["trade_version"] == 1
    assert expected["expected_oms_version"] == 2

    _, oms, broker, expected = _generate("S-014")
    batches = assign_delivery_batches(oms, broker, _scenario("S-014"))
    assert [event["confirmation_version"] for event in broker] == [1, 2]
    assert batches[0]["broker_events"][0]["confirmation_version"] == 2
    assert batches[1]["broker_events"][0]["confirmation_version"] == 1
    assert expected["expected_broker_version"] == 2


def test_duplicate_redeliveries_are_byte_equivalent_payloads() -> None:
    _, oms, _, expected = _generate("S-015")
    assert oms[0] == oms[1]
    assert oms[0] is not oms[1]
    assert expected["expected_reconciliation_status"] == "MATCHED"

    _, _, broker, expected = _generate("S-016")
    assert broker[0] == broker[1]
    assert broker[0] is not broker[1]
    assert expected["expected_reconciliation_status"] == "MATCHED"


def test_duplicate_event_helper_is_defensive_copy() -> None:
    event = {"event_id": "E1", "price": "10"}
    duplicate = duplicate_event(event)
    duplicate["price"] = "11"
    assert event["price"] == "10"


def test_conflicting_redeliveries_keep_id_but_change_payload_and_are_quarantinable() -> None:
    for scenario_id, key in (("S-017", "event_id"), ("S-018", "confirmation_event_id")):
        _, oms, broker, expected = _generate(scenario_id)
        events = oms if scenario_id == "S-017" else broker
        assert events[0][key] == events[1][key]
        assert events[0]["price"] != events[1]["price"]
        assert expected["expected_validation_errors"] == ["CONFLICTING_EVENT_ID"]
        assert expected["expected_reconciliation_status"] == "MATCHED"


def test_conflicting_redelivery_requires_real_conflict() -> None:
    event = {"event_id": "E1", "price": "10"}
    with pytest.raises(ValueError):
        create_conflicting_redelivery(event, "missing", "x")
    with pytest.raises(ValueError):
        create_conflicting_redelivery(event, "price", "10")


def test_duplicate_candidate_scenarios_generate_distinct_source_records_for_same_business_key() -> None:
    _, oms, broker, expected = _generate("S-019")
    assert oms[0]["trade_id"] == oms[1]["trade_id"]
    assert oms[0]["event_id"] != oms[1]["event_id"]
    assert expected["expected_break_types"] == ["DUPLICATE_INTERNAL_CANDIDATE"]

    _, oms, broker, expected = _generate("S-020")
    assert broker[0]["client_trade_id"] == broker[1]["client_trade_id"]
    assert broker[0]["confirmation_event_id"] != broker[1]["confirmation_event_id"]
    assert expected["expected_break_types"] == ["DUPLICATE_EXTERNAL_CANDIDATE"]


def test_cancellation_scenarios_use_local_v2_cancel_events() -> None:
    expectations = {
        "S-021": (2, 1, ["OMS_CANCEL_BROKER_ACTIVE"]),
        "S-022": (1, 2, ["BROKER_CANCEL_OMS_ACTIVE"]),
        "S-023": (2, 2, []),
    }
    for scenario_id, (oms_version, broker_version, breaks) in expectations.items():
        _, oms, broker, expected = _generate(scenario_id)
        assert max(event["trade_version"] for event in oms) == oms_version
        assert max(event["confirmation_version"] for event in broker) == broker_version
        if oms_version == 2:
            assert oms[-1]["event_type"] == "CANCEL"
        if broker_version == 2:
            assert broker[-1]["confirmation_type"] == "CANCEL"
        assert expected["expected_break_types"] == breaks


def test_late_cancel_changes_state_only_in_later_delivery_batch() -> None:
    _, oms, broker, expected = _generate("S-024")
    batches = assign_delivery_batches(oms, broker, _scenario("S-024"))
    assert batches[0]["oms_events"][0]["event_type"] == "NEW"
    assert batches[0]["broker_events"][0]["confirmation_type"] == "CONFIRM"
    assert batches[1]["oms_events"][0]["event_type"] == "CANCEL"
    assert batches[1]["delivery_offset_minutes"] > 30
    assert expected["expected_break_types"] == ["OMS_CANCEL_BROKER_ACTIVE"]


def test_invalid_payload_scenarios_are_quarantined_with_expected_validation_error() -> None:
    expectations = {
        "S-025": ("REQUIRED_VALUE_MISSING", "instrument_id", None),
        "S-026": ("INVALID_SCHEMA", "schema_version", "999.0"),
        "S-027": ("INVALID_DATA_TYPE", "quantity", "NOT_A_NUMBER"),
        "S-028": ("INVALID_ENUM", "side", "HOLD"),
    }
    for scenario_id, (error, field, value) in expectations.items():
        _, oms, broker, expected = _generate(scenario_id)
        assert broker == []
        assert oms[0][field] == value
        assert expected["expected_reconciliation_status"] == "QUARANTINED"
        assert expected["expected_validation_errors"] == [error]


def test_broker_and_account_mismatch_change_only_target_field() -> None:
    _, oms, broker, expected = _generate("S-029")
    assert oms[0]["broker_id"] != broker[0]["broker_id"]
    assert oms[0]["account_id"] == broker[0]["client_account"]
    assert expected["expected_break_types"] == ["BROKER_MISMATCH"]

    _, oms, broker, expected = _generate("S-030")
    assert oms[0]["account_id"] != broker[0]["client_account"]
    assert oms[0]["broker_id"] == broker[0]["broker_id"]
    assert expected["expected_break_types"] == ["ACCOUNT_MISMATCH"]


def test_s031_has_one_missing_episode_then_oms_v2_and_matching_broker_v2() -> None:
    _, oms, broker, expected = _generate("S-031")
    batches = assign_delivery_batches(oms, broker, _scenario("S-031"))
    assert [event["trade_version"] for event in oms] == [1, 2]
    assert broker[0]["confirmation_version"] == 2
    assert broker[0]["price"] == oms[-1]["price"]
    assert [batch["delivery_offset_minutes"] for batch in batches] == [0, 35, 36]
    assert batches[0]["oms_events"][0]["trade_version"] == 1
    assert batches[0]["broker_events"] == []
    assert batches[1]["oms_events"][0]["trade_version"] == 2
    assert batches[2]["broker_events"][0]["confirmation_version"] == 2
    assert expected["expected_reconciliation_status"] == "MATCHED"
    assert expected["expected_break_types"] == []
    assert expected["expected_break_history"] == ["MISSING_CONFIRMATION"]
    assert expected["expected_break_occurrences"] == {"MISSING_CONFIRMATION": 1}


def test_public_lifecycle_helpers_are_deterministic() -> None:
    trade = _base_trade()
    scenario = _scenario("S-011")
    assert apply_oms_amendment(trade, scenario) == apply_oms_amendment(trade, scenario)
    correction_scenario = _scenario("S-012")
    assert apply_broker_correction(trade, correction_scenario) == apply_broker_correction(trade, correction_scenario)
    cancel_scenario = {"cancel_side": "BOTH"}
    assert apply_cancellation(trade, cancel_scenario) == apply_cancellation(trade, cancel_scenario)


def test_writers_chunk_deterministically(tmp_path: Path) -> None:
    events = []
    for index in range(3):
        _, oms, broker, _ = _generate("S-001")
        oms[0]["event_id"] = f"OMS-{index}"
        broker[0]["confirmation_event_id"] = f"BRK-{index}"
        events.append((oms[0], broker[0]))

    oms_paths = write_oms_jsonl([item[0] for item in events], tmp_path / "oms", 2)
    broker_paths = write_broker_csv([item[1] for item in events], tmp_path / "broker", 2)
    assert [path.name for path in oms_paths] == ["oms_part_00001.jsonl", "oms_part_00002.jsonl"]
    assert [path.name for path in broker_paths] == ["broker_a_part_00001.csv", "broker_a_part_00002.csv"]
    assert len(oms_paths[0].read_text(encoding="utf-8").splitlines()) == 2
    with broker_paths[0].open(encoding="utf-8") as handle:
        assert len(list(csv.DictReader(handle))) == 2


def test_empty_writers_do_not_create_empty_source_files(tmp_path: Path) -> None:
    assert write_oms_jsonl([], tmp_path / "oms", 10) == []
    assert write_broker_csv([], tmp_path / "broker", 10) == []


def test_generate_dataset_builds_portable_manifest_and_all_scenarios(tmp_path: Path) -> None:
    manifest = generate_dataset(
        {
            "seed": 12345,
            "business_date": "2026-09-07",
            "scenario_ids": list(SCENARIO_NAMES),
            "trade_count": 31,
            "records_per_file": 10,
            "output_path": tmp_path,
        }
    )
    assert manifest["trade_count"] == 31
    assert set(manifest["scenario_counts"]) == set(SCENARIO_NAMES)
    assert all(value == 1 for value in manifest["scenario_counts"].values())
    assert manifest["source_event_count"] == manifest["oms_event_count"] + manifest["broker_event_count"]
    assert len(manifest["expected_results"]) == 31
    assert (tmp_path / "generation_manifest.json").exists()
    for batch in manifest["source_files"]:
        assert all(not Path(path).is_absolute() for path in batch["oms_files"] + batch["broker_files"])


def test_generate_dataset_is_deterministic_across_output_locations(tmp_path: Path) -> None:
    first = generate_dataset(
        {
            "seed": 7,
            "business_date": "2026-09-07",
            "scenario_ids": ["S-001", "S-007", "S-031"],
            "trade_count": 6,
            "output_path": tmp_path / "first",
        }
    )
    second = generate_dataset(
        {
            "seed": 7,
            "business_date": "2026-09-07",
            "scenario_ids": ["S-001", "S-007", "S-031"],
            "trade_count": 6,
            "output_path": tmp_path / "second",
        }
    )
    assert first == second


def test_manifest_writer_is_stable(tmp_path: Path) -> None:
    manifest = {"b": 2, "a": 1}
    path = write_manifest(manifest, tmp_path)
    assert json.loads(path.read_text(encoding="utf-8")) == manifest
    assert path.read_text(encoding="utf-8").endswith("\n")


def test_validate_generated_dataset_rejects_inconsistent_counts(tmp_path: Path) -> None:
    manifest = generate_dataset(
        {
            "seed": 1,
            "business_date": "2026-09-07",
            "scenario_ids": ["S-006"],
            "trade_count": 1,
            "output_path": tmp_path,
        }
    )
    broken = dict(manifest)
    broken["source_event_count"] = 999
    with pytest.raises(ValueError, match="source_event_count"):
        validate_generated_dataset(broken)


def test_validate_generated_dataset_rejects_second_missing_episode_for_s031(tmp_path: Path) -> None:
    manifest = generate_dataset(
        {
            "seed": 1,
            "business_date": "2026-09-07",
            "scenario_ids": ["S-031"],
            "trade_count": 1,
            "output_path": tmp_path,
        }
    )
    broken = json.loads(json.dumps(manifest))
    broken["expected_results"][0]["expected_break_occurrences"] = {"MISSING_CONFIRMATION": 2}
    with pytest.raises(ValueError, match="exactly one"):
        validate_generated_dataset(broken)


def test_generate_dataset_requires_explicit_business_date(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="business_date"):
        generate_dataset({"output_path": tmp_path})
