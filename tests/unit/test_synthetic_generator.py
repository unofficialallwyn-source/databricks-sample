"""TR-016 synthetic generator tests.

Only the base-trade milestone is enabled for now.
Enable the remaining scenario tests incrementally as each capability is implemented.
"""

import csv
from datetime import date
from decimal import Decimal
import json
from pathlib import Path
from random import Random

import pytest

from src.trade_recon.synthetic.generator import apply_price_mismatch, generate_base_trade, generate_broker_events, generate_oms_events, generate_scenario, write_broker_csv, write_manifest, write_oms_jsonl



def test_generate_base_trade_is_deterministic_for_same_seed() -> None:
    """Same seed, index, and business date should generate the same base trade."""
    business_date = date(2026, 9, 7)

    first = generate_base_trade(1, business_date, Random(12345))
    second = generate_base_trade(1, business_date, Random(12345))

    assert first == second


def test_generate_base_trade_is_source_neutral_and_valid() -> None:
    """Base trade should contain business economics, not OMS/Broker event metadata."""
    business_date = date(2026, 9, 7)

    trade = generate_base_trade(7, business_date, Random(12345))

    assert trade["business_trade_id"] == "TRD00000007"
    assert trade["trade_date"] == "2026-09-07"
    assert trade["settlement_date"] == "2026-09-09"
    assert trade["instrument_type"] == "EQUITY"
    assert trade["broker_id"] == "BROKER_A"
    assert trade["execution_timestamp"].endswith("+00:00")
    assert Decimal(trade["quantity"]) > 0
    assert Decimal(trade["price"]) > 0

    assert "event_id" not in trade
    assert "trade_version" not in trade
    assert "schema_version" not in trade

def test_oms_trade_and_broker_trade_is_exact_match() -> None:
    """OMS trade and Broker trade should contain business economics, and should exactly match."""
    business_date = date(2026, 9, 7)

    trade = generate_base_trade(7, business_date, Random(12345))
    oms_event = generate_oms_events(trade=trade,scenario=None,rng=Random(12345))
    broker_event = generate_broker_events(trade=trade,scenario=None,rng=Random(12345))

    assert oms_event[0].get("trade_id") == broker_event[0].get("client_trade_id")
    assert oms_event[0].get("instrument_id") == broker_event[0].get("instrument_code")
    assert oms_event[0].get("side") == broker_event[0].get("side")
    assert oms_event[0].get("quantity") == broker_event[0].get("quantity")
    assert oms_event[0].get("price") == broker_event[0].get("price")
    assert oms_event[0].get("currency") == broker_event[0].get("currency")
    assert oms_event[0].get("account_id") == broker_event[0].get("client_account")
    assert oms_event[0].get("broker_id") == broker_event[0].get("broker_id")
    assert oms_event[0].get("trade_version") == 1
    assert broker_event[0].get("confirmation_version") == 1
    assert oms_event[0].get("published_at") >= oms_event[0].get("event_time")
    assert broker_event[0].get("published_at") >= broker_event[0].get("confirmation_time")


def test_write_oms_jsonl_creates_expected_files(tmp_path):
    business_date = date(2026, 9, 7)
    trade1 = generate_base_trade(1, business_date, Random(1986))
    trade2 = generate_base_trade(2, business_date, Random(7433))
    trade3 = generate_base_trade(3, business_date, Random(5860))
    oms_event1 = generate_oms_events(trade=trade1,scenario=None,rng=Random(1986))
    oms_event2 = generate_oms_events(trade=trade2,scenario=None,rng=Random(7433))
    oms_event3 = generate_oms_events(trade=trade3,scenario=None,rng=Random(5860))
    oms_events = list()
    oms_events.append(oms_event1[0])
    oms_events.append(oms_event2[0])
    oms_events.append(oms_event3[0])
    
    output_folder = tmp_path/"oms_jsonl_trades"
    file_paths = write_oms_jsonl(output_path=output_folder,events=oms_events,records_per_file=2)

    file_records = []
    with open(file_paths[0], "r", encoding="utf-8") as f:
        for line in f:
            clean_line = line.strip()
            if clean_line:
                file_records.append(json.loads(clean_line))

    file2_records = []
    with open(file_paths[1], "r", encoding="utf-8") as f:
        for line in f:
            clean_line = line.strip()
            if clean_line:
                file2_records.append(json.loads(clean_line))

    file_names = {path.name for path in file_paths}

    assert len(file_paths) == 2
    assert file_names == {"oms_part_00001.jsonl", "oms_part_00002.jsonl"}
    assert file_records[0]["event_id"] == oms_event1[0]["event_id"]
    assert file_records[1]["event_id"] == oms_event2[0]["event_id"]
    assert file2_records[0]["event_id"] == oms_event3[0]["event_id"]
    assert len(file_records) == 2
    assert len(file2_records) == 1

def test_write_oms_jsonl_creates_exactly_1_file(tmp_path):
    business_date = date(2026, 9, 7)
    trade1 = generate_base_trade(1, business_date, Random(1986))
    trade2 = generate_base_trade(2, business_date, Random(7433))
    oms_event1 = generate_oms_events(trade=trade1,scenario=None,rng=Random(1986))
    oms_event2 = generate_oms_events(trade=trade2,scenario=None,rng=Random(7433))
    
    oms_events = list()
    oms_events.append(oms_event1[0])
    oms_events.append(oms_event2[0])
    
    output_folder = tmp_path/"oms_jsonl_trades"
    file_paths = write_oms_jsonl(output_path=output_folder,events=oms_events,records_per_file=2)

    file_records = []
    with open(file_paths[0], "r", encoding="utf-8") as f:
        for line in f:
            clean_line = line.strip()
            if clean_line:
                file_records.append(json.loads(clean_line))

    file_names = {path.name for path in file_paths}

    assert len(file_paths) == 1
    assert file_names == {"oms_part_00001.jsonl"}
    assert file_records[0]["event_id"] == oms_event1[0]["event_id"]
    assert file_records[1]["event_id"] == oms_event2[0]["event_id"]
    assert len(file_records) == 2

def test_write_oms_jsonl_with_0_records_per_file(tmp_path):
    business_date = date(2026, 9, 7)
    trade1 = generate_base_trade(1, business_date, Random(1986))
    trade2 = generate_base_trade(2, business_date, Random(7433))
    oms_event1 = generate_oms_events(trade=trade1,scenario=None,rng=Random(1986))
    oms_event2 = generate_oms_events(trade=trade2,scenario=None,rng=Random(7433))
    
    oms_events = list()
    oms_events.append(oms_event1[0])
    oms_events.append(oms_event2[0])
    
    output_folder = tmp_path/"oms_jsonl_trades"
    with pytest.raises(ValueError):
        write_oms_jsonl(output_path=output_folder,events=oms_events,records_per_file=0)

def test_write_broker_csv_creates_expected_files(tmp_path):
    business_date = date(2026, 9, 7)
    trade1 = generate_base_trade(1, business_date, Random(1986))
    trade2 = generate_base_trade(2, business_date, Random(7433))
    trade3 = generate_base_trade(3, business_date, Random(5860))
    broker_event1 = generate_broker_events(trade=trade1,scenario=None,rng=Random(1986))
    broker_event2 = generate_broker_events(trade=trade2,scenario=None,rng=Random(7433))
    broker_event3 = generate_broker_events(trade=trade3,scenario=None,rng=Random(5860))
    broker_events = list()
    broker_events.append(broker_event1[0])
    broker_events.append(broker_event2[0])
    broker_events.append(broker_event3[0])
    
    output_folder = tmp_path/"broker_a_csv_trades"
    file_paths = write_broker_csv(output_path=output_folder,events=broker_events,records_per_file=2)

    file_records = []
    with open(file_paths[0], "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        file_records.extend(list(reader))

    file2_records = []
    with open(file_paths[1], "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        file2_records.extend(list(reader))
                
    file_names = {path.name for path in file_paths}

    assert len(file_paths) == 2
    assert file_names == {"broker_a_part_00001.csv", "broker_a_part_00002.csv"}
    assert headers == ["confirmation_event_id","broker_trade_id","client_trade_id","confirmation_version",
                "confirmation_type","confirmation_time","schema_version","instrument_code",
                "instrument_type","side","quantity","price","currency","client_account","broker_id",
                "venue","trade_date","execution_timestamp","settlement_date","published_at"]
    assert file_records[0]['confirmation_event_id'] == broker_event1[0]["confirmation_event_id"]
    assert file_records[1]['confirmation_event_id'] == broker_event2[0]["confirmation_event_id"]
    assert file2_records[0]['confirmation_event_id'] == broker_event3[0]["confirmation_event_id"]
    assert file_records[0]['client_trade_id'] == broker_event1[0]["client_trade_id"]
    assert file_records[0]['quantity'] == broker_event1[0]["quantity"]
    assert file_records[0]['price'] == broker_event1[0]["price"]
    assert file_records[0]['side'] == broker_event1[0]["side"]
    assert file_records[0]['currency'] == broker_event1[0]["currency"]
    assert file_records[1]['client_trade_id'] == broker_event2[0]["client_trade_id"]
    assert file_records[1]['quantity'] == broker_event2[0]["quantity"]
    assert file_records[1]['price'] == broker_event2[0]["price"]
    assert file_records[1]['side'] == broker_event2[0]["side"]
    assert file_records[1]['currency'] == broker_event2[0]["currency"]
    assert file2_records[0]['client_trade_id'] == broker_event3[0]["client_trade_id"]
    assert file2_records[0]['quantity'] == broker_event3[0]["quantity"]
    assert file2_records[0]['price'] == broker_event3[0]["price"]
    assert file2_records[0]['side'] == broker_event3[0]["side"]
    assert file2_records[0]['currency'] == broker_event3[0]["currency"]
    assert len(file_records) == 2
    assert len(file2_records) == 1

def test_write_broker_csv_creates_exactly_1_file(tmp_path):
    business_date = date(2026, 9, 7)
    trade1 = generate_base_trade(1, business_date, Random(1986))
    trade2 = generate_base_trade(2, business_date, Random(7433))
    broker_event1 = generate_broker_events(trade=trade1,scenario=None,rng=Random(1986))
    broker_event2 = generate_broker_events(trade=trade2,scenario=None,rng=Random(7433))
    
    broker_events = list()
    broker_events.append(broker_event1[0])
    broker_events.append(broker_event2[0])
    
    output_folder = tmp_path/"broker_a_csv_trades"
    file_paths = write_broker_csv(output_path=output_folder,events=broker_events,records_per_file=2)

    file_records = []
    with open(file_paths[0], "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        file_records.extend(list(reader))

    file_names = {path.name for path in file_paths}

    assert len(file_paths) == 1
    assert headers == ["confirmation_event_id","broker_trade_id","client_trade_id","confirmation_version",
                "confirmation_type","confirmation_time","schema_version","instrument_code",
                "instrument_type","side","quantity","price","currency","client_account","broker_id",
                "venue","trade_date","execution_timestamp","settlement_date","published_at"]
    assert file_names == {"broker_a_part_00001.csv"}
    assert file_records[0]['confirmation_event_id'] == broker_event1[0]["confirmation_event_id"]
    assert file_records[1]['confirmation_event_id'] == broker_event2[0]["confirmation_event_id"]
    assert file_records[0]['client_trade_id'] == broker_event1[0]["client_trade_id"]
    assert file_records[0]['quantity'] == broker_event1[0]["quantity"]
    assert file_records[0]['price'] == broker_event1[0]["price"]
    assert file_records[0]['side'] == broker_event1[0]["side"]
    assert file_records[0]['currency'] == broker_event1[0]["currency"]
    assert file_records[1]['client_trade_id'] == broker_event2[0]["client_trade_id"]
    assert file_records[1]['quantity'] == broker_event2[0]["quantity"]
    assert file_records[1]['price'] == broker_event2[0]["price"]
    assert file_records[1]['side'] == broker_event2[0]["side"]
    assert file_records[1]['currency'] == broker_event2[0]["currency"]
    assert len(file_records) == 2

def test_write_broker_csv_with_0_records_per_file(tmp_path):
    business_date = date(2026, 9, 7)
    trade1 = generate_base_trade(1, business_date, Random(1986))
    trade2 = generate_base_trade(2, business_date, Random(7433))
    broker_event1 = generate_broker_events(trade=trade1,scenario=None,rng=Random(1986))
    broker_event2 = generate_broker_events(trade=trade2,scenario=None,rng=Random(7433))
    
    broker_events = list()
    broker_events.append(broker_event1[0])
    broker_events.append(broker_event2[0])
    
    output_folder = tmp_path/"broker_a_csv_trades"
    with pytest.raises(ValueError):
        write_broker_csv(output_path=output_folder,events=broker_events,records_per_file=0)

def test_write_manifest_creates_expected_json(tmp_path):
    manifest = {
        "business_date": "2026-09-09",
        "expected_results": [],
        "files": {
            "broker": [
                "broker_a_part_00001.csv",
                "broker_a_part_00002.csv"
            ],
            "oms": [
                "oms_part_00001.jsonl",
                "oms_part_00002.jsonl"
            ]
        },
        "generator_version": "1.0",
        "seed": 12345,
        "source_counts": {
            "broker_events": 3,
            "broker_files": 2,
            "oms_events": 3,
            "oms_files": 2
        }
    }

    manifest_path = write_manifest(output_path=tmp_path, manifest=manifest)

    with open(manifest_path, "r", encoding="utf-8") as f:
        loaded_manifest = json.load(f)

    assert manifest_path.name == "generation_manifest.json"
    assert loaded_manifest == manifest
    assert loaded_manifest["business_date"] == manifest["business_date"]
    assert loaded_manifest["generator_version"] == manifest["generator_version"]
    assert loaded_manifest["seed"] == manifest["seed"]
    assert loaded_manifest["source_counts"]["broker_events"] == manifest["source_counts"]["broker_events"]
    assert loaded_manifest["source_counts"]["broker_files"] == manifest["source_counts"]["broker_files"]
    assert loaded_manifest["source_counts"]["oms_events"] == manifest["source_counts"]["oms_events"]
    assert loaded_manifest["source_counts"]["oms_files"] == manifest["source_counts"]["oms_files"]
    assert loaded_manifest["files"]["broker"] == manifest["files"]["broker"]
    assert loaded_manifest["files"]["oms"] == manifest["files"]["oms"]

def test_write_manifest_is_deterministic(tmp_path):
    """Writing the same manifest twice should produce identical files."""
    manifest = {
        "business_date": "2026-09-09",
        "expected_results": [],
        "files": {
            "broker": [
                "broker_a_part_00001.csv",
                "broker_a_part_00002.csv"
            ],
            "oms": [
                "oms_part_00001.jsonl",
                "oms_part_00002.jsonl"
            ]
        },
        "generator_version": "1.0",
        "seed": 12345,
        "source_counts": {
            "broker_events": 3,
            "broker_files": 2,
            "oms_events": 3,
            "oms_files": 2
        }
    }

    manifest_path1 = write_manifest(output_path=tmp_path, manifest=manifest)
    with open(manifest_path1, "r", encoding="utf-8") as f1:
            content1 = f1.read()
            
    manifest_path2 = write_manifest(output_path=tmp_path, manifest=manifest)
    with open(manifest_path2, "r", encoding="utf-8") as f2:
        content2 = f2.read()

    assert content1 == content2

def test_manifest_contains_relative_filenames_only(tmp_path):
    """Manifest file paths should be relative, not absolute."""
    manifest = {
        "business_date": "2026-09-09",
        "expected_results": [],
        "files": {
            "broker": [
                "broker_a_part_00001.csv",
                "broker_a_part_00002.csv"
            ],
            "oms": [
                "oms_part_00001.jsonl",
                "oms_part_00002.jsonl"
            ]
        },
        "generator_version": "1.0",
        "seed": 12345,
        "source_counts": {
            "broker_events": 3,
            "broker_files": 2,
            "oms_events": 3,
            "oms_files": 2
        }
    }

    manifest_path = write_manifest(output_path=tmp_path, manifest=manifest)

    with open(manifest_path, "r", encoding="utf-8") as f:
        loaded_manifest = json.load(f)

    for file_list in loaded_manifest["files"].values():
        for file_name in file_list:
            assert not Path(file_name).is_absolute(), f"Manifest file path {file_name} should be relative, not absolute."

@pytest.mark.skip(reason="Implement TR-016 dataset orchestration.")
def test_same_seed_produces_same_logical_dataset() -> None:
    """Same seed/config/business date should generate identical logical data."""
    pytest.fail("Implement deterministic generation test")


@pytest.mark.skip(reason="Implement TR-016 dataset orchestration.")
def test_different_seed_changes_generated_dataset() -> None:
    """Different seeds should produce a different logical dataset."""
    pytest.fail("Implement seed variation test")


def test_exact_match_scenario_produces_matching_economics() -> None:
    """EXACT_MATCH should generate OMS and Broker records with matching economics."""
    base_trade = generate_base_trade(1, date(2026, 9, 7), Random(12345))
    scenario = {
        "scenario_id": "S-001",
        "scenario_name": "EXACT_MATCH",
    }

    oms_events, broker_events, expected_result = generate_scenario(
        trade=base_trade,
        scenario=scenario,
        rng=Random(12345),
    )

    assert len(oms_events) == 1
    assert len(broker_events) == 1
    assert oms_events[0]["trade_id"] == broker_events[0]["client_trade_id"]
    assert oms_events[0]["instrument_id"] == broker_events[0]["instrument_code"]
    assert oms_events[0]["side"] == broker_events[0]["side"]
    assert oms_events[0]["quantity"] == broker_events[0]["quantity"]
    assert oms_events[0]["price"] == broker_events[0]["price"]
    assert oms_events[0]["currency"] == broker_events[0]["currency"]
    assert oms_events[0]["account_id"] == broker_events[0]["client_account"]
    assert oms_events[0]["broker_id"] == broker_events[0]["broker_id"]
    assert expected_result["scenario_id"] == "S-001"
    assert expected_result["business_trade_id"] == base_trade["business_trade_id"]
    assert expected_result["expected_reconciliation_status"] == "MATCHED"
    assert expected_result["expected_break_types"] == []
    assert expected_result["expected_oms_version"] == 1
    assert expected_result["expected_broker_version"] == 1
    assert oms_events[0]["trade_version"] == 1
    assert broker_events[0]["confirmation_version"] == 1

def test_generate_scenario_rejects_missing_scenario_id():
    """generate_scenario should raise ValueError if scenario_id is missing."""
    base_trade = generate_base_trade(1, date(2026, 9, 7), Random(12345))
    scenario = {
        "scenario_name": "EXACT_MATCH",
    }

    with pytest.raises(ValueError) as exc_info:
        generate_scenario(
            trade=base_trade,
            scenario=scenario,
            rng=Random(12345),
        )

    assert "scenario_id is required in scenario config" in str(exc_info.value)

def test_generate_scenario_rejects_unsupported_scenario():
    """generate_scenario should raise ValueError if scenario_id is unsupported."""
    base_trade = generate_base_trade(1, date(2026, 9, 7), Random(12345))
    scenario = {
        "scenario_id": "S-999",
        "scenario_name": "PRICE_MISMATCH",
    }

    with pytest.raises(ValueError) as exc_info:
        generate_scenario(
            trade=base_trade,
            scenario=scenario,
            rng=Random(12345),
        )

    assert "Unsupported scenario_id: S-999" in str(exc_info.value)

def test_price_mismatch_exceeds_configured_tolerance() -> None:
    """PRICE_MISMATCH should exceed the configured reconciliation tolerance."""
    base_trade = generate_base_trade(1, date(2026, 9, 7), Random(12345))
    scenario = {
        "scenario_id": "S-002",
        "scenario_name": "PRICE_MISMATCH",
        "price_tolerance": "0.01",
        "price_delta": "0.02",
    }

    oms_events, broker_events, expected_result = generate_scenario(
        trade=base_trade,
        scenario=scenario,
        rng=Random(12345),
    )

    assert len(oms_events) == 1
    assert len(broker_events) == 1
    assert expected_result["scenario_id"] == "S-002"
    assert expected_result["expected_reconciliation_status"] == "BREAK"
    assert expected_result["expected_break_types"] == ["PRICE_MISMATCH"]
    assert oms_events[0]["trade_id"] == broker_events[0]["client_trade_id"]
    assert oms_events[0]["instrument_id"] == broker_events[0]["instrument_code"]
    assert oms_events[0]["side"] == broker_events[0]["side"]
    assert oms_events[0]["quantity"] == broker_events[0]["quantity"]
    assert oms_events[0]["price"] != broker_events[0]["price"]
    assert oms_events[0]["currency"] == broker_events[0]["currency"]
    assert oms_events[0]["account_id"] == broker_events[0]["client_account"]
    assert oms_events[0]["broker_id"] == broker_events[0]["broker_id"]
    assert expected_result["business_trade_id"] == base_trade["business_trade_id"]
    assert expected_result["expected_oms_version"] == 1
    assert expected_result["expected_broker_version"] == 1
    assert oms_events[0]["trade_version"] == 1
    assert broker_events[0]["confirmation_version"] == 1
    assert abs(Decimal(broker_events[0]["price"]) - Decimal(oms_events[0]["price"])) > Decimal("0.01")
    assert Decimal(broker_events[0]["price"]) - Decimal(oms_events[0]["price"]) == Decimal("0.02")

def test_price_mismatch_zero_tolerance_with_positive_delta() -> None:
    """PRICE_MISMATCH should allow zero tolerance with positive delta."""
    base_trade = generate_base_trade(1, date(2026, 9, 7), Random(12345))
    scenario = {
        "scenario_id": "S-002",
        "scenario_name": "PRICE_MISMATCH",
        "price_tolerance": "0.00",
        "price_delta": "0.02",
    }

    oms_events, broker_events, expected_result = generate_scenario(
        trade=base_trade,
        scenario=scenario,
        rng=Random(12345),
    )

    assert len(oms_events) == 1
    assert len(broker_events) == 1
    assert expected_result["scenario_id"] == "S-002"
    assert expected_result["expected_reconciliation_status"] == "BREAK"
    assert expected_result["expected_break_types"] == ["PRICE_MISMATCH"]
    assert oms_events[0]["trade_id"] == broker_events[0]["client_trade_id"]
    assert oms_events[0]["instrument_id"] == broker_events[0]["instrument_code"]
    assert oms_events[0]["side"] == broker_events[0]["side"]
    assert oms_events[0]["quantity"] == broker_events[0]["quantity"]
    assert oms_events[0]["price"] != broker_events[0]["price"]
    assert oms_events[0]["currency"] == broker_events[0]["currency"]
    assert oms_events[0]["account_id"] == broker_events[0]["client_account"]
    assert oms_events[0]["broker_id"] == broker_events[0]["broker_id"]
    assert expected_result["business_trade_id"] == base_trade["business_trade_id"]
    assert expected_result["expected_oms_version"] == 1
    assert expected_result["expected_broker_version"] == 1
    assert oms_events[0]["trade_version"] == 1
    assert broker_events[0]["confirmation_version"] == 1

def test_price_mismatch_rejects_delta_below_tolerance() -> None:
    """PRICE_MISMATCH should reject a delta that is below the configured tolerance."""
    base_trade = generate_base_trade(1, date(2026, 9, 7), Random(12345))
    scenario = {
        "scenario_id": "S-002",
        "scenario_name": "PRICE_MISMATCH",
        "price_tolerance": "0.01",
        "price_delta": "0.005",
    }

    with pytest.raises(ValueError) as exc_info:
        generate_scenario(
            trade=base_trade,
            scenario=scenario,
            rng=Random(12345),
        )

    assert "Price delta must exceed price tolerance" in str(exc_info.value)

def test_price_mismatch_rejects_non_positive_delta() -> None:
    """PRICE_MISMATCH should reject a non-positive delta."""
    base_trade = generate_base_trade(1, date(2026, 9, 7), Random(12345))
    scenario = {
        "scenario_id": "S-002",
        "scenario_name": "PRICE_MISMATCH",
        "price_tolerance": "0.01",
        "price_delta": "-0.01",
    }

    with pytest.raises(ValueError) as exc_info:
        generate_scenario(
            trade=base_trade,
            scenario=scenario,
            rng=Random(12345),
        )

    assert "Price tolerance or delta must be positive" in str(exc_info.value)

def test_price_mismatch_rejects_negative_tolerance() -> None:
    """PRICE_MISMATCH should reject a negative tolerance."""
    base_trade = generate_base_trade(1, date(2026, 9, 7), Random(12345))
    scenario = {
        "scenario_id": "S-002",
        "scenario_name": "PRICE_MISMATCH",
        "price_tolerance": "-0.01",
        "price_delta": "0.02",
    }

    with pytest.raises(ValueError) as exc_info:
        generate_scenario(
            trade=base_trade,
            scenario=scenario,
            rng=Random(12345),
        )

    assert "Price tolerance or delta must be positive" in str(exc_info.value)

def test_apply_price_mismatch_does_not_mutate_base_trade():
    base_trade = generate_base_trade(1, date(2026, 9, 7), Random(12345))
    scenario = {
        "scenario_id": "S-002",
        "scenario_name": "PRICE_MISMATCH",
        "price_tolerance": "0.01",
        "price_delta": "0.02",
    }

    base_trade_price = Decimal(base_trade["price"])

    mismatched_trade = apply_price_mismatch(base_trade, scenario)

    assert base_trade["business_trade_id"] == mismatched_trade["business_trade_id"]
    assert Decimal(base_trade["price"]) == base_trade_price
    assert Decimal(base_trade["price"]) != Decimal(mismatched_trade["price"])

def test_price_mismatch_rejects_delta_within_tolerance():
    base_trade = generate_base_trade(1, date(2026, 9, 7), Random(12345))
    scenario = {
        "scenario_id": "S-002",
        "scenario_name": "PRICE_MISMATCH",
        "price_tolerance": "0.01",
        "price_delta": "0.01",
    }

    with pytest.raises(ValueError) as exc_info:
        apply_price_mismatch(base_trade, scenario)

    assert "Price delta must exceed price tolerance" in str(exc_info.value)

@pytest.mark.skip(reason="Implement S-003 PRICE_WITHIN_TOLERANCE.")
def test_price_within_tolerance_stays_within_boundary() -> None:
    """PRICE_WITHIN_TOLERANCE should remain inside the configured tolerance."""
    pytest.fail("Implement PRICE_WITHIN_TOLERANCE scenario test")


@pytest.mark.skip(reason="Implement S-007 LATE_CONFIRMATION.")
def test_late_confirmation_uses_later_delivery_phase() -> None:
    """LATE_CONFIRMATION should place Broker delivery after the OMS/SLA phase."""
    pytest.fail("Implement staged delivery test")


@pytest.mark.skip(reason="Implement S-013 OUT_OF_ORDER_OMS.")
def test_out_of_order_oms_delivers_v2_before_v1() -> None:
    """OUT_OF_ORDER_OMS should preserve source versions but reverse delivery order."""
    pytest.fail("Implement out-of-order OMS test")


@pytest.mark.skip(reason="Implement duplicate-event scenarios.")
def test_duplicate_event_reuses_event_id_and_payload() -> None:
    """Duplicate redelivery must reuse the event ID with identical payload."""
    pytest.fail("Implement duplicate event test")


@pytest.mark.skip(reason="Implement conflicting-event scenarios.")
def test_conflicting_redelivery_reuses_id_but_changes_payload() -> None:
    """Conflicting redelivery must reuse the event ID while altering payload content."""
    pytest.fail("Implement conflicting event test")


@pytest.mark.skip(reason="Implement S-011 OMS_AMENDMENT.")
def test_oms_amendment_uses_source_local_versions() -> None:
    """OMS_AMENDMENT should generate increasing OMS versions without implying Broker version parity."""
    pytest.fail("Implement OMS amendment test")


@pytest.mark.skip(reason="Implement S-031 regression scenario.")
def test_oms_version_after_missing_break_has_staged_inputs() -> None:
    """S-031 should produce OMS v1, missing phase, OMS v2, then late Broker confirmation."""
    pytest.fail("Implement S-031 regression-input test")


@pytest.mark.skip(reason="Implement generation manifest.")
def test_manifest_counts_match_generated_source_events() -> None:
    """Manifest source-event counts should match generated OMS/Broker outputs."""
    pytest.fail("Implement manifest accounting test")


@pytest.mark.skip(reason="Implement generator self-validation.")
def test_generator_validation_rejects_internal_inconsistency() -> None:
    """Generator self-validation should fail when scenario invariants are violated."""
    pytest.fail("Implement generator validation test")
