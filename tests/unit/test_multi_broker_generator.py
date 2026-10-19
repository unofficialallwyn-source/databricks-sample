import csv
from datetime import date
from pathlib import Path
from random import Random
import sys

SYNTHETIC_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "trade_recon"
    / "synthetic"
)
sys.path.insert(0, str(SYNTHETIC_PATH))

from generator import (
    default_scenario_config,
    generate_base_trade,
    generate_dataset,
    generate_scenario,
)


def test_dataset_can_rotate_across_multiple_brokers(tmp_path):
    manifest = generate_dataset(
        {
            "seed": 42,
            "business_date": "2026-10-19",
            "scenario_ids": ["S-001"],
            "trade_count": 6,
            "broker_ids": [
                "BROKER_A",
                "BROKER_B",
                "BROKER_C",
            ],
            "output_path": tmp_path,
        }
    )

    assert manifest["broker_ids"] == [
        "BROKER_A",
        "BROKER_B",
        "BROKER_C",
    ]
    assert len(manifest["source_files"]) == 1

    batch = manifest["source_files"][0]
    assert set(batch["broker_files_by_broker"]) == {
        "BROKER_A",
        "BROKER_B",
        "BROKER_C",
    }

    broker_ids = set()
    for relative_path in batch["broker_files"]:
        with (tmp_path / relative_path).open(
            encoding="utf-8"
        ) as handle:
            broker_ids.update(
                row["broker_id"]
                for row in csv.DictReader(handle)
            )

    assert broker_ids == {
        "BROKER_A",
        "BROKER_B",
        "BROKER_C",
    }


def test_broker_mismatch_stays_a_mismatch_for_broker_b():
    trade = dict(
        generate_base_trade(
            1,
            date(2026, 10, 19),
            Random(7),
        )
    )
    trade["broker_id"] = "BROKER_B"

    oms, broker, expected = generate_scenario(
        trade,
        default_scenario_config("S-029"),
        Random(7),
    )

    assert oms[0]["broker_id"] == "BROKER_B"
    assert broker[0]["broker_id"] != "BROKER_B"
    assert expected["expected_break_types"] == [
        "BROKER_MISMATCH"
    ]
