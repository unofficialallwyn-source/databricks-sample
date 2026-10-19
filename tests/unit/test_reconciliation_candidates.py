from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
import sys

RECONCILIATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "trade_recon"
    / "reconciliation"
)
sys.path.insert(0, str(RECONCILIATION_PATH))

from candidates import build_reconciliation_candidates


NOW = datetime(2026, 10, 19, 10, 0, tzinfo=timezone.utc)


def _trade(
    role,
    business_id,
    record_id,
    source_trade_id,
    broker_id,
):
    return {
        "record_role": role,
        "business_trade_id": business_id,
        "canonical_record_id": record_id,
        "source_system": "OMS" if role == "INTERNAL" else broker_id,
        "source_trade_id": source_trade_id,
        "source_version": 1,
        "event_type": "NEW" if role == "INTERNAL" else "CONFIRM",
        "event_time": NOW,
        "instrument_id": "AAPL",
        "instrument_type": "EQUITY",
        "side": "BUY",
        "quantity": Decimal("100.000000"),
        "price": Decimal("150.0000000000"),
        "currency": "USD",
        "account_id": "ACC000001",
        "trade_date": date(2026, 10, 19),
        "settlement_date": date(2026, 10, 21),
        "broker_id": broker_id,
        "venue_id": "XNAS",
        "execution_timestamp": NOW,
        "ingestion_timestamp": NOW,
    }


def test_candidates_support_multiple_brokers_without_cross_pairing(spark):
    rows = [
        _trade("INTERNAL", "TRD1", "i1", "OMS1", "BROKER_A"),
        _trade("EXTERNAL", "TRD1", "e1", "BA1", "BROKER_A"),
        _trade("INTERNAL", "TRD2", "i2", "OMS2", "BROKER_B"),
        _trade("EXTERNAL", "TRD2", "e2", "BB1", "BROKER_B"),
    ]

    results = {
        row.business_trade_id: row.asDict(recursive=True)
        for row in build_reconciliation_candidates(
            spark.createDataFrame(rows)
        ).collect()
    }

    assert set(results) == {"TRD1", "TRD2"}
    assert results["TRD1"]["candidate_presence"] == "BOTH_SIDES"
    assert results["TRD1"]["external_broker_id"] == "BROKER_A"
    assert results["TRD2"]["external_broker_id"] == "BROKER_B"


def test_duplicate_external_candidates_remain_explicit(spark):
    rows = [
        _trade("INTERNAL", "TRD1", "i1", "OMS1", "BROKER_A"),
        _trade("EXTERNAL", "TRD1", "e1", "BA1", "BROKER_A"),
        _trade("EXTERNAL", "TRD1", "e2", "BA2", "BROKER_A"),
    ]

    result = build_reconciliation_candidates(
        spark.createDataFrame(rows)
    ).first()

    assert result.internal_candidate_count == 1
    assert result.external_candidate_count == 2
    assert result.external_canonical_record_id is None
