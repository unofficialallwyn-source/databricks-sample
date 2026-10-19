from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
import sys

from pyspark.sql import functions as F

RECONCILIATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "trade_recon"
    / "reconciliation"
)
sys.path.insert(0, str(RECONCILIATION_PATH))

from engine import evaluate_reconciliation


EVALUATION_TIME = "2026-10-19 12:00:00"
OBSERVED_TIME = datetime(2026, 10, 19, 11, 50, tzinfo=timezone.utc)


def _base_candidate(
    candidate_id="candidate-1",
    business_trade_id="TRD00000001",
    broker_id="BROKER_A",
):
    return {
        "candidate_id": candidate_id,
        "candidate_key": business_trade_id,
        "business_trade_id": business_trade_id,
        "candidate_presence": "BOTH_SIDES",
        "internal_candidate_count": 1,
        "external_candidate_count": 1,
        "internal_present": True,
        "external_present": True,
        "match_key_missing": False,
        "internal_source_version": 1,
        "external_source_version": 1,
        "internal_event_type": "NEW",
        "external_event_type": "CONFIRM",
        "internal_event_time": OBSERVED_TIME,
        "external_event_time": OBSERVED_TIME,
        "internal_ingestion_timestamp": OBSERVED_TIME,
        "external_ingestion_timestamp": OBSERVED_TIME,
        "internal_instrument_id": "AAPL",
        "external_instrument_id": "AAPL",
        "internal_side": "BUY",
        "external_side": "BUY",
        "internal_quantity": Decimal("100.000000"),
        "external_quantity": Decimal("100.000000"),
        "internal_price": Decimal("150.0000000000"),
        "external_price": Decimal("150.0000000000"),
        "internal_currency": "USD",
        "external_currency": "USD",
        "internal_broker_id": broker_id,
        "external_broker_id": broker_id,
        "internal_account_id": "ACC000001",
        "external_account_id": "ACC000001",
        "internal_trade_date": date(2026, 10, 19),
        "external_trade_date": date(2026, 10, 19),
        "internal_settlement_date": date(2026, 10, 21),
        "external_settlement_date": date(2026, 10, 21),
    }


def _evaluate(spark, rows, **kwargs):
    return evaluate_reconciliation(
        spark.createDataFrame(rows),
        evaluation_time=kwargs.pop("evaluation_time", EVALUATION_TIME),
        **kwargs,
    )


def _result_by_id(df):
    return {
        row.candidate_id: row.asDict(recursive=True)
        for row in df.collect()
    }


def test_exact_match_and_price_tolerance(spark):
    exact = _base_candidate("exact")
    within = _base_candidate("within", "TRD00000002")
    within["external_price"] = Decimal("150.0100000000")

    results = _result_by_id(_evaluate(spark, [exact, within]))

    assert results["exact"]["reconciliation_status"] == "MATCHED"
    assert results["exact"]["break_types"] == []
    assert results["within"]["reconciliation_status"] == "MATCHED"


def test_multi_field_break_retains_each_reason(spark):
    row = _base_candidate()
    row["external_price"] = Decimal("150.0200000000")
    row["external_quantity"] = Decimal("110.000000")

    result = _evaluate(spark, [row]).first()

    assert result.reconciliation_status == "BREAK"
    assert result.break_types == [
        "PRICE_MISMATCH",
        "QUANTITY_MISMATCH",
    ]


def test_broker_and_account_mismatches(spark):
    row = _base_candidate()
    row["external_broker_id"] = "BROKER_B"
    row["external_account_id"] = "ACC999999"

    result = _evaluate(spark, [row]).first()

    assert result.break_types == [
        "ACCOUNT_MISMATCH",
        "BROKER_MISMATCH",
    ]


def test_missing_confirmation_is_pending_until_sla(spark):
    base = spark.createDataFrame([_base_candidate()])
    internal_only = (
        base.withColumn("candidate_presence", F.lit("INTERNAL_ONLY"))
        .withColumn("external_candidate_count", F.lit(0))
        .withColumn("external_present", F.lit(False))
    )

    before = evaluate_reconciliation(
        internal_only,
        missing_side_sla_minutes=30,
        evaluation_time="2026-10-19 12:10:00",
    ).first()
    after = evaluate_reconciliation(
        internal_only,
        missing_side_sla_minutes=30,
        evaluation_time="2026-10-19 12:25:00",
    ).first()

    assert before.reconciliation_status == "PENDING"
    assert before.pending_reason == "AWAITING_CONFIRMATION"
    assert after.reconciliation_status == "BREAK"
    assert after.break_types == ["MISSING_CONFIRMATION"]


def test_oms_amendment_does_not_reset_missing_confirmation_sla(spark):
    base = spark.createDataFrame([_base_candidate()])
    amended_internal_only = (
        base.withColumn("candidate_presence", F.lit("INTERNAL_ONLY"))
        .withColumn("external_candidate_count", F.lit(0))
        .withColumn("external_present", F.lit(False))
        .withColumn("internal_source_version", F.lit(2))
        .withColumn(
            "internal_ingestion_timestamp",
            F.lit("2026-10-19 11:40:00").cast("timestamp"),
        )
        .withColumn(
            "previous_first_observed_at",
            F.lit("2026-10-19 11:00:00").cast("timestamp"),
        )
    )

    result = evaluate_reconciliation(
        amended_internal_only,
        missing_side_sla_minutes=30,
        evaluation_time="2026-10-19 11:40:00",
    ).first()

    assert result.first_observed_at == datetime(2026, 10, 19, 11, 0)
    assert result.reconciliation_status == "BREAK"
    assert result.break_types == ["MISSING_CONFIRMATION"]


def test_missing_trade_and_match_key_missing_are_distinct(spark):
    base = spark.createDataFrame([_base_candidate()])
    external_only = (
        base.withColumn("candidate_presence", F.lit("EXTERNAL_ONLY"))
        .withColumn("internal_candidate_count", F.lit(0))
        .withColumn("internal_present", F.lit(False))
        .withColumn(
            "internal_ingestion_timestamp",
            F.lit(None).cast("timestamp"),
        )
        .withColumn(
            "external_ingestion_timestamp",
            F.lit("2026-10-19 11:00:00").cast("timestamp"),
        )
    )

    missing_trade = evaluate_reconciliation(
        external_only,
        evaluation_time=EVALUATION_TIME,
    ).first()
    missing_key = evaluate_reconciliation(
        external_only.withColumn("match_key_missing", F.lit(True)),
        evaluation_time=EVALUATION_TIME,
    ).first()

    assert missing_trade.break_types == ["MISSING_TRADE"]
    assert missing_key.break_types == ["MATCH_KEY_MISSING"]


def test_duplicate_candidates_are_not_arbitrarily_matched(spark):
    base = spark.createDataFrame([_base_candidate()])

    internal_duplicate = evaluate_reconciliation(
        base.withColumn("internal_candidate_count", F.lit(2)),
        evaluation_time=EVALUATION_TIME,
    ).first()
    external_duplicate = evaluate_reconciliation(
        base.withColumn("external_candidate_count", F.lit(2)),
        evaluation_time=EVALUATION_TIME,
    ).first()

    assert internal_duplicate.break_types == [
        "DUPLICATE_INTERNAL_CANDIDATE"
    ]
    assert external_duplicate.break_types == [
        "DUPLICATE_EXTERNAL_CANDIDATE"
    ]


def test_cancellation_rules(spark):
    base = spark.createDataFrame([_base_candidate()])

    oms_cancelled = evaluate_reconciliation(
        base.withColumn("internal_event_type", F.lit("CANCEL")),
        evaluation_time=EVALUATION_TIME,
    ).first()
    both_cancelled = evaluate_reconciliation(
        base.withColumn("internal_event_type", F.lit("CANCEL"))
        .withColumn("external_event_type", F.lit("CANCEL")),
        evaluation_time=EVALUATION_TIME,
    ).first()

    assert oms_cancelled.break_types == [
        "OMS_CANCEL_BROKER_ACTIVE"
    ]
    assert both_cancelled.reconciliation_status == "MATCHED"


def test_multiple_brokers_are_evaluated_independently(spark):
    broker_a = _base_candidate(
        "broker-a",
        "TRD00000011",
        "BROKER_A",
    )
    broker_b = _base_candidate(
        "broker-b",
        "TRD00000012",
        "BROKER_B",
    )

    results = _result_by_id(
        _evaluate(spark, [broker_a, broker_b])
    )

    assert results["broker-a"]["broker_id"] == "BROKER_A"
    assert results["broker-b"]["broker_id"] == "BROKER_B"
    assert {
        row["reconciliation_status"]
        for row in results.values()
    } == {"MATCHED"}
