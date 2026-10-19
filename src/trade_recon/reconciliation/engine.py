from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from rules import (
    DEFAULT_MISSING_SIDE_SLA_MINUTES,
    DEFAULT_PRICE_TOLERANCE,
    FIELD_RULES,
    REQUIRED_ACTIVE_FIELDS,
    validate_runtime_config,
)


def _timestamp_literal(value: datetime | str | None) -> Column:
    if value is None:
        return F.current_timestamp()
    if isinstance(value, datetime):
        return F.lit(value).cast("timestamp")
    return F.to_timestamp(F.lit(value))


def _all_present(*columns: str) -> Column:
    return reduce(
        lambda left, right: left & right,
        (F.col(name).isNotNull() for name in columns),
    )


def _any_missing(*columns: str) -> Column:
    return reduce(
        lambda left, right: left | right,
        (F.col(name).isNull() for name in columns),
    )


def evaluate_reconciliation(
    candidates: DataFrame,
    *,
    price_tolerance: Decimal = DEFAULT_PRICE_TOLERANCE,
    missing_side_sla_minutes: int = DEFAULT_MISSING_SIDE_SLA_MINUTES,
    evaluation_time: datetime | str | None = None,
) -> DataFrame:
    """Evaluate structural, lifecycle, economic and SLA reconciliation rules."""
    validate_runtime_config(price_tolerance, missing_side_sla_minutes)

    both_sides = F.col("candidate_presence") == "BOTH_SIDES"
    internal_only = F.col("candidate_presence") == "INTERNAL_ONLY"
    external_only = F.col("candidate_presence") == "EXTERNAL_ONLY"

    unique_internal = F.col("internal_candidate_count") == 1
    unique_external = F.col("external_candidate_count") == 1
    unique_both = (
        both_sides
        & unique_internal
        & unique_external
        & ~F.col("match_key_missing")
    )

    internal_cancelled = F.col("internal_event_type") == "CANCEL"
    external_cancelled = F.col("external_event_type") == "CANCEL"
    active_both = unique_both & ~internal_cancelled & ~external_cancelled

    previous_anchor = (
        F.col("previous_first_observed_at")
        if "previous_first_observed_at" in candidates.columns
        else F.lit(None).cast("timestamp")
    )
    observed_at = F.coalesce(
        previous_anchor,
        F.col("internal_ingestion_timestamp"),
        F.col("external_ingestion_timestamp"),
        F.col("internal_event_time"),
        F.col("external_event_time"),
    )

    evaluated = candidates.withColumn("evaluated_at", _timestamp_literal(evaluation_time))
    evaluated = evaluated.withColumn("first_observed_at", observed_at)
    if "previous_first_observed_at" in evaluated.columns:
        evaluated = evaluated.drop("previous_first_observed_at")
    evaluated = evaluated.withColumn(
        "sla_due_at",
        F.col("first_observed_at")
        + F.expr(f"INTERVAL {int(missing_side_sla_minutes)} MINUTES"),
    )

    break_expressions: list[Column] = [
        F.when(F.col("match_key_missing"), F.lit("MATCH_KEY_MISSING")),
        F.when(
            F.col("internal_candidate_count") > 1,
            F.lit("DUPLICATE_INTERNAL_CANDIDATE"),
        ),
        F.when(
            F.col("external_candidate_count") > 1,
            F.lit("DUPLICATE_EXTERNAL_CANDIDATE"),
        ),
        F.when(
            unique_both & internal_cancelled & ~external_cancelled,
            F.lit("OMS_CANCEL_BROKER_ACTIVE"),
        ),
        F.when(
            unique_both & ~internal_cancelled & external_cancelled,
            F.lit("BROKER_CANCEL_OMS_ACTIVE"),
        ),
    ]

    required_columns = []
    for field_name in REQUIRED_ACTIVE_FIELDS:
        required_columns.extend(
            (f"internal_{field_name}", f"external_{field_name}")
        )

    break_expressions.append(
        F.when(
            active_both & _any_missing(*required_columns),
            F.lit("REQUIRED_VALUE_MISSING"),
        )
    )

    for rule in FIELD_RULES:
        values_present = _all_present(rule.internal_column, rule.external_column)
        mismatch = F.col(rule.internal_column) != F.col(rule.external_column)
        break_expressions.append(
            F.when(
                active_both & values_present & mismatch,
                F.lit(rule.break_type),
            )
        )

    price_values_present = _all_present("internal_price", "external_price")
    price_mismatch = (
        F.abs(F.col("internal_price") - F.col("external_price"))
        > F.lit(str(price_tolerance)).cast("decimal(18,10)")
    )
    break_expressions.append(
        F.when(
            active_both & price_values_present & price_mismatch,
            F.lit("PRICE_MISMATCH"),
        )
    )

    sla_expired = (
        F.col("first_observed_at").isNotNull()
        & (F.col("evaluated_at") >= F.col("sla_due_at"))
    )

    break_expressions.extend(
        [
            F.when(
                internal_only & sla_expired,
                F.lit("MISSING_CONFIRMATION"),
            ),
            F.when(
                external_only
                & ~F.col("match_key_missing")
                & sla_expired,
                F.lit("MISSING_TRADE"),
            ),
        ]
    )

    evaluated = evaluated.withColumn(
        "break_types_raw",
        F.array(*break_expressions),
    )
    evaluated = evaluated.withColumn(
        "break_types",
        F.array_sort(
            F.array_distinct(
                F.expr("filter(break_types_raw, x -> x is not null)")
            )
        ),
    ).drop("break_types_raw")

    evaluated = evaluated.withColumn(
        "break_count",
        F.size(F.col("break_types")),
    )
    evaluated = evaluated.withColumn(
        "pending_reason",
        F.when(
            internal_only & (F.col("break_count") == 0),
            F.lit("AWAITING_CONFIRMATION"),
        ).when(
            external_only
            & ~F.col("match_key_missing")
            & (F.col("break_count") == 0),
            F.lit("AWAITING_INTERNAL_TRADE"),
        ),
    )

    evaluated = evaluated.withColumn(
        "reconciliation_status",
        F.when(F.col("break_count") > 0, F.lit("BREAK"))
        .when(internal_only | external_only, F.lit("PENDING"))
        .otherwise(F.lit("MATCHED")),
    )

    evaluated = (
        evaluated.withColumn(
            "broker_id",
            F.coalesce(
                F.col("internal_broker_id"),
                F.col("external_broker_id"),
            ),
        )
        .withColumn(
            "trade_date",
            F.coalesce(
                F.col("internal_trade_date"),
                F.col("external_trade_date"),
            ),
        )
    )

    state_text = F.concat_ws(
        "||",
        F.col("reconciliation_status"),
        F.coalesce(F.array_join(F.col("break_types"), ","), F.lit("")),
        F.coalesce(F.col("pending_reason"), F.lit("")),
        F.coalesce(
            F.col("internal_source_version").cast("string"),
            F.lit(""),
        ),
        F.coalesce(
            F.col("external_source_version").cast("string"),
            F.lit(""),
        ),
        F.coalesce(F.col("candidate_presence"), F.lit("")),
    )

    return evaluated.withColumn("state_hash", F.sha2(state_text, 256))
