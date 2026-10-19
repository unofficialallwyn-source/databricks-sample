from __future__ import annotations

import argparse
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from engine import evaluate_reconciliation
from rules import RESOLUTION_REASON_BY_BREAK


_BREAK_ACTIVE_SCHEMA = StructType(
    [
        StructField("candidate_id", StringType(), False),
        StructField("business_trade_id", StringType(), True),
        StructField("broker_id", StringType(), True),
        StructField("trade_date", DateType(), True),
        StructField("break_id", StringType(), False),
        StructField("break_type", StringType(), False),
        StructField("opened_at", TimestampType(), False),
        StructField("last_evaluated_at", TimestampType(), False),
        StructField("status", StringType(), False),
    ]
)


def _table_exists(spark: SparkSession, table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def _current_breaks(evaluated: DataFrame) -> DataFrame:
    return (
        evaluated.select(
            "candidate_id",
            "business_trade_id",
            "broker_id",
            "trade_date",
            "evaluated_at",
            F.explode("break_types").alias("break_type"),
        )
        .withColumnRenamed("evaluated_at", "last_evaluated_at")
    )


def _new_break_id() -> F.Column:
    return F.sha2(
        F.concat_ws(
            "||",
            F.col("candidate_id"),
            F.col("break_type"),
            F.col("opened_at").cast("string"),
        ),
        256,
    )


def _resolution_reason() -> F.Column:
    expression = None
    for break_type, reason in RESOLUTION_REASON_BY_BREAK.items():
        condition = F.col("break_type") == break_type
        if expression is None:
            expression = F.when(condition, F.lit(reason))
        else:
            expression = expression.when(condition, F.lit(reason))
    return expression.otherwise(F.lit("SOURCE_STATE_CHANGED"))


def update_break_lifecycle(
    spark: SparkSession,
    evaluated: DataFrame,
    *,
    active_table: str,
    history_table: str,
) -> None:
    current = _current_breaks(evaluated)

    if _table_exists(spark, active_table):
        previous = spark.table(active_table)
    else:
        previous = spark.createDataFrame([], _BREAK_ACTIVE_SCHEMA)

    key = ["candidate_id", "break_type"]

    newly_open = (
        current.alias("c")
        .join(previous.select(*key).alias("p"), key, "left_anti")
        .withColumn("opened_at", F.col("last_evaluated_at"))
        .withColumn("break_id", _new_break_id())
        .withColumn("status", F.lit("OPEN"))
        .select(*_BREAK_ACTIVE_SCHEMA.fieldNames())
    )

    still_open = (
        current.alias("c")
        .join(
            previous.select(
                "candidate_id",
                "break_type",
                "break_id",
                "opened_at",
            ).alias("p"),
            key,
            "inner",
        )
        .select(
            F.col("c.candidate_id").alias("candidate_id"),
            F.col("c.business_trade_id").alias("business_trade_id"),
            F.col("c.broker_id").alias("broker_id"),
            F.col("c.trade_date").alias("trade_date"),
            F.col("p.break_id").alias("break_id"),
            F.col("c.break_type").alias("break_type"),
            F.col("p.opened_at").alias("opened_at"),
            F.col("c.last_evaluated_at").alias("last_evaluated_at"),
            F.lit("OPEN").alias("status"),
        )
    )

    resolved = previous.alias("p").join(
        current.select(*key).alias("c"),
        key,
        "left_anti",
    )

    resolved_events = resolved.select(
        "candidate_id",
        "business_trade_id",
        "broker_id",
        "trade_date",
        "break_id",
        "break_type",
        "opened_at",
        F.current_timestamp().alias("event_at"),
        F.lit("RESOLVED").alias("event_type"),
        F.lit("AUTO").alias("resolution_type"),
        _resolution_reason().alias("resolution_reason"),
    )

    opened_events = newly_open.select(
        "candidate_id",
        "business_trade_id",
        "broker_id",
        "trade_date",
        "break_id",
        "break_type",
        "opened_at",
        F.col("opened_at").alias("event_at"),
        F.lit("OPENED").alias("event_type"),
        F.lit(None).cast("string").alias("resolution_type"),
        F.lit(None).cast("string").alias("resolution_reason"),
    )

    next_active = newly_open.unionByName(still_open)

    (
        next_active.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(active_table)
    )

    lifecycle_events = opened_events.unionByName(resolved_events)
    if not lifecycle_events.isEmpty():
        (
            lifecycle_events.write.format("delta")
            .mode("append")
            .saveAsTable(history_table)
        )


def update_reconciliation_state(
    spark: SparkSession,
    evaluated: DataFrame,
    *,
    current_table: str,
    history_table: str,
) -> None:
    if _table_exists(spark, current_table):
        previous = spark.table(current_table).select(
            "candidate_id",
            "state_hash",
        )
        changed = (
            evaluated.alias("n")
            .join(previous.alias("p"), "candidate_id", "left")
            .filter(
                F.col("p.state_hash").isNull()
                | (F.col("n.state_hash") != F.col("p.state_hash"))
            )
            .select("n.*")
        )
    else:
        changed = evaluated

    if not changed.isEmpty():
        (
            changed.withColumn("recorded_at", F.current_timestamp())
            .write.format("delta")
            .mode("append")
            .saveAsTable(history_table)
        )

    (
        evaluated.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(current_table)
    )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-table", required=True)
    parser.add_argument("--current-table", required=True)
    parser.add_argument("--history-table", required=True)
    parser.add_argument("--break-active-table", required=True)
    parser.add_argument("--break-history-table", required=True)
    parser.add_argument("--price-tolerance", default="0.01")
    parser.add_argument("--sla-minutes", type=int, default=30)
    parser.add_argument("--evaluation-time")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    evaluated = evaluate_reconciliation(
        spark.table(args.candidate_table),
        price_tolerance=Decimal(args.price_tolerance),
        missing_side_sla_minutes=args.sla_minutes,
        evaluation_time=args.evaluation_time,
    )

    update_break_lifecycle(
        spark,
        evaluated,
        active_table=args.break_active_table,
        history_table=args.break_history_table,
    )
    update_reconciliation_state(
        spark,
        evaluated,
        current_table=args.current_table,
        history_table=args.history_table,
    )


if __name__ == "__main__":
    main()
