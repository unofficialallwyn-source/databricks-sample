import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


_SIDE_COLUMNS = (
    "canonical_record_id",
    "source_system",
    "source_trade_id",
    "source_version",
    "event_type",
    "event_time",
    "instrument_id",
    "instrument_type",
    "side",
    "quantity",
    "price",
    "currency",
    "account_id",
    "trade_date",
    "settlement_date",
    "broker_id",
    "venue_id",
    "execution_timestamp",
    "ingestion_timestamp",
)


def _project_side(trade_current: DataFrame, role: str, prefix: str) -> DataFrame:
    projected = trade_current.filter(F.col("record_role") == role).select(
        F.col("business_trade_id").alias(f"{prefix}_business_trade_id"),
        *[F.col(name).alias(f"{prefix}_{name}") for name in _SIDE_COLUMNS],
    )

    business_id = F.col(f"{prefix}_business_trade_id")
    record_id = F.col(f"{prefix}_canonical_record_id")

    return projected.withColumn(
        "candidate_key",
        F.when(
            business_id.isNull(),
            F.concat(F.lit(f"{prefix.upper()}::"), record_id),
        ).otherwise(business_id),
    )


def _candidate_counts(df: DataFrame, count_column: str) -> DataFrame:
    return df.groupBy("candidate_key").agg(F.count(F.lit(1)).alias(count_column))


def _unique_rows(df: DataFrame, count_column: str) -> DataFrame:
    counts = _candidate_counts(df, count_column)
    return (
        df.join(counts, "candidate_key")
        .filter(F.col(count_column) == 1)
        .drop(count_column)
    )


def build_reconciliation_candidates(trade_current: DataFrame) -> DataFrame:
    """Build one reconciliation candidate row per business match key."""
    internal = _project_side(trade_current, "INTERNAL", "internal")
    external = _project_side(trade_current, "EXTERNAL", "external")

    internal_counts = _candidate_counts(internal, "internal_candidate_count")
    external_counts = _candidate_counts(external, "external_candidate_count")

    candidate_keys = (
        internal_counts.select("candidate_key")
        .unionByName(external_counts.select("candidate_key"))
        .distinct()
    )

    summary = (
        candidate_keys.join(internal_counts, "candidate_key", "left")
        .join(external_counts, "candidate_key", "left")
        .fillna(0, subset=["internal_candidate_count", "external_candidate_count"])
    )

    internal_unique = _unique_rows(internal, "internal_candidate_count")
    external_unique = _unique_rows(external, "external_candidate_count")

    candidates = (
        summary.join(internal_unique, "candidate_key", "left")
        .join(external_unique, "candidate_key", "left")
        .withColumn(
            "business_trade_id",
            F.coalesce(
                F.col("internal_business_trade_id"),
                F.col("external_business_trade_id"),
            ),
        )
        .withColumn("candidate_id", F.sha2(F.col("candidate_key"), 256))
        .withColumn(
            "internal_present",
            F.col("internal_candidate_count") > 0,
        )
        .withColumn(
            "external_present",
            F.col("external_candidate_count") > 0,
        )
        .withColumn(
            "match_key_missing",
            (F.col("external_candidate_count") > 0)
            & F.col("external_business_trade_id").isNull(),
        )
        .withColumn(
            "candidate_presence",
            F.when(
                (F.col("internal_candidate_count") > 0)
                & (F.col("external_candidate_count") > 0),
                F.lit("BOTH_SIDES"),
            )
            .when(
                F.col("internal_candidate_count") > 0,
                F.lit("INTERNAL_ONLY"),
            )
            .otherwise(F.lit("EXTERNAL_ONLY")),
        )
    )

    ordered_columns = [
        "candidate_id",
        "candidate_key",
        "business_trade_id",
        "candidate_presence",
        "internal_candidate_count",
        "external_candidate_count",
        "internal_present",
        "external_present",
        "match_key_missing",
    ]

    side_columns = [
        column
        for column in candidates.columns
        if column.startswith("internal_") or column.startswith("external_")
    ]
    side_columns = [column for column in side_columns if column not in ordered_columns]

    return candidates.select(*ordered_columns, *side_columns)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-table", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    candidates = build_reconciliation_candidates(spark.table(args.source_table))

    (
        candidates.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(args.target_table)
    )


if __name__ == "__main__":
    main()
