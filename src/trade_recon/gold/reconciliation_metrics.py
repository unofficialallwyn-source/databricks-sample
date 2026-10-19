import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_reconciliation_summary(
    reconciliation_current: DataFrame,
) -> DataFrame:
    return (
        reconciliation_current.groupBy("trade_date", "broker_id")
        .agg(
            F.count(F.lit(1)).alias("trade_count"),
            F.sum(
                F.when(
                    F.col("reconciliation_status") == "MATCHED",
                    1,
                ).otherwise(0)
            ).alias("matched_count"),
            F.sum(
                F.when(
                    F.col("reconciliation_status") == "BREAK",
                    1,
                ).otherwise(0)
            ).alias("break_count"),
            F.sum(
                F.when(
                    F.col("reconciliation_status") == "PENDING",
                    1,
                ).otherwise(0)
            ).alias("pending_count"),
        )
        .withColumn(
            "match_rate_pct",
            F.when(
                F.col("trade_count") > 0,
                F.round(
                    F.col("matched_count")
                    * F.lit(100.0)
                    / F.col("trade_count"),
                    2,
                ),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("refreshed_at", F.current_timestamp())
    )


def build_break_summary(
    reconciliation_current: DataFrame,
) -> DataFrame:
    return (
        reconciliation_current.select(
            "trade_date",
            "broker_id",
            F.explode("break_types").alias("break_type"),
        )
        .groupBy("trade_date", "broker_id", "break_type")
        .agg(F.count(F.lit(1)).alias("break_count"))
        .withColumn("refreshed_at", F.current_timestamp())
    )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--summary-table", required=True)
    parser.add_argument("--break-summary-table", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    reconciliation_current = spark.table(args.source_table)

    summary = build_reconciliation_summary(reconciliation_current)
    break_summary = build_break_summary(reconciliation_current)

    (
        summary.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(args.summary_table)
    )
    (
        break_summary.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(args.break_summary_table)
    )


if __name__ == "__main__":
    main()
