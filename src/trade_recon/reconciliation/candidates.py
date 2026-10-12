import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_reconciliation_candidates(
    trade_current: DataFrame,
) -> DataFrame:

    internal = (
        trade_current
        .filter(F.col("record_role") == "INTERNAL")
        .select(
            F.col("business_trade_id")
                .alias("internal_business_trade_id"),

            F.col("canonical_record_id")
                .alias("internal_record_id"),

            F.col("source_system")
                .alias("internal_source_system"),

            F.col("source_trade_id")
                .alias("internal_source_trade_id"),

            F.col("source_version")
                .alias("internal_source_version"),

            F.col("event_type")
                .alias("internal_event_type"),

            F.col("event_time")
                        .alias("internal_event_time"),

            F.col("instrument_id")
                .alias("internal_instrument_id"),

            F.col("instrument_type")
                .alias("internal_instrument_type"),

            F.col("side")
                .alias("internal_side"),

            F.col("quantity")
                .alias("internal_quantity"),
            F.col("price")
                .alias("internal_price"),
            F.col("currency")
                .alias("internal_currency"),

            F.col("account_id")
                .alias("internal_account_id"),

            F.col("trade_date")
                .alias("internal_trade_date"),
            F.col("settlement_date")
                .alias("internal_settlement_date"),
            
            F.col("broker_id").alias("internal_broker_id"),
            F.col("venue_id").alias("internal_venue_id"),
            F.col("execution_timestamp")
                .alias("internal_execution_timestamp")
        )
    )

    external = (
        trade_current
        .filter(F.col("record_role") == "EXTERNAL")
        .select(
            F.col("business_trade_id")
                .alias("external_business_trade_id"),

            F.col("canonical_record_id")
                .alias("external_record_id"),

            F.col("source_system")
                .alias("external_source_system"),

            F.col("source_trade_id")
                .alias("external_source_trade_id"),

            F.col("source_version")
                .alias("external_source_version"),

            F.col("event_type")
                .alias("external_event_type"),

            F.col("event_time")
                        .alias("external_event_time"),

            F.col("instrument_id")
                .alias("external_instrument_id"),
            F.col("instrument_type")
                .alias("external_instrument_type"),
            F.col("side")
                .alias("external_side"),

            F.col("quantity")
                .alias("external_quantity"),
            F.col("price")
                .alias("external_price"),
            F.col("currency")
                .alias("external_currency"),

            F.col("account_id")
                .alias("external_account_id"),

            F.col("trade_date")
                .alias("external_trade_date"),
            F.col("settlement_date")
                .alias("external_settlement_date"),
            F.col("broker_id").alias("external_broker_id"),
            F.col("venue_id").alias("external_venue_id"),
            F.col("execution_timestamp")
                .alias("external_execution_timestamp"),
        )
    )

    joined = (
        internal.alias("i")
        .join(
            external.alias("e"),
            F.col("i.internal_business_trade_id")
            == F.col("e.external_business_trade_id"),
            "full_outer",
        )
    )

    candidates = joined.select(
        F.coalesce(
            F.col("i.internal_business_trade_id"),
            F.col("e.external_business_trade_id"),
        ).alias("business_trade_id"),

        F.col("i.internal_record_id"),
        F.col("e.external_record_id"),

        F.col("i.internal_source_system"),
        F.col("i.internal_source_trade_id"),
        F.col("i.internal_source_version"),
        F.col("i.internal_event_type"),
        F.col("i.internal_event_time"),
        F.col("i.internal_instrument_id"),
        F.col("i.internal_instrument_type"),
        F.col("i.internal_side"),
        F.col("i.internal_quantity"),
        F.col("i.internal_price"),
        F.col("i.internal_currency"),
        F.col("i.internal_account_id"),
        F.col("i.internal_trade_date"),
        F.col("i.internal_settlement_date"),
        F.col("i.internal_execution_timestamp"),
        F.col("i.internal_broker_id"),
        F.col("i.internal_venue_id"),
        

        F.col("e.external_source_system"),
        F.col("e.external_source_trade_id"),
        F.col("e.external_source_version"),
        F.col("e.external_event_type"),
        F.col("e.external_event_time"),
        F.col("e.external_instrument_id"),
        F.col("e.external_instrument_type"),
        F.col("e.external_side"),
        F.col("e.external_quantity"),
        F.col("e.external_price"),
        F.col("e.external_currency"),
        F.col("e.external_account_id"),
        F.col("e.external_trade_date"),
        F.col("e.external_settlement_date"),
        F.col("e.external_execution_timestamp"),
        F.col("e.external_broker_id"),
        F.col("e.external_venue_id")
    )

    candidates = (
        candidates
        .withColumn(
            "internal_present",
            F.col("internal_record_id").isNotNull(),
        )
        .withColumn(
            "external_present",
            F.col("external_record_id").isNotNull(),
        )
    )

    candidates = (
            candidates
            .withColumn(
                "candidate_presence",
                F.when(
                    F.col("internal_present") & F.col("external_present"),
                    "BOTH_SIDES"
                ).when(
                    F.col("internal_present"),
                    "INTERNAL_ONLY"
                ).otherwise(
                    "EXTERNAL_ONLY"
                )
            )
        )

    return candidates

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--source-table",
        required=True,
    )

    parser.add_argument(
        "--target-table",
        required=True,
    )

    return parser.parse_args()


def main():

    args = parse_args()

    spark = SparkSession.builder.getOrCreate()

    trade_current = spark.table(
        args.source_table
    )

    candidates = build_reconciliation_candidates(
        trade_current
    )

    (
        candidates.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(args.target_table)
    )


if __name__ == "__main__":
    main()