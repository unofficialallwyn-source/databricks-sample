from pyspark import pipelines as dp
from pyspark.sql import functions as F


OMS_VALIDATED_TABLE = spark.conf.get(
    "trade_recon.oms_validated_table"
)

BROKER_VALIDATED_TABLE = spark.conf.get(
    "trade_recon.broker_validated_table"
)

def canonicalize_oms(df):

    canonical = df.select(
        F.lit("INTERNAL").alias("record_role"),

        F.col("source_system"),

        F.col("source_event_id")
            .alias("source_record_id"),

        F.col("source_trade_id"),

        F.col("source_trade_id")
            .alias("business_trade_id"),

        F.col("source_version"),

        F.col("source_event_type")
            .alias("event_type"),

        F.col("source_event_time")
            .alias("event_time"),

        F.col("schema_version"),

        F.col("instrument_id"),
        F.col("instrument_type"),
        F.col("side"),

        F.col("quantity"),
        F.col("price"),
        F.col("currency"),

        F.col("account_id"),
        F.col("portfolio_id"),
        F.col("broker_id"),
        F.col("venue_id"),

        F.col("trade_date"),
        F.col("execution_timestamp"),
        F.col("settlement_date"),
        F.col("published_at"),

        F.col("_source_file_path"),
        F.col("_source_file_name"),
        F.col("_source_file_modification_time"),

        F.col("_ingested_at")
            .alias("ingestion_timestamp"),
    )

    canonical = filter_valid_identity(canonical)

    canonical = add_canonical_record_id(canonical)

    return canonical

def canonicalize_broker(df):

    canonical = df.select(
        F.lit("EXTERNAL").alias("record_role"),

        F.col("source_system"),

        F.col("source_event_id")
            .alias("source_record_id"),

        F.col("source_trade_id"),

        F.col("business_trade_id"),

        F.col("source_version"),

        F.col("source_event_type")
            .alias("event_type"),

        F.col("source_event_time")
            .alias("event_time"),

        F.col("schema_version"),

        F.col("instrument_id"),
        F.col("instrument_type"),
        F.col("side"),

        F.col("quantity"),
        F.col("price"),
        F.col("currency"),

        F.col("account_id"),

        F.lit(None)
            .cast("string")
            .alias("portfolio_id"),

        F.col("broker_id"),
        F.col("venue_id"),

        F.col("trade_date"),
        F.col("execution_timestamp"),
        F.col("settlement_date"),
        F.col("published_at"),

        F.col("_source_file_path"),
        F.col("_source_file_name"),
        F.col("_source_file_modification_time"),

        F.col("_ingested_at")
            .alias("ingestion_timestamp"),
    )

    canonical = filter_valid_identity(canonical)

    canonical = add_canonical_record_id(canonical)

    return canonical

def filter_valid_identity(df):
    return df.filter(
        F.col("source_record_id").isNotNull()
        & F.col("source_trade_id").isNotNull()
        & F.col("source_version").isNotNull()
        & F.col("event_type").isNotNull()
        & F.col("event_time").isNotNull()
    )

def add_canonical_record_id(df):
    return df.withColumn(
        "canonical_record_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("source_system"),
                F.col("source_record_id"),
                F.col("source_version").cast("string"),
            ),
            256,
        ),
    )

@dp.table(
    name="trade_event_canonical",
    comment="Canonical normalized OMS and Broker trade events",
)
def trade_event_canonical():

    oms = spark.readStream.table(
        OMS_VALIDATED_TABLE
    )

    broker = spark.readStream.table(
        BROKER_VALIDATED_TABLE
    )

    canonical_oms = canonicalize_oms(oms)

    canonical_broker = canonicalize_broker(broker)

    return canonical_oms.unionByName(
        canonical_broker
    )

dp.create_streaming_table(
    name="trade_state_history"
)
dp.create_auto_cdc_flow(
    target="trade_state_history",
    source="trade_event_canonical",
    keys=[
        "record_role",
        "source_system",
        "source_trade_id",
    ],
    sequence_by=F.col("source_version"),
    stored_as_scd_type=2,
)

@dp.materialized_view(
    name="trade_current",
    comment="Current canonical state for each source-side trade",
)
def trade_current():
    return (
        spark.read
            .table("trade_state_history")
            .filter(
                F.col("__END_AT").isNull()
            )
    )