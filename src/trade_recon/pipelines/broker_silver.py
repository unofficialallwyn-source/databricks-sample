from pyspark import pipelines as dp
from pyspark.sql import functions as F

BROKER_BRONZE_TABLE = spark.conf.get(
    "trade_recon.broker_bronze_table"
)

@dp.table(
    name="broker_confirmation_validated",
    comment="Validated and strongly typed Broker trade events",
)
def broker_confirmation_validated():

    bronze_data = spark.readStream.table(BROKER_BRONZE_TABLE)

    valid_data = bronze_data.filter(
        F.col("_rescued_data").isNull()
        & F.col("_corrupt_record").isNull()
    )

    typed_data = valid_data.select(
        # Source identity
        F.col("confirmation_event_id")
            .cast("string")
            .alias("source_event_id"),

        F.lit("BROKER_A")
            .alias("source_system"),

        F.lit("EXTERNAL_TRADE")
            .alias("record_role"),

        F.col("broker_trade_id")
            .cast("string")
            .alias("source_trade_id"),

        F.col("client_trade_id")
            .cast("string")
            .alias("business_trade_id"),            

        F.col("confirmation_version")
            .cast("long")
            .alias("source_version"),

        F.col("confirmation_type")
            .cast("string")
            .alias("source_event_type"),

        F.col("confirmation_time")
            .try_cast("timestamp")
            .alias("source_event_time"),

        F.col("schema_version"),

        # Instrument
        F.col("instrument_code").alias("instrument_id"),
        F.col("instrument_type"),
        F.col("side"),

        # Economics
        F.col("quantity")
        .try_cast("decimal(18,6)")
        .alias("quantity"),

        F.col("price")
        .try_cast("decimal(18,10)")
        .alias("price"),
        
        F.col("currency"),

        # Account / broker
        F.col("client_account").alias("account_id"),
        F.col("broker_id"),
        F.col("venue").alias("venue_id"),

        # Dates/timestamps
        F.col("trade_date")
            .try_cast("date")
            .alias("trade_date"),

        F.col("execution_timestamp")
            .try_cast("timestamp")
            .alias("execution_timestamp"),

        F.col("settlement_date")
            .try_cast("date")
            .alias("settlement_date"),

        F.col("published_at")
            .try_cast("timestamp")
            .alias("published_at"),

        # Bronze lineage
        F.col("_source_file_path"),
        F.col("_source_file_name"),
        F.col("_source_file_modification_time"),
        F.col("_ingested_at"),
    )

    required_identity = (
        F.col("source_event_id").isNotNull()
        & F.col("source_trade_id").isNotNull()
        & F.col("source_version").isNotNull()
        & F.col("source_event_type").isNotNull()
        & F.col("source_event_time").isNotNull()
    )

    economics_valid = (
        (F.col("source_event_type") == "CANCEL")
        |
        (
            F.col("quantity").isNotNull()
            & F.col("price").isNotNull()
        )
    )

    validated_data = typed_data.filter(
        required_identity & economics_valid
    )
    
    return validated_data
  
dp.create_streaming_table(
    name="broker_trade_state_history"
)
dp.create_auto_cdc_flow(
    target="broker_trade_state_history",
    source="broker_confirmation_validated",
    keys=[
        "record_role",
        "source_system",
        "source_trade_id",
    ],
    sequence_by=F.col("source_version"),
    stored_as_scd_type=2,
)