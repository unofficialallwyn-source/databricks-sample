from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

OMS_SOURCE_PATH = spark.conf.get(
    "trade_recon.oms_source_path"
)

oms_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("trade_id", StringType(), False),
    StructField("trade_version", IntegerType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("schema_version", StringType(), False),

    StructField("instrument_id", StringType(), True),
    StructField("instrument_type", StringType(), True),
    StructField("side", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("price", StringType(), True),
    StructField("currency", StringType(), True),

    StructField("account_id", StringType(), True),
    StructField("portfolio_id", StringType(), True),
    StructField("broker_id", StringType(), True),
    StructField("venue_id", StringType(), True),

    StructField("trade_date", StringType(), True),
    StructField("execution_timestamp", StringType(), True),
    StructField("settlement_date", StringType(), True),
    StructField("published_at", StringType(), True),
])

oms_bronze_schema = StructType(
    list(oms_schema.fields) + [
        StructField("_rescued_data", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)

@dp.table(
    name="oms_trade_event_raw_lakeflow",
    comment="Raw OMS events ingested with Auto Loader"
)
@dp.expect(
    "no_rescued_data",
    "_rescued_data IS NULL"
)
@dp.expect(
    "no_corrupt_records",
    "_corrupt_record IS NULL"
)
def oms_trade_event_raw_lakeflow():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("rescuedDataColumn", "_rescued_data")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("pathGlobFilter", "*.jsonl")
            .schema(oms_bronze_schema)
            .load(OMS_SOURCE_PATH)
            .select(
                "*",
                col("_metadata.file_path").alias("_source_file_path"),
                col("_metadata.file_name").alias("_source_file_name"),
                col("_metadata.file_modification_time")
                    .alias("_source_file_modification_time"),
                current_timestamp().alias("_ingested_at"),
            )
    )