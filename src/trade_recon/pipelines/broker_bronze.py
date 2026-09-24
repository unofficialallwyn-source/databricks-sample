from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

BROKER_SOURCE_PATH = spark.conf.get(
    "trade_recon.broker_source_path"
)

broker_schema = StructType([
    StructField("confirmation_event_id", StringType(), False),
    StructField("broker_trade_id", StringType(), False),
    StructField("client_trade_id", StringType(), True),
    StructField("confirmation_version", IntegerType(), False),
    StructField("confirmation_type", StringType(), False),
    StructField("confirmation_time", StringType(), False),

    StructField("schema_version", StringType(), True),
    StructField("instrument_code", StringType(), True),
    StructField("instrument_type", StringType(), True),
    StructField("side", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("price", StringType(), True),
    StructField("currency", StringType(), True),

    StructField("client_account", StringType(), True),
    StructField("broker_id", StringType(), True),
    StructField("venue", StringType(), True),

    StructField("trade_date", StringType(), True),
    StructField("execution_timestamp", StringType(), True),
    StructField("settlement_date", StringType(), True),
    StructField("published_at", StringType(), True),
])

broker_bronze_schema = StructType(
    list(broker_schema.fields) + [
        StructField("_rescued_data", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)

@dp.table(
    name="broker_confirmation_raw_lakeflow",
    comment="Raw Broker events ingested with Auto Loader"
)
@dp.expect(
    "no_rescued_data",
    "_rescued_data IS NULL"
)
@dp.expect(
    "no_corrupt_records",
    "_corrupt_record IS NULL"
)
def broker_confirmation_raw_lakeflow():
    return (
        spark.readStream 
        .format("cloudFiles") 
        .option("cloudFiles.format", "csv") 
        .option("header", "true") 
        .option("cloudFiles.schemaEvolutionMode", "rescue") 
        .option("rescuedDataColumn", "_rescued_data") 
        .option("columnNameOfCorruptRecord", "_corrupt_record") 
        .option("pathGlobFilter", "*.csv") 
        .schema(broker_bronze_schema) 
        .load(BROKER_SOURCE_PATH) 
        .select(
            "*",
            col("_metadata.file_path").alias("_source_file_path"),
            col("_metadata.file_name").alias("_source_file_name"),
            col("_metadata.file_modification_time")
                .alias("_source_file_modification_time"),
            current_timestamp().alias("_ingested_at"),
        )
    )