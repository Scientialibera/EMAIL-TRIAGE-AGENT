# Fabric Notebook -- 01_ingest_main
# Reads email classification records from Cosmos DB into the Landing lakehouse.

# %run ../modules/config_module

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

cosmos_config = {
    "spark.cosmos.accountEndpoint": COSMOS_ENDPOINT,
    "spark.cosmos.database": COSMOS_DATABASE,
    "spark.cosmos.container": COSMOS_CONTAINER,
    "spark.cosmos.read.inferSchema.enabled": "true",
    "spark.cosmos.accountKey": "",
}

raw_df = (
    spark.read
    .format("cosmos.oltp")
    .options(**cosmos_config)
    .load()
)

expected_cols = [
    "id", "mailbox", "message_id", "conversation_id", "subject",
    "from_address", "from_name", "received_at", "has_attachments",
    "classification", "urgency", "topic", "sentiment", "confidence",
    "reasoning", "feedback", "feedback_at", "response_sent_at", "processed_at",
]
for col in expected_cols:
    if col not in raw_df.columns:
        raw_df = raw_df.withColumn(col, F.lit(None).cast("string"))

raw_df = raw_df.select(*expected_cols)
raw_df = raw_df.withColumn("_ingested_at", F.current_timestamp())

landing_path = get_table_path(LANDING_LAKEHOUSE, RAW_TABLE)
raw_df.write.format("delta").mode("overwrite").save(landing_path)

print(f"Ingested {raw_df.count()} records into {RAW_TABLE}.")
