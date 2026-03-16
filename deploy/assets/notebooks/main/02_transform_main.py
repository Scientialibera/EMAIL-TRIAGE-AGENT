# Fabric Notebook -- 02_transform_main
# Landing -> Bronze: normalize schema, type casting, dedup.

# %run ../modules/config_module
# %run ../modules/utils_module

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

landing_path = get_table_path(LANDING_LAKEHOUSE, RAW_TABLE)
raw_df = spark.read.format("delta").load(landing_path)

clean_df = (
    raw_df
    .withColumn("received_at", safe_timestamp_parse("received_at"))
    .withColumn("processed_at", safe_timestamp_parse("processed_at"))
    .withColumn("feedback_at", safe_timestamp_parse("feedback_at"))
    .withColumn("response_sent_at", safe_timestamp_parse("response_sent_at"))
    .withColumn("has_attachments", F.coalesce(F.col("has_attachments").cast("boolean"), F.lit(False)))
    .withColumn("confidence", F.coalesce(F.col("confidence").cast("double"), F.lit(0.0)))
    .withColumn("mailbox", F.lower(F.trim(F.col("mailbox"))))
    .withColumn("from_address", F.lower(F.trim(F.col("from_address"))))
    .withColumn("classification", F.lower(F.trim(F.col("classification"))))
    .withColumn("urgency", F.lower(F.trim(F.col("urgency"))))
    .withColumn("sentiment", F.lower(F.trim(F.col("sentiment"))))
    .withColumn("feedback", F.lower(F.trim(F.coalesce(F.col("feedback"), F.lit("")))))
)

clean_df = deduplicate(clean_df, partition_cols=["mailbox", "message_id"], order_col="processed_at")
clean_df = clean_df.withColumn("_transformed_at", F.current_timestamp())

bronze_path = get_table_path(BRONZE_LAKEHOUSE, CLEAN_TABLE)
clean_df.write.format("delta").mode("overwrite").save(bronze_path)

print(f"Transformed {clean_df.count()} records into {CLEAN_TABLE}.")
