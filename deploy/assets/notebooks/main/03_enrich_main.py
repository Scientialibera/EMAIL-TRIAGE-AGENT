# Fabric Notebook -- 03_enrich_main
# Bronze -> Silver: response time, classification accuracy, temporal features.

# %run ../modules/config_module
# %run ../modules/utils_module

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

bronze_path = get_table_path(BRONZE_LAKEHOUSE, CLEAN_TABLE)
clean_df = spark.read.format("delta").load(bronze_path)

enriched_df = clean_df

# Response time for replied emails
enriched_df = enriched_df.withColumn(
    "response_time_minutes",
    F.when(
        F.col("response_sent_at").isNotNull() & F.col("received_at").isNotNull(),
        response_time_minutes("received_at", "response_sent_at"),
    ).otherwise(F.lit(None).cast("double"))
)

# Classification correctness derived from feedback
# approved/edited = classification was useful; rejected = possibly wrong
enriched_df = enriched_df.withColumn(
    "classification_correct",
    F.when(F.col("feedback").isin("approved", "edited"), F.lit(True))
    .when(F.col("feedback") == "rejected", F.lit(False))
    .otherwise(F.lit(None).cast("boolean"))
)

# Temporal features
enriched_df = (
    enriched_df
    .withColumn("received_date", F.to_date(F.col("received_at")))
    .withColumn("day_of_week", F.dayofweek(F.col("received_at")))
    .withColumn("hour_of_day", F.hour(F.col("received_at")))
    .withColumn("week_number", F.weekofyear(F.col("received_at")))
    .withColumn("month", F.month(F.col("received_at")))
    .withColumn("year", F.year(F.col("received_at")))
)

# Topic normalization
enriched_df = enriched_df.withColumn("topic_lower", F.lower(F.trim(F.col("topic"))))

enriched_df = enriched_df.withColumn("_enriched_at", F.current_timestamp())

silver_path = get_table_path(SILVER_LAKEHOUSE, ENRICHED_TABLE)
enriched_df.write.format("delta").mode("overwrite").save(silver_path)

print(f"Enriched {enriched_df.count()} records into {ENRICHED_TABLE}.")
