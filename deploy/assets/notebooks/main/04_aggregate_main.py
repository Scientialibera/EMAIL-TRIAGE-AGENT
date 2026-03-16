# Fabric Notebook -- 04_aggregate_main
# Silver -> Gold: inbox metrics, classification accuracy, SLA compliance, topic trends.

# %run ../modules/config_module
# %run ../modules/utils_module

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

silver_path = get_table_path(SILVER_LAKEHOUSE, ENRICHED_TABLE)
enriched_df = spark.read.format("delta").load(silver_path)

# ---- inbox_metrics: daily counts by classification per mailbox ----
inbox_metrics = (
    enriched_df
    .groupBy("mailbox", "received_date", "classification")
    .agg(
        F.count("*").alias("email_count"),
        F.avg("confidence").alias("avg_confidence"),
        F.countDistinct("from_address").alias("unique_senders"),
    )
    .withColumn("_aggregated_at", F.current_timestamp())
)

metrics_path = get_table_path(GOLD_LAKEHOUSE, GOLD_TABLES["inbox_metrics"])
inbox_metrics.write.format("delta").mode("overwrite").save(metrics_path)
print(f"  inbox_metrics: {inbox_metrics.count()} rows")

# ---- classification_accuracy: feedback-based accuracy per mailbox ----
with_feedback = enriched_df.filter(F.col("classification_correct").isNotNull())
accuracy = (
    with_feedback
    .groupBy("mailbox", "classification")
    .agg(
        F.count("*").alias("total_with_feedback"),
        F.sum(F.when(F.col("classification_correct"), 1).otherwise(0)).alias("correct_count"),
        F.avg("confidence").alias("avg_confidence"),
    )
    .withColumn("accuracy_rate", F.col("correct_count") / F.col("total_with_feedback"))
    .withColumn("_aggregated_at", F.current_timestamp())
)

accuracy_path = get_table_path(GOLD_LAKEHOUSE, GOLD_TABLES["classification_accuracy"])
accuracy.write.format("delta").mode("overwrite").save(accuracy_path)
print(f"  classification_accuracy: {accuracy.count()} rows")

# ---- response_time_sla: % within target by urgency ----
SLA_TARGETS = {"critical": 60, "high": 240, "medium": 1440, "low": 4320}

needs_reply = enriched_df.filter(
    (F.col("classification") == "needs_reply") & F.col("response_time_minutes").isNotNull()
)

sla_rows = []
for urgency_level, target_minutes in SLA_TARGETS.items():
    subset = needs_reply.filter(F.col("urgency") == urgency_level)
    if subset.count() > 0:
        sla_rows.append(
            subset.agg(
                F.lit(urgency_level).alias("urgency"),
                F.count("*").alias("total"),
                F.sum(F.when(F.col("response_time_minutes") <= target_minutes, 1).otherwise(0)).alias("within_sla"),
                F.avg("response_time_minutes").alias("avg_response_minutes"),
                F.lit(target_minutes).alias("sla_target_minutes"),
            )
        )

if sla_rows:
    from functools import reduce
    sla_df = reduce(lambda a, b: a.union(b), sla_rows)
    sla_df = (
        sla_df
        .withColumn("sla_compliance_rate", F.col("within_sla") / F.col("total"))
        .withColumn("_aggregated_at", F.current_timestamp())
    )
    sla_path = get_table_path(GOLD_LAKEHOUSE, GOLD_TABLES["response_time_sla"])
    sla_df.write.format("delta").mode("overwrite").save(sla_path)
    print(f"  response_time_sla: {sla_df.count()} rows")
else:
    print("  response_time_sla: no data")

# ---- topic_trends: volume over time by topic ----
topic_trends = (
    enriched_df
    .filter(F.col("topic_lower").isNotNull() & (F.length(F.col("topic_lower")) > 0))
    .groupBy("mailbox", "topic_lower", "year", "week_number")
    .agg(
        F.count("*").alias("email_count"),
        F.avg("confidence").alias("avg_confidence"),
        F.sum(F.when(F.col("classification") == "urgent", 1).otherwise(0)).alias("urgent_count"),
    )
    .withColumn("_aggregated_at", F.current_timestamp())
)

trends_path = get_table_path(GOLD_LAKEHOUSE, GOLD_TABLES["topic_trends"])
topic_trends.write.format("delta").mode("overwrite").save(trends_path)
print(f"  topic_trends: {topic_trends.count()} rows")

print("Gold aggregation complete.")
