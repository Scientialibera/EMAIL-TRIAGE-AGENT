# Fabric Notebook -- utils_module
# Shared analytics helpers for email triage enrichment and aggregation.

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def deduplicate(df: DataFrame, partition_cols: list, order_col: str = "processed_at") -> DataFrame:
    """Keep latest record per partition key."""
    from pyspark.sql.window import Window

    w = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def safe_timestamp_parse(col_name: str) -> F.Column:
    return F.to_timestamp(F.col(col_name))


def response_time_minutes(received_col: str, sent_col: str) -> F.Column:
    """Calculate response time in minutes between received and sent timestamps."""
    return (
        F.unix_timestamp(F.col(sent_col)) - F.unix_timestamp(F.col(received_col))
    ) / 60.0
