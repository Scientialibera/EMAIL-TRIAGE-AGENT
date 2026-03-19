# Fabric Notebook -- config_module
# Shared configuration and lakehouse helpers for the Email Triage analytics pipeline.

import os


def _get_workspace_id() -> str:
    try:
        return spark.conf.get("trident.workspace.id")
    except Exception:
        pass
    return os.environ.get("WORKSPACE_ID", os.environ.get("fabric_workspace_id", ""))


LANDING_LAKEHOUSE = "lh_email_landing"
BRONZE_LAKEHOUSE = "lh_email_bronze"
SILVER_LAKEHOUSE = "lh_email_silver"
GOLD_LAKEHOUSE = "lh_email_gold"

COSMOS_ENDPOINT = ""
COSMOS_DATABASE = "email-triage"
COSMOS_CONTAINER = "emails"

RAW_TABLE = "raw_emails"
CLEAN_TABLE = "emails_clean"
ENRICHED_TABLE = "emails_enriched"

GOLD_TABLES = {
    "inbox_metrics": "inbox_metrics",
    "classification_accuracy": "classification_accuracy",
    "response_time_sla": "response_time_sla",
    "topic_trends": "topic_trends",
}


def get_lakehouse_path(lakehouse_id: str, zone: str = "Tables") -> str:
    ws = _get_workspace_id()
    return f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/{zone}"


def get_table_path(lakehouse_id: str, table: str) -> str:
    return f"{get_lakehouse_path(lakehouse_id)}/{table}"


def read_lakehouse_table(spark_session, lakehouse_id: str, table_name: str):
    path = get_table_path(lakehouse_id, table_name)
    return spark_session.read.format("delta").load(path)


def write_lakehouse_table(df, lakehouse_id: str, table_name: str, mode: str = "overwrite"):
    """Write as managed Delta table via saveAsTable for proper catalog registration."""
    try:
        if mode == "overwrite":
            spark.sql(f"DROP TABLE IF EXISTS `{table_name}`")
        df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(table_name)
    except Exception as e:
        print(f"  [warn] saveAsTable failed for '{table_name}', falling back to path write: {e}")
        path = get_table_path(lakehouse_id, table_name)
        df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(path)
