# Fabric Notebook -- config_module
# Shared configuration and lakehouse helpers for the Email Triage analytics pipeline.

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


def get_lakehouse_path(lakehouse: str, zone: str = "Tables") -> str:
    return f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{zone}"


def get_table_path(lakehouse: str, table: str) -> str:
    return f"{get_lakehouse_path(lakehouse)}/{table}"
