# Databricks notebook source
from pathlib import Path
import sys

from pyspark.sql import functions as F


project_root = Path.cwd().parent.parent

sys.path.insert(
    0,
    str(project_root / "src"),
)

from common_data_platform.setup.config import load_config
from common_data_platform.foundation.merge import merge_upsert

# COMMAND ----------

dbutils.widgets.text("environment","dev")

environment = (
    dbutils.widgets.get("environment")
    .strip()
    .lower()
)

config = load_config(environment)
catalog_name = config["catalog_name"]

source_table_name = 'supplier'
source_table = f"{catalog_name}.foundation.{source_table_name}"
target_table = f"{catalog_name}.trusted.{source_table_name}"

print(f"source_table: {source_table}")
print(f"target_table: {target_table}")

# COMMAND ----------

foundation_df = spark.table(source_table)

# display(foundation_df)

# COMMAND ----------

valid_supplier_df = (
    foundation_df
    .filter(
        F.col("supplier_id").isNotNull()
        & (F.col("supplier_id") != "")
        & F.col("supplier_name").isNotNull()
        & (F.col("supplier_name") != "")
        & F.col("source_system").isNotNull()
        & (F.col("source_system") != "")
    )
)

trusted_supplier_df = (
    valid_supplier_df
    .select(
        "supplier_id",
        "supplier_name",
        "supplier_type",
        "city",
        "state",
        "country",
        "source_system",
        "record_created_timestamp",
        "record_updated_timestamp",
    )
)

# COMMAND ----------

merge_upsert(
    spark=spark,
    source_df=trusted_supplier_df,
    target_table=target_table,
    keys=[
        "source_system",
        "supplier_id",
    ],
)