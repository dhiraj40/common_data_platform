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

source_table_name = 'material'
source_table = f"{catalog_name}.foundation.{source_table_name}"
target_table = f"{catalog_name}.trusted.{source_table_name}"

print(f"source_table: {source_table}")
print(f"target_table: {target_table}")

# COMMAND ----------

foundation_df = spark.table(source_table)

# display(foundation_df)

# COMMAND ----------

valid_material_df = (
    foundation_df
    .filter(
        F.col("material_id").isNotNull()
        & (F.col("material_id") != "")
        & F.col("material_name").isNotNull()
        & (F.col("material_name") != "")
    )
)

trusted_material_df = (
    valid_material_df
    .select(
        "material_id",
        "material_name",
        "material_type",
        "unit_of_measure",
        "product_group",
        "source_system",
        "record_created_timestamp",
        "record_updated_timestamp",
    )
)

# COMMAND ----------

merge_upsert(
    spark=spark,
    source_df=trusted_material_df,
    target_table=target_table,
    keys=[
        "source_system",
        "material_id",
    ],
)

# COMMAND ----------

