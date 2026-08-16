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

source_table_name = 'inventory'
source_table = f"{catalog_name}.foundation.{source_table_name}"
target_table = f"{catalog_name}.trusted.{source_table_name}"

print(f"source_table: {source_table}")
print(f"target_table: {target_table}")

# COMMAND ----------

foundation_df = spark.table(source_table)

trusted_material_df = spark.table(
    f"{catalog_name}.trusted.material"
)

trusted_plant_df = spark.table(
    f"{catalog_name}.trusted.plant"
)

# COMMAND ----------

valid_inventory_df = (
    foundation_df
    .filter(
        F.col("inventory_snapshot_date").isNotNull()
        & F.col("material_id").isNotNull()
        & (F.col("material_id") != "")
        & F.col("plant_id").isNotNull()
        & (F.col("plant_id") != "")
        & F.col("source_system").isNotNull()
        & (F.col("source_system") != "")
    )
)


material_ref_df = (
    trusted_material_df
    .select(
        "source_system",
        "material_id",
    )
    .dropDuplicates()
)

material_ref_df = (
    trusted_material_df
    .select(
        "source_system",
        "material_id",
    )
    .dropDuplicates()
)

material_valid_inventory_df = (
    valid_inventory_df
    .join(
        material_ref_df,
        on=[
            "source_system",
            "material_id",
        ],
        how="left_semi",
    )
)

plant_ref_df = (
    trusted_plant_df
    .select(
        "source_system",
        "plant_id",
    )
    .dropDuplicates()
)

trusted_valid_inventory_df = (
    material_valid_inventory_df
    .join(
        plant_ref_df,
        on=[
            "source_system",
            "plant_id",
        ],
        how="left_semi",
    )
)

# COMMAND ----------

trusted_inventory_df = (
    trusted_valid_inventory_df
    .select(
        "inventory_snapshot_date",
        "material_id",
        "plant_id",
        "storage_location",
        "available_quantity",
        "blocked_quantity",
        "in_transit_quantity",
        "unit_of_measure",
        "source_system",
        "record_created_timestamp",
        "record_updated_timestamp",
    )
)

# COMMAND ----------

merge_upsert(
    spark=spark,
    source_df=trusted_inventory_df,
    target_table=target_table,
    keys=[
        "source_system",
        "inventory_snapshot_date",
        "material_id",
        "plant_id",
        "storage_location",
    ],
)

# COMMAND ----------

