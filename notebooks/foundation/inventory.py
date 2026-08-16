# Databricks notebook source
from pathlib import Path
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

project_root = Path.cwd().parent.parent

sys.path.insert(0, str(project_root / "src"),)

# COMMAND ----------

from common_data_platform.setup.config import load_config

from common_data_platform.ingestion.ingestion_utils import (
    build_checkpoint_path,
)

from common_data_platform.foundation.transformations import (
    trim_columns,
    uppercase_columns,
    normalize_null_values,
    deduplicate_latest,
)

from common_data_platform.foundation.merge import (
    merge_upsert, merge_insert_only
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.dropdown("environment", "dev", ["dev", "uat", "prod"])
dbutils.widgets.dropdown("debug", "false", ["false", "true"])

environment = dbutils.widgets.get("environment").strip().lower()
debug = dbutils.widgets.get("debug").strip().lower() == "true"

print(f"Environment: {environment}")
print(f"Debug: {debug}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve environment

# COMMAND ----------

config = load_config(environment=environment)

source_table_name = 'inventory'
catelog_name = config['catalog_name']
source_table = f"{catelog_name}.raw.{source_table_name}"

target_table = f"{catelog_name}.foundation.{source_table_name}"

checkpoint_root = build_checkpoint_path(
    catalog_name=catelog_name,
    layer='foundation',
    dataset_name=source_table_name
)

checkpoint_path = f"{checkpoint_root}/checkpoint"

print(f"Source table: {source_table}")
print(f"Target table: {target_table}")
print(f"Checkpoint path: {checkpoint_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Load

# COMMAND ----------

raw_df = spark.table(source_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trim & Normalize columns

# COMMAND ----------

standardized_df = (
    raw_df
    .withColumn(
        "material_id",
        F.upper(F.trim("material_id")),
    )
    .withColumn(
        "plant_id",
        F.upper(F.trim("plant_id")),
    )
    .withColumn(
        "storage_location",
        F.upper(F.trim("storage_location")),
    )
    .withColumn(
        "unit_of_measure",
        F.upper(F.trim("unit_of_measure")),
    )
    .withColumn(
        "source_system",
        F.upper(F.trim("source_system")),
    )
)

# display(standardized_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### GET NOT NULL IDs and DEDUPLICATE keeping latest record

# COMMAND ----------

valid_df = (
    standardized_df
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

# display(valid_df)

# COMMAND ----------

inventory_window = (
    Window
    .partitionBy(
        "source_system",
        "inventory_snapshot_date",
        "material_id",
        "plant_id",
        "storage_location",
    )
    .orderBy(
        F.col("ingestion_timestamp").desc(),
    )
)

latest_df = (
    valid_df
    .withColumn(
        "_row_number",
        F.row_number().over(inventory_window),
    )
    .filter(
        F.col("_row_number") == 1
    )
    .drop("_row_number")
)

# display(latest_df)

# COMMAND ----------

latest_df = (
    latest_df
    .withColumn(
        "source_record_id",
        F.concat_ws(
            "|",
            F.col("inventory_snapshot_date").cast("string"),
            F.col("material_id"),
            F.col("plant_id"),
            F.coalesce(
                F.col("storage_location"),
                F.lit(""),
            ),
        ),
    )
)

foundation_inventory_df = (
    latest_df
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
        "source_record_id",
        F.current_timestamp().alias(
            "record_created_timestamp"
        ),

        F.current_timestamp().alias(
            "record_updated_timestamp"
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SAVE DATA

# COMMAND ----------

merge_upsert(
    spark=spark,
    source_df=foundation_inventory_df,
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

