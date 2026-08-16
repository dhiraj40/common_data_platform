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

source_table_name = 'shipment'
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
        "source_record_id",
        F.col("shipment_id"),
    )
    .withColumn(
        "shipment_id",
        F.upper(F.trim("shipment_id")),
    )
    .withColumn(
        "purchase_order_id",
        F.upper(F.trim("purchase_order_id")),
    )
    .withColumn(
        "purchase_order_line",
        F.trim("purchase_order_line"),
    )
    .withColumn(
        "material_id",
        F.upper(F.trim("material_id")),
    )
    .withColumn(
        "supplier_id",
        F.upper(F.trim("supplier_id")),
    )
    .withColumn(
        "plant_id",
        F.upper(F.trim("plant_id")),
    )
    .withColumn(
        "shipment_status",
        F.upper(F.trim("shipment_status")),
    )
    .withColumn(
        "carrier_name",
        F.trim("carrier_name"),
    )
    .withColumn(
        "transport_mode",
        F.upper(F.trim("transport_mode")),
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
        F.col("shipment_id").isNotNull()
        & (F.col("shipment_id") != "")
        & F.col("material_id").isNotNull()
        & (F.col("material_id") != "")
        & F.col("supplier_id").isNotNull()
        & (F.col("supplier_id") != "")
        & F.col("plant_id").isNotNull()
        & (F.col("plant_id") != "")
        & F.col("source_system").isNotNull()
        & (F.col("source_system") != "")
    )
)

# display(valid_df)

# COMMAND ----------

shipment_window = (
    Window
    .partitionBy(
        "source_system",
        "shipment_id",
    )
    .orderBy(
        F.col("ingestion_timestamp").desc(),
    )
)

latest_df = (
    valid_df
    .withColumn(
        "_row_number",
        F.row_number().over(shipment_window),
    )
    .filter(
        F.col("_row_number") == 1
    )
    .drop("_row_number")
)

# display(latest_df)

# COMMAND ----------

foundation_shipment_df = (
    latest_df
    .select(
        "shipment_id",
        "purchase_order_id",
        "purchase_order_line",
        "material_id",
        "supplier_id",
        "plant_id",
        "shipment_date",
        "actual_delivery_date",
        "shipped_quantity",
        "received_quantity",
        "shipment_status",
        "carrier_name",
        "transport_mode",
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
    source_df=foundation_shipment_df,
    target_table=target_table,
    keys=[
        "source_system",
        "shipment_id",
    ],
)

# COMMAND ----------

