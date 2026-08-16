# Databricks notebook source
from pathlib import Path
import sys

from pyspark.sql import functions as F

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
    merge_upsert,
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

source_table_name = 'plant'
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

raw_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trim & Normalize columns

# COMMAND ----------

standardized_df = raw_df.withColumn('source_record_id', F.col('plant_id'))

standardized_df = trim_columns(standardized_df,
    [
        'plant_id',
        'plant_name',
        'plant_type',
        'city',
        'state',
        'country',
        'source_system',
    ],
)

standardized_df = normalize_null_values(
    standardized_df,
    [
        'plant_id',
        'plant_name',
        'plant_type',
        'city',
        'state',
        'country',
        'source_system',
    ],
)

standardized_df = uppercase_columns(
    standardized_df,
    [
        "plant_id",
        "plant_type",
        "country",
        "source_system",
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### GET NOT NULL IDs and DEDUPLICATE keeping latest record

# COMMAND ----------

valid_records_df = standardized_df.filter(
    F.col('plant_id').isNotNull() & F.col('source_system').isNotNull()
)

latest_df = deduplicate_latest(
    valid_records_df,
    keys=[
        "source_system",
        "plant_id",
    ],
    order_column="ingestion_timestamp",
)

# COMMAND ----------

foundation_plant_df = (
    latest_df
    .select(
        "plant_id",
        "plant_name",
        "plant_type",
        "city",
        "state",
        "country",
        "source_system",
        "source_record_id",
        F.col("ingestion_timestamp").alias(
            "source_ingestion_timestamp"
        ),

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
    source_df=foundation_plant_df,
    target_table=target_table,
    keys=[
        "source_system",
        "plant_id",
    ],
    exclude_update_columns=[
        "record_created_timestamp",
    ],
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from common_data_platform_dev.raw.plant

# COMMAND ----------

