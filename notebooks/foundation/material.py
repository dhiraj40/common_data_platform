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

catelog_name = config['catalog_name']
source_table = f"{catelog_name}.raw.material"

target_table = f"{catelog_name}.foundation.material"

checkpoint_root = build_checkpoint_path(
    catalog_name=catelog_name,
    layer='foundation',
    dataset_name="material"
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

material_df = raw_df.withColumn('source_record_id', F.col('material_id'))

material_df = trim_columns(material_df,
    [
        "material_id",
        "material_name",
        "material_type",
        "unit_of_measure",
        "product_group",
        "source_system",
    ],
)

material_df = normalize_null_values(
    material_df,
    [
        "material_id",
        "material_name",
        "material_type",
        "unit_of_measure",
        "product_group",
    ],
)

material_df = uppercase_columns(
    material_df,
    [
        "material_id",
        "material_type",
        "unit_of_measure",
        "source_system",
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### GET NOT NULL IDs and DEDUPLICATE keeping latest record

# COMMAND ----------

valid_material_df = material_df.filter(
    F.col('material_id').isNotNull() & F.col('source_system').isNotNull()
)

deduplicated_material_df = deduplicate_latest(
    valid_material_df,
    keys=[
        "source_system",
        "material_id",
    ],
    order_column="ingestion_timestamp",
)

# COMMAND ----------

foundation_material_df = (
    deduplicated_material_df
    .select(
        "material_id",
        "material_name",
        "material_type",
        "unit_of_measure",
        "product_group",
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
    source_df=foundation_material_df,
    target_table=target_table,
    keys=[
        "source_system",
        "material_id",
    ],
    exclude_update_columns=[
        "record_created_timestamp",
    ],
)

# COMMAND ----------

