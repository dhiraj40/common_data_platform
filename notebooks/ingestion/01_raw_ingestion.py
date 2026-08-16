# Databricks notebook source
from pathlib import Path
import sys

from pyspark.sql import SparkSession

# COMMAND ----------

project_root = Path.cwd().parent.parent
src_path = project_root / "src"

sys.path.insert(0, str(src_path))

# COMMAND ----------

from common_data_platform.setup.config import load_config

from common_data_platform.ingestion.ingestion_utils import (
    load_ingestion_configs,
)

from common_data_platform.ingestion.raw_ingestion import (
    ingest_raw,
)

# COMMAND ----------

spark = SparkSession.getActiveSession()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.dropdown("environment", "dev", ["dev", "uat", "prod"])
dbutils.widgets.text("datasets","all")
dbutils.widgets.dropdown("continuous", "false", ["false", "true"])

environment = (
    dbutils.widgets.get("environment")
    .strip()
    .lower()
)

datasets_parameter = (
    dbutils.widgets.get("datasets")
    .strip()
    .lower()
)

continuous = (
    dbutils.widgets.get("continuous")
    .strip()
    .lower()
    == "true"
)

print(f"Environment: {environment}")
print(f"Datasets: {datasets_parameter}")
print(f"Continuous: {continuous}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load platform configuration

# COMMAND ----------

config = load_config(environment=environment)
catalog_name = config["catalog_name"]
catalog_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load ingestion configurations

# COMMAND ----------

ingestion_config_directory = project_root / "resources" / "ingestion"
ingestion_configs = load_ingestion_configs(directory=ingestion_config_directory)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select datasets

# COMMAND ----------

if datasets_parameter == "all":
    selected_configs = ingestion_configs
else:
    requested_datasets = {
        dataset.strip()
        for dataset in datasets_parameter.split(",")
        if dataset.strip()
    }

    selected_configs = [
        config
        for config in ingestion_configs
        if config["name"].lower() in requested_datasets
    ]

    available_datasets = {
        config["name"].lower()
        for config in ingestion_configs
    }

    unknown_datasets = (
        requested_datasets
        - available_datasets
    )

    if unknown_datasets:
        raise ValueError(
            f"Unknown datasets requested: "
            f"{sorted(unknown_datasets)}"
        )

if not selected_configs:
    raise ValueError(
        "No ingestion datasets selected."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute ingestion

# COMMAND ----------

queries = []
for ingestion_config in selected_configs:
    dataset_name = ingestion_config["name"]
    print(
        f"Starting Raw ingestion: "
        f"{dataset_name}"
    )
    query = ingest_raw(
        spark=spark,
        catalog_name=catalog_name,
        ingestion_config=ingestion_config,
        continuous=continuous,
    )

    queries.append(query)

    print(
        f"Started Raw ingestion: "
        f"{dataset_name}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuous mode

# COMMAND ----------

if continuous:
    for query in queries:
        query.awaitTermination()

# COMMAND ----------

print(
    f"Raw ingestion completed successfully "
    f"for environment: {environment}"
)