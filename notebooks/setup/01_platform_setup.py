# Databricks notebook source
from pathlib import Path
import sys

project_root = Path.cwd().parent.parent
src_path = project_root / "src"

sys.path.insert(0, str(src_path))

# COMMAND ----------

from pyspark.sql import SparkSession

from common_data_platform.setup.config import load_config
from common_data_platform.setup.catalog import create_catalog
from common_data_platform.setup.schemas import create_schemas
from common_data_platform.setup.volumes import create_volumes
from common_data_platform.setup.tables import create_tables_from_directory, load_table_definitions
from common_data_platform.setup.validation import validate_platform_setup

# COMMAND ----------

spark = SparkSession.getActiveSession()

# COMMAND ----------

dbutils.widgets.dropdown("environment", "dev", ["dev", "uat", "prod"])
environment = dbutils.widgets.get("environment")

# COMMAND ----------

config = load_config(environment)

catalog_name = config["catalog_name"]
schemas = config["schemas"]
volumes = config["volumes"]

project_root = Path.cwd().parent.parent
tables_root = project_root / "resources" / "tables"

print(f"catalog_name = {catalog_name}")
print(f"schemas = {schemas}")
print(f"volumes = {volumes}")
print(f"tables_root = {tables_root}")

# COMMAND ----------

# 1. Catalog
create_catalog(
    spark=spark,
    catalog_name=catalog_name,
)

# COMMAND ----------

# 2. Schemas
create_schemas(
    spark=spark,
    catalog_name=catalog_name,
    schemas=schemas,
)

# COMMAND ----------

# 3. Volumes
create_volumes(
    spark=spark,
    catalog_name=catalog_name,
    volumes=volumes,
)

# COMMAND ----------

# 4. Create Raw, Foundation and Trusted tables
table_definitions = []

for layer in ["raw", "foundation", "trusted"]:

    table_directory = tables_root / layer

    create_tables_from_directory(
        spark=spark,
        catalog_name=catalog_name,
        directory=table_directory,
    )

    table_definitions.extend(
        load_table_definitions(table_directory)
    )

# COMMAND ----------

# 5. Validation
validate_platform_setup(
    spark=spark,
    catalog_name=catalog_name,
    schemas=schemas,
    volumes=volumes,
    table_definitions=table_definitions,
)

# COMMAND ----------

print(
    f"Common Data Platform setup completed successfully "
    f"for environment: {environment}"
)

# COMMAND ----------

