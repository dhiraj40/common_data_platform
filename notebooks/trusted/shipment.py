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

source_table_name = 'shipment'
source_table = f"{catalog_name}.foundation.{source_table_name}"
target_table = f"{catalog_name}.trusted.{source_table_name}"

print(f"source_table: {source_table}")
print(f"target_table: {target_table}")

# COMMAND ----------

foundation_df = spark.table(source_table)

trusted_material_df = spark.table(
    f"{catalog_name}.trusted.material"
)

trusted_supplier_df = spark.table(
    f"{catalog_name}.trusted.supplier"
)

trusted_plant_df = spark.table(
    f"{catalog_name}.trusted.plant"
)

trusted_po_df = spark.table(
    f"{catalog_name}.trusted.purchase_order"
)

# COMMAND ----------

valid_shipment_df = (
    foundation_df
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


material_ref_df = (
    trusted_material_df
    .select(
        "source_system",
        "material_id",
    )
    .dropDuplicates()
)

mmaterial_ref_df = (
    trusted_material_df
    .select(
        "source_system",
        "material_id",
    )
    .dropDuplicates()
)

material_valid_df = (
    valid_shipment_df
    .join(
        material_ref_df,
        on=[
            "source_system",
            "material_id",
        ],
        how="left_semi",
    )
)

supplier_ref_df = (
    trusted_supplier_df
    .select(
        "source_system",
        "supplier_id",
    )
    .dropDuplicates()
)

supplier_valid_df = (
    material_valid_df
    .join(
        supplier_ref_df,
        on=[
            "source_system",
            "supplier_id",
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

plant_valid_df = (
    supplier_valid_df
    .join(
        plant_ref_df,
        on=[
            "source_system",
            "plant_id",
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

po_ref_df = (
    trusted_po_df
    .select(
        "source_system",
        "purchase_order_id",
        "purchase_order_line",
    )
    .dropDuplicates()
    .withColumn(
        "_valid_po",
        F.lit(True),
    )
)

po_checked_df = (
    plant_valid_df
    .join(
        po_ref_df,
        on=[
            "source_system",
            "purchase_order_id",
            "purchase_order_line",
        ],
        how="left",
    )
)

# COMMAND ----------

trusted_valid_shipment_df = (
    po_checked_df
    .filter(
        (
            F.col("purchase_order_id").isNull()
            & F.col("purchase_order_line").isNull()
        )
        |
        F.col("_valid_po").isNotNull()
    )
    .drop("_valid_po")
)

# COMMAND ----------

trusted_shipment_df = (
    trusted_valid_shipment_df
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
        "record_created_timestamp",
        "record_updated_timestamp",
    )
)

# COMMAND ----------

merge_upsert(
    spark=spark,
    source_df=trusted_shipment_df,
    target_table=target_table,
    keys=[
        "source_system",
        "shipment_id",
    ],
)

# COMMAND ----------

