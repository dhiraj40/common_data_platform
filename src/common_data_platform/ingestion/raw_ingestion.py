from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common_data_platform.ingestion.file_ingestion import (
    read_file_stream,
)
from common_data_platform.ingestion.ingestion_utils import (
    build_checkpoint_path,
    build_table_name,
    build_volume_path,
    generate_batch_id,
)


def add_ingestion_metadata(
    df: DataFrame,
    source_system: str,
    batch_id: str,
) -> DataFrame:
    """Add standard Raw ingestion metadata."""

    return (
        df
        .withColumn(
            "source_file",
            F.col("_metadata.file_path"),
        )
        .withColumn(
            "source_system",
            F.lit(source_system),
        )
        .withColumn(
            "ingestion_batch_id",
            F.lit(batch_id),
        )
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp(),
        )
    )


def align_to_target_schema(
    spark: SparkSession,
    df: DataFrame,
    target_table: str,
) -> DataFrame:
    """Align incoming data to the existing Raw table schema."""

    target_schema = spark.table(target_table).schema
    source_columns = set(df.columns)

    expressions = []

    for field in target_schema.fields:

        if field.name in source_columns:
            expressions.append(
                F.col(field.name)
                .cast(field.dataType)
                .alias(field.name)
            )
        else:
            expressions.append(
                F.lit(None)
                .cast(field.dataType)
                .alias(field.name)
            )

    return df.select(*expressions)


def ingest_raw(
    spark: SparkSession,
    catalog_name: str,
    ingestion_config: dict,
    continuous: bool = False,
):
    """
    Execute Raw ingestion using Auto Loader.

    continuous=False:
        process all currently available new files and stop.

    continuous=True:
        keep the streaming query running.
    """

    dataset_name = ingestion_config["name"]

    source_config = ingestion_config["source"]
    target_config = ingestion_config["target"]
    metadata_config = ingestion_config.get(
        "metadata",
        {},
    )

    if source_config["type"] != "file":
        raise ValueError(
            f"Unsupported source type: "
            f"{source_config['type']}"
        )

    source_path = build_volume_path(
        catalog_name=catalog_name,
        schema_name="raw",
        volume_name="source_files",
        relative_path=source_config["path"],
    )

    checkpoint_root = build_checkpoint_path(
        catalog_name=catalog_name,
        layer="raw",
        dataset_name=dataset_name,
    )

    checkpoint_path = (
        f"{checkpoint_root}/checkpoint"
    )

    schema_path = (
        f"{checkpoint_root}/schema"
    )

    target_table = build_table_name(
        catalog_name=catalog_name,
        schema_name=target_config["schema"],
        table_name=target_config["table"],
    )

    batch_id = generate_batch_id()

    df = read_file_stream(
        spark=spark,
        source_path=source_path,
        schema_path=schema_path,
        source_config=source_config,
    )

    df = add_ingestion_metadata(
        df=df,
        source_system=metadata_config["source_system"],
        batch_id=batch_id,
    )

    df = align_to_target_schema(
        spark=spark,
        df=df,
        target_table=target_table,
    )

    writer = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            checkpoint_path,
        )
    )

    if not continuous:
        writer = writer.trigger(
            availableNow=True
        )

    query = writer.toTable(
        target_table
    )

    if not continuous:
        query.awaitTermination()

    return query