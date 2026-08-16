from pyspark.sql import DataFrame, SparkSession


SUPPORTED_FORMATS = {
    "csv",
    "json",
    "parquet",
}


def read_file_stream(
    spark: SparkSession,
    source_path: str,
    schema_path: str,
    source_config: dict,
) -> DataFrame:
    """
    Read files incrementally using Databricks Auto Loader.
    """

    file_format = source_config["format"].lower()

    if file_format not in SUPPORTED_FORMATS:
        raise ValueError(
            f"Unsupported file format: {file_format}. "
            f"Expected one of: {SUPPORTED_FORMATS}"
        )

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option(
            "cloudFiles.format",
            file_format,
        )
        .option(
            "cloudFiles.schemaLocation",
            schema_path,
        )
    )

    if file_format == "csv":

        reader = (
            reader
            .option(
                "header",
                source_config.get("header", True),
            )
            .option(
                "delimiter",
                source_config.get("delimiter", ","),
            )
        )

    return reader.load(source_path)