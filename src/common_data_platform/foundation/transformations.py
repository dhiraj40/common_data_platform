from collections.abc import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def validate_columns(
    df: DataFrame,
    columns: Iterable[str],
) -> None:
    """
    Validate that all requested columns exist in the DataFrame.
    """

    missing_columns = set(columns) - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Columns not found in DataFrame: "
            f"{sorted(missing_columns)}"
        )


def trim_columns(
    df: DataFrame,
    columns: Iterable[str],
) -> DataFrame:
    """
    Trim leading and trailing spaces from selected columns.
    """

    columns = list(columns)

    validate_columns(
        df=df,
        columns=columns,
    )

    for column in columns:
        df = df.withColumn(
            column,
            F.trim(F.col(column)),
        )

    return df


def uppercase_columns(
    df: DataFrame,
    columns: Iterable[str],
) -> DataFrame:
    """
    Convert selected string columns to uppercase.
    """

    columns = list(columns)

    validate_columns(
        df=df,
        columns=columns,
    )

    for column in columns:
        df = df.withColumn(
            column,
            F.upper(F.col(column)),
        )

    return df


def lowercase_columns(
    df: DataFrame,
    columns: Iterable[str],
) -> DataFrame:
    """
    Convert selected string columns to lowercase.
    """

    columns = list(columns)

    validate_columns(
        df=df,
        columns=columns,
    )

    for column in columns:
        df = df.withColumn(
            column,
            F.lower(F.col(column)),
        )

    return df


def nullify_empty_strings(
    df: DataFrame,
    columns: Iterable[str],
) -> DataFrame:
    """
    Convert empty strings to null for selected columns.
    """

    columns = list(columns)

    validate_columns(
        df=df,
        columns=columns,
    )

    for column in columns:
        df = df.withColumn(
            column,
            F.when(
                F.trim(F.col(column)) == "",
                F.lit(None),
            ).otherwise(
                F.col(column)
            ),
        )

    return df


def normalize_null_values(
    df: DataFrame,
    columns: Iterable[str],
    null_values: Iterable[str] = (
        "",
        "NULL",
        "null",
        "N/A",
        "NA",
    ),
) -> DataFrame:
    """
    Convert configured string representations of null to actual null.
    """

    columns = list(columns)
    null_values = list(null_values)

    validate_columns(
        df=df,
        columns=columns,
    )

    for column in columns:
        df = df.withColumn(
            column,
            F.when(
                F.trim(F.col(column)).isin(
                    null_values
                ),
                F.lit(None),
            ).otherwise(
                F.col(column)
            ),
        )

    return df


def deduplicate_latest(
    df: DataFrame,
    keys: Iterable[str],
    order_column: str,
) -> DataFrame:
    """
    Keep the latest record for each configured key.
    """

    keys = list(keys)

    validate_columns(
        df=df,
        columns=[
            *keys,
            order_column,
        ],
    )

    window = (
        Window
        .partitionBy(*keys)
        .orderBy(
            F.col(order_column)
            .desc_nulls_last()
        )
    )

    return (
        df
        .withColumn(
            "_row_number",
            F.row_number().over(window),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop("_row_number")
    )