from collections.abc import Iterable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def build_merge_condition(
    keys: Iterable[str],
    target_alias: str = "target",
    source_alias: str = "source",
) -> str:
    """
    Build a Delta MERGE condition from one or more business keys.
    """

    keys = list(keys)

    if not keys:
        raise ValueError(
            "At least one merge key is required."
        )

    return " AND ".join(
        f"{target_alias}.`{key}` = "
        f"{source_alias}.`{key}`"
        for key in keys
    )


def merge_upsert(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    keys: Iterable[str],
    exclude_update_columns: Iterable[str] | None = None,
) -> None:
    """
    Upsert source records into a Delta target table.

    Existing records are updated.
    New records are inserted.

    Columns listed in exclude_update_columns are preserved
    when the target row already exists.
    """

    keys = list(keys)
    exclude_update_columns = set(
        exclude_update_columns or []
    )

    if source_df.isEmpty():
        return

    target = DeltaTable.forName(
        spark,
        target_table,
    )

    condition = build_merge_condition(
        keys=keys,
    )

    update_columns = [
        column
        for column in source_df.columns
        if column not in exclude_update_columns
    ]

    update_mapping = {
        column: f"source.`{column}`"
        for column in update_columns
    }

    (
        target.alias("target")
        .merge(
            source_df.alias("source"),
            condition,
        )
        .whenMatchedUpdate(
            set=update_mapping,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_insert_only(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    keys: Iterable[str],
) -> None:
    """
    Insert only records whose merge keys do not already
    exist in the target table.
    """

    if source_df.isEmpty():
        return

    target = DeltaTable.forName(
        spark,
        target_table,
    )

    condition = build_merge_condition(
        keys=keys,
    )

    (
        target.alias("target")
        .merge(
            source_df.alias("source"),
            condition,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )