"""Null filters and typed row conditions over nested struct columns on Spark."""

from __future__ import annotations

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.expectations.row_conditions import Column

pytestmark = pytest.mark.spark


def _get_batch(spark_session, rows):
    # Bypasses @parameterize_batch_for_data_sources: its Spark config
    # (SparkFilesystemCsvDatasourceTestConfig) round-trips data through a flat
    # CSV file, which can't carry nested struct columns like Row(address=Row(...)).
    # Same manual add_spark -> add_dataframe_asset chain as
    # test_spark_nested_column_value_counts.py, for the same reason.
    df = spark_session.createDataFrame(rows)
    context = gx.get_context(mode="ephemeral")
    asset = context.data_sources.add_spark(name="spark").add_dataframe_asset(name="people")
    return asset.add_batch_definition_whole_dataframe("bd").get_batch(
        batch_parameters={"dataframe": df}
    )


def _people(spark_session):
    from pyspark.sql import Row

    return _get_batch(
        spark_session,
        [
            Row(id=1, address=Row(city="paris")),
            Row(id=2, address=Row(city="rome")),
            Row(id=3, address=Row(city="paris")),
            Row(id=4, address=Row(city=None)),
        ],
    )


def test_unique_on_nested_struct_column_filters_nulls(spark_session) -> None:
    # ExpectColumnValuesToBeUnique filters nulls through add_column_row_condition,
    # so the nested path must reach Spark as struct access, not one identifier.
    result = _people(spark_session).validate(
        gxe.ExpectColumnValuesToBeUnique(column="address.city")
    )

    assert result.result, result.exception_info
    assert not result.success
    assert result.result["unexpected_count"] == 2  # both "paris" rows
    assert result.result["missing_count"] == 1


@pytest.mark.parametrize(
    "row_condition,expected_count",
    [
        pytest.param(Column("address.city") == "paris", 2, id="comparison"),
        pytest.param(Column("address.city").is_in(["paris", "rome"]), 3, id="in"),
        pytest.param(Column("address.city").is_not_null(), 3, id="nullity"),
    ],
)
def test_typed_row_condition_on_nested_struct_field(
    spark_session, row_condition, expected_count
) -> None:
    result = _people(spark_session).validate(
        gxe.ExpectColumnValuesToNotBeNull(column="id", row_condition=row_condition)
    )

    assert result.result, result.exception_info
    assert result.success
    assert result.result["element_count"] == expected_count


def test_nested_struct_field_under_a_parent_name_with_a_space(spark_session) -> None:
    from pyspark.sql import Row

    batch = _get_batch(
        spark_session,
        [
            Row(**{"home address": Row(city="paris")}),
            Row(**{"home address": Row(city="rome")}),
        ],
    )

    result = batch.validate(
        gxe.ExpectColumnValuesToBeUnique(
            column="home address.city",
            row_condition=Column("home address.city") != "rome",
        )
    )

    assert result.result, result.exception_info
    assert result.success
    assert result.result["element_count"] == 1


def test_flat_column_name_containing_a_dot_is_not_split(spark_session) -> None:
    batch = _get_batch(spark_session, [{"a.b": 1}, {"a.b": 2}, {"a.b": 2}])

    result = batch.validate(gxe.ExpectColumnValuesToBeUnique(column="a.b"))

    assert result.result, result.exception_info
    assert not result.success
    assert result.result["unexpected_count"] == 2
