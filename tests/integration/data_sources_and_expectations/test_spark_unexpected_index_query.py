"""The Spark ``unexpected_index_query`` must be a runnable Python expression."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.compatibility.pyspark import functions as F

if TYPE_CHECKING:
    from great_expectations.core.expectation_validation_result import (
        ExpectationValidationResult,
    )

pytestmark = pytest.mark.spark


@pytest.fixture
def cities_df(spark_session):
    return spark_session.createDataFrame([("nyc",), ("sf",), ("boston",), ("la",)], "city string")


def _validate_city_lengths(df) -> ExpectationValidationResult:
    """Validate a condition with no string literals in it, so that the outer quoting of the
    rendered query is the only thing standing between it and a runnable expression.
    """
    context = gx.get_context(mode="ephemeral")
    asset = context.data_sources.add_spark(name="spark").add_dataframe_asset(name="cities")
    batch = asset.add_batch_definition_whole_dataframe(name="cities_bd").get_batch(
        batch_parameters={"dataframe": df}
    )
    return batch.validate(
        gxe.ExpectColumnValueLengthsToBeBetween(column="city", min_value=2, max_value=3),
        result_format="COMPLETE",
    )


def test_spark_unexpected_index_query_is_valid_python(cities_df) -> None:
    """The rendered query parses as Python on every supported pyspark version."""
    result = _validate_city_lengths(cities_df)

    query = result.result["unexpected_index_query"]

    compile(query, "<unexpected_index_query>", "eval")


def test_spark_unexpected_index_query_returns_the_unexpected_rows(cities_df) -> None:
    """Evaluating the rendered query returns exactly the rows counted in unexpected_count."""
    result = _validate_city_lengths(cities_df)

    query = result.result["unexpected_index_query"]

    unexpected_rows = eval(query, {"df": cities_df, "F": F})
    assert unexpected_rows.count() == result.result["unexpected_count"] == 1


def test_spark_unexpected_index_query_quotes_string_literals(cities_df) -> None:
    """String literals in the condition are quoted, so an in-set query evaluates too."""
    context = gx.get_context(mode="ephemeral")
    asset = context.data_sources.add_spark(name="spark").add_dataframe_asset(name="cities")
    batch = asset.add_batch_definition_whole_dataframe(name="cities_bd").get_batch(
        batch_parameters={"dataframe": cities_df}
    )
    result = batch.validate(
        gxe.ExpectColumnValuesToBeInSet(column="city", value_set=["nyc", "sf"]),
        result_format="COMPLETE",
    )

    query = result.result["unexpected_index_query"]

    unexpected_rows = eval(query, {"df": cities_df, "F": F})
    assert unexpected_rows.count() == result.result["unexpected_count"] == 2
