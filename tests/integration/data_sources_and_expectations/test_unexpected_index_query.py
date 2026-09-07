import re

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.data_context import get_context
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config import (
    SQL_DATA_SOURCES,
)

DATA = pd.DataFrame(
    {
        "id": [1, 2, 3, 4, 5, 6],
        "val": [3, 4, 5, 6, 7, None],
    }
)


@parameterize_batch_for_data_sources(data_source_configs=SQL_DATA_SOURCES, data=DATA)
def test_unexpected_index_query_compiles_parameters(
    batch_for_datasource: Batch,
) -> None:
    """
    Test that unexpected_index_query has compiled SQL parameters, not placeholders like :param_1.

    For ExpectColumnValuesToBeBetween with min_value=3 and max_value=5:
    - Expected: WHERE val IS NOT NULL AND NOT (val >= 3 AND val <= 5)
    - Bug: WHERE val IS NOT NULL AND NOT (val >= :param_1 AND val <= :param_2)
    """
    min_value = 3
    max_value = 5
    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="val",
        min_value=min_value,
        max_value=max_value,
    )

    result = batch_for_datasource.validate(
        expectation,
        result_format={
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["id"],
        },
    )

    # The expectation should fail because values 6 and 7 are outside the range [3, 5]
    assert not result.success
    result_dict = result["result"]

    # Check that unexpected_index_query exists
    assert "unexpected_index_query" in result_dict
    unexpected_index_query = result_dict["unexpected_index_query"]

    # These assertions exist strictly to protect against regressions
    assert not re.search(r":param_\d+", unexpected_index_query), (
        f"Parameter placeholder (:param_N) was not compiled. Query: {unexpected_index_query}"
    )
    assert not re.search(r"%\(param_\d+\)s", unexpected_index_query), (
        f"Parameter placeholder (%(param_N)s) was not compiled. Query: {unexpected_index_query}"
    )
    # Note: We don't check for positional parameter "?" since it could appear in legitimate SQL

    # Verify the query contains the actual values
    assert str(min_value) in unexpected_index_query, (
        f"Value {min_value} not found in query: {unexpected_index_query}"
    )
    assert str(max_value) in unexpected_index_query, (
        f"Value {max_value} not found in query: {unexpected_index_query}"
    )


@parameterize_batch_for_data_sources(data_source_configs=SQL_DATA_SOURCES, data=DATA)
def test_boolean_only_with_return_unexpected_index_query(
    batch_for_datasource: Batch,
) -> None:
    """
    Test that BOOLEAN_ONLY includes unexpected_index_query when explicitly requested.
    """
    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="val",
        min_value=3,
        max_value=5,
    )

    result = batch_for_datasource.validate(
        expectation,
        result_format={
            "result_format": "BOOLEAN_ONLY",
            "return_unexpected_index_query": True,
            "unexpected_index_column_names": ["id"],
            "partial_unexpected_count": 0,
        },
    )

    # The expectation should fail because values 6 and 7 are outside the range [3, 5]
    assert not result.success

    # BOOLEAN_ONLY with return_unexpected_index_query should include the query
    assert "result" in result
    assert "unexpected_index_query" in result["result"]
    assert result["result"]["unexpected_index_query"] is not None

    # Should also include unexpected_index_column_names
    assert "unexpected_index_column_names" in result["result"]
    assert result["result"]["unexpected_index_column_names"] == ["id"]


@parameterize_batch_for_data_sources(data_source_configs=SQL_DATA_SOURCES, data=DATA)
def test_boolean_only_without_return_unexpected_index_query(
    batch_for_datasource: Batch,
) -> None:
    """Test that BOOLEAN_ONLY without return_unexpected_index_query.

    Maintains backwards compatibility.
    """
    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="val",
        min_value=3,
        max_value=5,
    )

    result = batch_for_datasource.validate(
        expectation,
        result_format={
            "result_format": "BOOLEAN_ONLY",
        },
    )

    # The expectation should fail
    assert not result.success

    # BOOLEAN_ONLY without the flag should NOT include result dict
    assert result.result == {}


@parameterize_batch_for_data_sources(data_source_configs=SQL_DATA_SOURCES, data=DATA)
def test_basic_with_return_unexpected_index_query(
    batch_for_datasource: Batch,
) -> None:
    """
    Test that BASIC includes unexpected_index_query when explicitly requested.
    """
    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="val",
        min_value=3,
        max_value=5,
    )

    result = batch_for_datasource.validate(
        expectation,
        result_format={
            "result_format": "BASIC",
            "return_unexpected_index_query": True,
            "unexpected_index_column_names": ["id"],
            "partial_unexpected_count": 25,
        },
    )

    # The expectation should fail
    assert not result.success

    # BASIC with return_unexpected_index_query should include the query
    assert "result" in result
    assert "unexpected_index_query" in result["result"]
    assert result["result"]["unexpected_index_query"] is not None

    # BASIC should still include its normal result fields
    assert "element_count" in result["result"]
    assert "unexpected_count" in result["result"]


@parameterize_batch_for_data_sources(data_source_configs=SQL_DATA_SOURCES, data=DATA)
def test_basic_without_return_unexpected_index_query(
    batch_for_datasource: Batch,
) -> None:
    """Test that BASIC without return_unexpected_index_query.

    Maintains backwards compatibility.
    """
    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="val",
        min_value=3,
        max_value=5,
    )

    result = batch_for_datasource.validate(
        expectation,
        result_format={
            "result_format": "BASIC",
        },
    )

    # The expectation should fail
    assert not result.success

    # BASIC without the flag should NOT include unexpected_index_query
    assert "unexpected_index_query" not in result["result"]

    # But should still have normal BASIC result fields
    assert "element_count" in result["result"]
    assert "unexpected_count" in result["result"]


# Pandas-specific tests (no SQL required)
@pytest.mark.unit
def test_pandas_boolean_only_with_return_unexpected_index_query() -> None:
    """
    Test BOOLEAN_ONLY with return_unexpected_index_query using Pandas.
    """
    context = get_context(mode="ephemeral")
    data_source = context.data_sources.pandas_default
    data_asset = data_source.add_dataframe_asset(name="test_asset")
    batch_def = data_asset.add_batch_definition_whole_dataframe(name="batch_def")

    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "val": ["a", "b", "c", "d", "e"]})
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    expectation = gxe.ExpectColumnValuesToBeInSet(
        column="val",
        value_set=["a", "b", "c"],
    )

    result = batch.validate(
        expectation,
        result_format={
            "result_format": "BOOLEAN_ONLY",
            "return_unexpected_index_query": True,
            "partial_unexpected_count": 0,
        },
    )

    # Should fail because "d" and "e" are not in the set
    assert not result.success

    # Should include the query
    assert "result" in result
    assert "unexpected_index_query" in result["result"]
    # Pandas returns a filter expression like "df.filter(items=[3, 4], axis=0)"
    assert "df.filter" in result["result"]["unexpected_index_query"]


@pytest.mark.unit
def test_pandas_basic_with_return_unexpected_index_query() -> None:
    """
    Test BASIC with return_unexpected_index_query using Pandas.
    """
    context = get_context(mode="ephemeral")
    data_source = context.data_sources.pandas_default
    data_asset = data_source.add_dataframe_asset(name="test_asset_2")
    batch_def = data_asset.add_batch_definition_whole_dataframe(name="batch_def")

    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "val": ["a", "b", "c", "d", "e"]})
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    expectation = gxe.ExpectColumnValuesToBeInSet(
        column="val",
        value_set=["a", "b", "c"],
    )

    result = batch.validate(
        expectation,
        result_format={
            "result_format": "BASIC",
            "return_unexpected_index_query": True,
            "partial_unexpected_count": 25,
        },
    )

    # Should fail
    assert not result.success

    # Should include the query
    assert "unexpected_index_query" in result["result"]

    # Should also have BASIC fields
    assert "element_count" in result["result"]
    assert "unexpected_count" in result["result"]
