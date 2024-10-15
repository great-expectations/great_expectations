from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.data_context.context_factory import project_manager
from great_expectations.datasource.fluent.interfaces import Datasource

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import CheckpointResult
    from great_expectations.data_context import AbstractDataContext, FileDataContext

DATASOURCE_NAME = "my_datasource"
ANIMAL_ASSET = "animals_names_asset"
COLUMN_PAIR_ASSET = "column_pair_asset"
MULTI_COLUMN_SUM_ASSET = "multi_column_sum_asset"


@pytest.fixture
def data_context_with_connection_to_metrics_db(
    tmp_path,
) -> FileDataContext:
    """
    Returns DataContext that has a single datasource that connects to a sqlite database.

    The sqlite database (metrics_test.db) contains one table `animal_names` that contains the following data

        "pk_1": [0, 1, 2, 3, 4, 5],
        "pk_2": ["zero", "one", "two", "three", "four", "five"],
        "animals": [
            "cat",
            "fish",
            "dog",
            "giraffe",
            "lion",
            "zebra",
        ],

    It is used by tests for unexpected_index_list (ID/Primary Key).
    """  # noqa: E501

    project_path = tmp_path / "test_configuration"
    context = gx.get_context(mode="file", project_root_dir=project_path)

    sqlite_path = pathlib.Path(__file__).parent / ".." / "test_sets/metrics_test.db"
    assert sqlite_path.exists()

    ds = context.data_sources.add_sqlite(
        name=DATASOURCE_NAME, connection_string=f"sqlite:///{sqlite_path}"
    )
    ds.add_table_asset(name=ANIMAL_ASSET, table_name="animal_names")
    ds.add_table_asset(name=COLUMN_PAIR_ASSET, table_name="column_pairs")
    ds.add_table_asset(name=MULTI_COLUMN_SUM_ASSET, table_name="multi_column_sums")

    project_manager.set_project(context)
    return context


@pytest.fixture
def expected_unexpected_indices_output() -> list[dict]:
    return [
        {"animals": "giraffe", "pk_1": 3},
        {"animals": "lion", "pk_1": 4},
        {"animals": "zebra", "pk_1": 5},
    ]


@pytest.fixture
def expected_sql_query_output() -> str:
    return "SELECT pk_1, animals \n\
FROM animal_names \n\
WHERE animals IS NOT NULL AND (animals NOT IN ('cat', 'fish', 'dog'));"


@pytest.fixture
def expect_column_values_to_be_in_set() -> gxe.ExpectColumnValuesToBeInSet:
    return gxe.ExpectColumnValuesToBeInSet(column="animals", value_set=["cat", "fish", "dog"])


@pytest.fixture
def expect_column_values_to_not_be_in_set() -> gxe.ExpectColumnValuesToNotBeInSet:
    return gxe.ExpectColumnValuesToNotBeInSet(
        column="animals",
        value_set=["giraffe", "lion", "zebra"],
    )


@pytest.fixture
def expect_column_pair_values_to_be_equal() -> gxe.ExpectColumnPairValuesToBeEqual:
    return gxe.ExpectColumnPairValuesToBeEqual(column_A="ordered_item", column_B="received_item")


@pytest.fixture
def expect_multicolumn_sum_to_equal() -> gxe.ExpectMulticolumnSumToEqual:
    return gxe.ExpectMulticolumnSumToEqual(column_list=["a", "b", "c"], sum_total=30)


def _build_checkpoint_and_run(
    context: AbstractDataContext,
    expectations: list[gxe.Expectation],
    asset_name: str,
    result_format: dict,
) -> CheckpointResult:
    suite = context.suites.add(
        suite=ExpectationSuite(name="metrics_exp", expectations=expectations)
    )

    ds = context.data_sources.get("my_datasource")
    assert isinstance(ds, Datasource)
    asset = ds.get_asset(asset_name)
    batch_definition = asset.add_batch_definition(name="my_batch_def")

    validation_definition = context.validation_definitions.add(
        ValidationDefinition(name="my_validation_def", suite=suite, data=batch_definition)
    )

    checkpoint = Checkpoint(
        name="my_checkpoint",
        validation_definitions=[validation_definition],
        result_format=result_format,
    )

    return checkpoint.run()


@pytest.mark.filesystem
def test_sql_result_format_in_checkpoint_pk_defined_one_expectation_complete_output(
    data_context_with_connection_to_metrics_db: FileDataContext,
    expect_column_values_to_be_in_set: gxe.ExpectColumnValuesToBeInSet,
    expected_unexpected_indices_output: list[dict],
    expected_sql_query_output: str,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - COMPLETE output, which means we have `unexpected_index_list` and
          `partial_unexpected_index_list`
        - 1 Expectations added to suite
    """
    result = _build_checkpoint_and_run(
        context=data_context_with_connection_to_metrics_db,
        expectations=[expect_column_values_to_be_in_set],
        asset_name=ANIMAL_ASSET,
        result_format={"result_format": "COMPLETE", "unexpected_index_column_names": ["pk_1"]},
    )
    evrs = list(result.run_results.values())

    index_column_names = evrs[0]["results"][0]["result"]["unexpected_index_column_names"]
    assert index_column_names == ["pk_1"]

    first_result_full_list = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result_full_list == expected_unexpected_indices_output

    first_result_partial_list = evrs[0]["results"][0]["result"]["partial_unexpected_index_list"]
    assert first_result_partial_list == expected_unexpected_indices_output

    unexpected_index_query: str = evrs[0]["results"][0]["result"]["unexpected_index_query"]
    assert unexpected_index_query == expected_sql_query_output


@pytest.mark.filesystem
def test_sql_result_format_in_checkpoint_pk_defined_one_expectation_complete_output_with_query(
    data_context_with_connection_to_metrics_db: FileDataContext,
    expect_column_values_to_be_in_set: gxe.ExpectColumnValuesToBeInSet,
    expected_unexpected_indices_output: list[dict[str, str | int]],
    expected_sql_query_output: str,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - COMPLETE output, which means we have `unexpected_index_list` and
         `partial_unexpected_index_list`
        - 1 Expectations added to suite
        - return_unexpected_index_query flag set to True
    """
    result = _build_checkpoint_and_run(
        context=data_context_with_connection_to_metrics_db,
        expectations=[expect_column_values_to_be_in_set],
        asset_name=ANIMAL_ASSET,
        result_format={
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
            "return_unexpected_index_query": True,
        },
    )
    evrs = list(result.run_results.values())

    index_column_names = evrs[0]["results"][0]["result"]["unexpected_index_column_names"]
    assert index_column_names == ["pk_1"]

    first_result_full_list = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result_full_list == expected_unexpected_indices_output

    first_result_partial_list = evrs[0]["results"][0]["result"]["partial_unexpected_index_list"]
    assert first_result_partial_list == expected_unexpected_indices_output

    unexpected_index_query = evrs[0]["results"][0]["result"]["unexpected_index_query"]
    assert unexpected_index_query == expected_sql_query_output


@pytest.mark.filesystem
def test_sql_result_format_in_checkpoint_pk_defined_column_pair_expectation_complete_output_with_query(  # noqa: E501
    data_context_with_connection_to_metrics_db: FileDataContext,
    expect_column_pair_values_to_be_equal: gxe.ExpectColumnPairValuesToBeEqual,
):
    result = _build_checkpoint_and_run(
        context=data_context_with_connection_to_metrics_db,
        expectations=[expect_column_pair_values_to_be_equal],
        asset_name=COLUMN_PAIR_ASSET,
        result_format={
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        },
    )
    evrs = list(result.run_results.values())

    index_column_names = evrs[0]["results"][0]["result"]["unexpected_index_column_names"]
    assert index_column_names == ["pk_1"]

    first_result_full_list = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result_full_list == [
        {"pk_1": 3, "ordered_item": "eraser", "received_item": "desk"},
        {"pk_1": 4, "ordered_item": "eraser", "received_item": "desk"},
        {"pk_1": 5, "ordered_item": "eraser", "received_item": "desk"},
    ]

    first_result_partial_list = evrs[0]["results"][0]["result"]["partial_unexpected_index_list"]
    assert first_result_partial_list == [
        {"pk_1": 3, "ordered_item": "eraser", "received_item": "desk"},
        {"pk_1": 4, "ordered_item": "eraser", "received_item": "desk"},
        {"pk_1": 5, "ordered_item": "eraser", "received_item": "desk"},
    ]

    unexpected_index_query = evrs[0]["results"][0]["result"]["unexpected_index_query"]
    assert (
        unexpected_index_query
        == "SELECT pk_1, ordered_item, received_item \nFROM column_pairs \nWHERE NOT (ordered_item = received_item AND NOT (ordered_item IS NULL OR received_item IS NULL));"  # noqa: E501
    )


@pytest.mark.filesystem
def test_sql_result_format_in_checkpoint_pk_defined_column_pair_expectation_summary_output(
    data_context_with_connection_to_metrics_db: FileDataContext,
    expect_column_pair_values_to_be_equal: gxe.ExpectColumnPairValuesToBeEqual,
):
    result = _build_checkpoint_and_run(
        context=data_context_with_connection_to_metrics_db,
        expectations=[expect_column_pair_values_to_be_equal],
        asset_name=COLUMN_PAIR_ASSET,
        result_format={
            "result_format": "SUMMARY",
            "unexpected_index_column_names": ["pk_1"],
        },
    )
    evrs = list(result.run_results.values())

    index_column_names = evrs[0]["results"][0]["result"]["unexpected_index_column_names"]
    assert index_column_names == ["pk_1"]
    first_result_full_list = evrs[0]["results"][0]["result"].get("unexpected_index_list")
    assert not first_result_full_list

    first_result_partial_list = evrs[0]["results"][0]["result"]["partial_unexpected_index_list"]
    assert first_result_partial_list == [
        {"pk_1": 3, "ordered_item": "eraser", "received_item": "desk"},
        {"pk_1": 4, "ordered_item": "eraser", "received_item": "desk"},
        {"pk_1": 5, "ordered_item": "eraser", "received_item": "desk"},
    ]

    unexpected_index_query = evrs[0]["results"][0]["result"].get("unexpected_index_query")
    assert not unexpected_index_query


@pytest.mark.filesystem
def test_sql_result_format_in_checkpoint_pk_defined_multi_column_sum_expectation_complete_output_with_query(  # noqa: E501
    data_context_with_connection_to_metrics_db: FileDataContext,
    expect_multicolumn_sum_to_equal: gxe.ExpectMulticolumnSumToEqual,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - COMPLETE output, which means we have `unexpected_index_list` and
          `partial_unexpected_index_list`
        - 1 Expectations added to suite
        - return_unexpected_index_query flag set to True
    """
    result = _build_checkpoint_and_run(
        context=data_context_with_connection_to_metrics_db,
        expectations=[expect_multicolumn_sum_to_equal],
        asset_name=MULTI_COLUMN_SUM_ASSET,
        result_format={
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
            "return_unexpected_index_query": True,
        },
    )
    evrs = list(result.run_results.values())

    index_column_names = evrs[0]["results"][0]["result"]["unexpected_index_column_names"]
    assert index_column_names == ["pk_1"]

    first_result_full_list = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result_full_list == [
        {"pk_1": 1, "a": 20, "b": 20, "c": 20},
        {"pk_1": 2, "a": 30, "b": 30, "c": 30},
        {"pk_1": 3, "a": 40, "b": 40, "c": 40},
        {"pk_1": 4, "a": 50, "b": 50, "c": 50},
        {"pk_1": 5, "a": 60, "b": 60, "c": 60},
    ]

    first_result_partial_list = evrs[0]["results"][0]["result"]["partial_unexpected_index_list"]
    assert first_result_partial_list == [
        {"pk_1": 1, "a": 20, "b": 20, "c": 20},
        {"pk_1": 2, "a": 30, "b": 30, "c": 30},
        {"pk_1": 3, "a": 40, "b": 40, "c": 40},
        {"pk_1": 4, "a": 50, "b": 50, "c": 50},
        {"pk_1": 5, "a": 60, "b": 60, "c": 60},
    ]

    unexpected_index_query = evrs[0]["results"][0]["result"]["unexpected_index_query"]
    assert (
        unexpected_index_query
        == "SELECT pk_1, a, b, c \nFROM multi_column_sums \nWHERE 0 + a + b + c != 30.0;"
    )


@pytest.mark.filesystem
def test_sql_result_format_in_checkpoint_pk_defined_multi_column_sum_expectation_summary_output(
    data_context_with_connection_to_metrics_db: FileDataContext,
    expect_multicolumn_sum_to_equal: gxe.ExpectMulticolumnSumToEqual,
):
    result = _build_checkpoint_and_run(
        context=data_context_with_connection_to_metrics_db,
        expectations=[expect_multicolumn_sum_to_equal],
        asset_name=MULTI_COLUMN_SUM_ASSET,
        result_format={
            "result_format": "SUMMARY",
            "unexpected_index_column_names": ["pk_1"],
        },
    )
    evrs = list(result.run_results.values())

    index_column_names = evrs[0]["results"][0]["result"]["unexpected_index_column_names"]
    assert index_column_names == ["pk_1"]

    first_result_full_list = evrs[0]["results"][0]["result"].get("unexpected_index_list")
    assert not first_result_full_list

    first_result_partial_list = evrs[0]["results"][0]["result"]["partial_unexpected_index_list"]
    assert first_result_partial_list == [
        {"pk_1": 1, "a": 20, "b": 20, "c": 20},
        {"pk_1": 2, "a": 30, "b": 30, "c": 30},
        {"pk_1": 3, "a": 40, "b": 40, "c": 40},
        {"pk_1": 4, "a": 50, "b": 50, "c": 50},
        {"pk_1": 5, "a": 60, "b": 60, "c": 60},
    ]
    unexpected_index_query = evrs[0]["results"][0]["result"].get("unexpected_index_query")
    assert not unexpected_index_query


@pytest.mark.filesystem
def test_sql_result_format_in_checkpoint_pk_defined_one_expectation_complete_output_no_query(
    data_context_with_connection_to_metrics_db: FileDataContext,
    expect_column_values_to_be_in_set: gxe.ExpectColumnValuesToBeInSet,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - COMPLETE output, which means we have `unexpected_index_list` and
          `partial_unexpected_index_list`
        - 1 Expectations added to suite
        - return_unexpected_index_query flag set to False
    """
    result = _build_checkpoint_and_run(
        context=data_context_with_connection_to_metrics_db,
        expectations=[expect_column_values_to_be_in_set],
        asset_name=ANIMAL_ASSET,
        result_format={
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
            "return_unexpected_index_query": False,
        },
    )
    evrs = list(result.run_results.values())

    index_column_names = evrs[0]["results"][0]["result"]["unexpected_index_column_names"]
    assert index_column_names == ["pk_1"]

    first_result_full_list = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result_full_list == expected_unexpected_indices_output

    first_result_partial_list = evrs[0]["results"][0]["result"]["partial_unexpected_index_list"]
    assert first_result_partial_list == expected_unexpected_indices_output

    assert evrs[0]["results"][0]["result"].get("unexpected_index_query") is None


@pytest.mark.filesystem
def test_sql_result_format_in_checkpoint_pk_defined_two_expectation_complete_output(
    data_context_with_connection_to_metrics_db: FileDataContext,
    expect_column_values_to_be_in_set: gxe.ExpectColumnValuesToBeInSet,
    expect_column_values_to_not_be_in_set: gxe.ExpectColumnValuesToNotBeInSet,
    expected_unexpected_indices_output: list[dict[str, str | int]],
    expected_sql_query_output: str,
):
    """
    What does this test?
        - unexpected_index_column not defined in Checkpoint config, but passed in at
          run_checkpoint.
        - COMPLETE output, which means we have `unexpected_index_list` and
          `partial_unexpected_index_list`
        - 2 Expectations added to suite
    """
    result = _build_checkpoint_and_run(
        context=data_context_with_connection_to_metrics_db,
        expectations=[
            expect_column_values_to_be_in_set,
            expect_column_values_to_not_be_in_set,
        ],
        asset_name=ANIMAL_ASSET,
        result_format={
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        },
    )
    evrs = list(result.run_results.values())

    index_column_names = evrs[0]["results"][0]["result"]["unexpected_index_column_names"]
    assert index_column_names == ["pk_1"]

    # first and second expectations have same results. Although one is "expect_to_be"
    # and the other is "expect_to_not_be", they have opposite value_sets
    first_result_full_list = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result_full_list == expected_unexpected_indices_output

    first_result_partial_list = evrs[0]["results"][0]["result"]["partial_unexpected_index_list"]
    assert first_result_partial_list == expected_unexpected_indices_output

    second_result_full_list = evrs[0]["results"][1]["result"]["unexpected_index_list"]
    assert second_result_full_list == expected_unexpected_indices_output

    second_result_partial_list = evrs[0]["results"][1]["result"]["partial_unexpected_index_list"]
    assert second_result_partial_list == expected_unexpected_indices_output

    unexpected_index_query = evrs[0]["results"][0]["result"]["unexpected_index_query"]
    assert unexpected_index_query == expected_sql_query_output


@pytest.mark.filesystem
def test_sql_result_format_in_checkpoint_pk_defined_one_expectation_summary_output(
    data_context_with_connection_to_metrics_db: FileDataContext,
    expect_column_values_to_be_in_set: gxe.ExpectColumnValuesToBeInSet,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - SUMMARY output, which means we have `partial_unexpected_index_list` only
        - 1 Expectations added to suite
    """
    result = _build_checkpoint_and_run(
        context=data_context_with_connection_to_metrics_db,
        expectations=[
            expect_column_values_to_be_in_set,
        ],
        asset_name=ANIMAL_ASSET,
        result_format={
            "result_format": "SUMMARY",
            "unexpected_index_column_names": ["pk_1"],
        },
    )
    evrs = list(result.run_results.values())

    index_column_names = evrs[0]["results"][0]["result"]["unexpected_index_column_names"]
    assert index_column_names == ["pk_1"]

    first_result_full_list = evrs[0]["results"][0]["result"].get("unexpected_index_list")
    assert not first_result_full_list

    first_result_partial_list = evrs[0]["results"][0]["result"]["partial_unexpected_index_list"]
    assert first_result_partial_list == expected_unexpected_indices_output


@pytest.mark.filesystem
def test_sql_result_format_in_checkpoint_pk_defined_one_expectation_basic_output(
    data_context_with_connection_to_metrics_db: FileDataContext,
    expect_column_values_to_be_in_set: gxe.ExpectColumnValuesToBeInSet,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - BASIC output, which means we have no unexpected_index_list output
        - 1 Expectations added to suite
    """
    result = _build_checkpoint_and_run(
        context=data_context_with_connection_to_metrics_db,
        expectations=[
            expect_column_values_to_be_in_set,
        ],
        asset_name=ANIMAL_ASSET,
        result_format={
            "result_format": "BASIC",
            "unexpected_index_column_names": ["pk_1"],
        },
    )
    evrs = list(result.run_results.values())

    index_column_names = evrs[0]["results"][0]["result"]["unexpected_index_column_names"]
    assert index_column_names == ["pk_1"]

    first_result_full_list = evrs[0]["results"][0]["result"].get("unexpected_index_list")
    assert not first_result_full_list

    first_result_partial_list = evrs[0]["results"][0]["result"].get("partial_unexpected_index_list")
    assert not first_result_partial_list

    assert evrs[0]["results"][0]["result"].get("unexpected_index_query") is None


@pytest.mark.filesystem
def test_sql_complete_output_no_id_pk_fallback(
    data_context_with_connection_to_metrics_db: FileDataContext,
    expect_column_values_to_be_in_set: gxe.ExpectColumnValuesToBeInSet,
):
    result = _build_checkpoint_and_run(
        context=data_context_with_connection_to_metrics_db,
        expectations=[
            expect_column_values_to_be_in_set,
        ],
        asset_name=ANIMAL_ASSET,
        result_format={
            "result_format": "COMPLETE",
        },
    )
    evrs = list(result.run_results.values())

    index_column_names = evrs[0]["results"][0]["result"].get("unexpected_index_column_names")
    assert not index_column_names

    first_result_full_list = evrs[0]["results"][0]["result"].get("unexpected_index_list")
    assert not first_result_full_list

    first_result_partial_list = evrs[0]["results"][0]["result"].get("partial_unexpected_index_list")
    assert not first_result_partial_list

    unexpected_index_query = evrs[0]["results"][0]["result"].get("unexpected_index_query")
    # query does not contain id_pk column
    assert (
        unexpected_index_query
        == "SELECT animals \nFROM animal_names \nWHERE animals IS NOT NULL AND (animals NOT IN ('cat', 'fish', 'dog'));"  # noqa: E501
    )
