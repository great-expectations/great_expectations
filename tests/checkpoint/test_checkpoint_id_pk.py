from __future__ import annotations

import pathlib

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations import project_manager
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context import FileDataContext


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

    ds = context.sources.add_sqlite(
        name="my_datasource", connection_string=f"sqlite:///{sqlite_path}"
    )
    ds.add_table_asset(name="animals_names_asset", table_name="animal_names")
    ds.add_table_asset(name="column_pair_asset", table_name="column_pairs")
    ds.add_table_asset(name="multi_column_sum_asset", table_name="multi_column_sums")

    context._save_project_config()
    project_manager.set_project(context)
    return context  # type: ignore[return-value]


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
def expect_column_values_to_be_in_set():
    return gxe.ExpectColumnValuesToBeInSet(column="animals", value_set=["cat", "fish", "dog"])


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
    context = data_context_with_connection_to_metrics_db

    suite = context.suites.add(
        suite=ExpectationSuite(name="metrics_exp", expectations=[expect_column_values_to_be_in_set])
    )

    ds = context.get_datasource("my_datasource")
    asset = ds.get_asset("animals_names_asset")
    batch_definition = asset.add_batch_definition(name="my_batch_def")

    validation_definition = ValidationDefinition(
        name="my_validation_def", suite=suite, data=batch_definition
    )

    checkpoint = Checkpoint(
        name="my_checkpoint",
        validation_definitions=[validation_definition],
        result_format={"result_format": "COMPLETE", "unexpected_index_column_names": ["pk_1"]},
    )

    result = checkpoint.run()

    evrs = list(result.run_results.values())
    index_column_names = evrs[0]["results"][0]["result"]["unexpected_index_column_names"]
    assert index_column_names == ["pk_1"]

    first_result_full_list = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result_full_list == expected_unexpected_indices_output

    first_result_partial_list = evrs[0]["results"][0]["result"]["partial_unexpected_index_list"]
    assert first_result_partial_list == expected_unexpected_indices_output

    unexpected_index_query: str = evrs[0]["results"][0]["result"]["unexpected_index_query"]
    assert unexpected_index_query == expected_sql_query_output
