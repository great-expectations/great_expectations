from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List

import pytest

from great_expectations.checkpoint.checkpoint import SimpleCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationSuiteValidationResult,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.data_context import (
    AbstractDataContext,
    DataContext,
    FileDataContext,
)
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.exceptions import CheckpointError
from great_expectations.util import filter_properties_dict

if TYPE_CHECKING:
    import pandas as pd

    from great_expectations.compatibility import pyspark

yaml = YAMLHandler()

logger = logging.getLogger(__name__)


@pytest.fixture()
def reference_checkpoint_config_for_unexpected_column_names() -> dict:
    """
    This is a reference checkpoint dict. It connects to Datasource defined in
    data_context_with_connection_to_metrics_db fixture
    """
    checkpoint_dict: dict = {
        "name": "my_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "module_name": "great_expectations.checkpoint",
        "template_name": None,
        "run_name_template": "%Y-%M-foo-bar-template-test",
        "expectation_suite_name": None,
        "batch_request": None,
        "profilers": [],
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
        ],
        "validations": [],
        "runtime_configuration": {},
    }
    return checkpoint_dict


@pytest.fixture()
def reference_sql_checkpoint_config_for_animal_names_table(
    reference_checkpoint_config_for_unexpected_column_names,
) -> dict:
    """
    This is a reference checkpoint dict. It connects to Datasource defined in
    data_context_with_connection_to_metrics_db fixture
    """
    reference_checkpoint_config_for_unexpected_column_names["validations"] = [
        {
            "batch_request": {
                "datasource_name": "my_datasource",
                "data_connector_name": "my_sql_data_connector",
                "data_asset_name": "animals_names_asset",
            },
            "expectation_suite_name": "metrics_exp",
        }
    ]
    return reference_checkpoint_config_for_unexpected_column_names


@pytest.fixture()
def reference_sql_checkpoint_config_for_column_pairs_table(
    reference_checkpoint_config_for_unexpected_column_names,
) -> dict:
    """
    This is a reference checkpoint dict. It connects to Datasource defined in
    data_context_with_connection_to_metrics_db fixture
    """
    reference_checkpoint_config_for_unexpected_column_names["validations"] = [
        {
            "batch_request": {
                "datasource_name": "my_datasource",
                "data_connector_name": "my_sql_data_connector",
                "data_asset_name": "column_pair_asset",
            },
            "expectation_suite_name": "metrics_exp",
        }
    ]
    return reference_checkpoint_config_for_unexpected_column_names


@pytest.fixture()
def reference_sql_checkpoint_config_for_multi_column_sum_table(
    reference_checkpoint_config_for_unexpected_column_names,
) -> dict:
    """
    This is a reference checkpoint dict. It connects to Datasource defined in
    data_context_with_connection_to_metrics_db fixture
    """
    reference_checkpoint_config_for_unexpected_column_names["validations"] = [
        {
            "batch_request": {
                "datasource_name": "my_datasource",
                "data_connector_name": "my_sql_data_connector",
                "data_asset_name": "multi_column_sum_asset",
            },
            "expectation_suite_name": "metrics_exp",
        }
    ]
    return reference_checkpoint_config_for_unexpected_column_names


@pytest.fixture()
def expectation_config_expect_multicolumn_sum_to_equal() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_multicolumn_sum_to_equal",
        kwargs={"column_list": ["a", "b", "c"], "sum_total": 30},
    )


@pytest.fixture()
def expectation_config_expect_column_pair_values_to_be_equal() -> (
    ExpectationConfiguration
):
    return ExpectationConfiguration(
        expectation_type="expect_column_pair_values_to_be_equal",
        kwargs={"column_A": "ordered_item", "column_B": "received_item"},
    )


@pytest.fixture()
def expectation_config_expect_column_values_to_be_in_set() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
        },
    )


@pytest.fixture()
def expectation_config_expect_column_values_to_not_be_in_set() -> (
    ExpectationConfiguration
):
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["giraffe", "lion", "zebra"],
        },
    )


@pytest.fixture()
def batch_request_for_pandas_unexpected_rows_and_index_column_pair(
    pandas_column_pairs_dataframe_for_unexpected_rows_and_index,
) -> dict:
    dataframe: pd.DataFrame = (
        pandas_column_pairs_dataframe_for_unexpected_rows_and_index
    )
    return {
        "datasource_name": "pandas_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }


@pytest.fixture()
def batch_request_for_pandas_unexpected_rows_and_index_multicolumn_sum(
    pandas_multicolumn_sum_dataframe_for_unexpected_rows_and_index,
) -> dict:
    dataframe: pd.DataFrame = (
        pandas_multicolumn_sum_dataframe_for_unexpected_rows_and_index
    )
    return {
        "datasource_name": "pandas_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }


@pytest.fixture()
def batch_request_for_pandas_unexpected_rows_and_index(
    pandas_animals_dataframe_for_unexpected_rows_and_index,
) -> dict:
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    return {
        "datasource_name": "pandas_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }


@pytest.fixture()
def batch_request_for_spark_unexpected_rows_and_index(
    spark_dataframe_for_unexpected_rows_with_index,
) -> dict:
    dataframe: pyspark.sql.dataframe.DataFrame = (
        spark_dataframe_for_unexpected_rows_with_index
    )
    return {
        "datasource_name": "spark_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }


@pytest.fixture()
def batch_request_for_spark_unexpected_rows_and_index_column_pair(
    spark_column_pairs_dataframe_for_unexpected_rows_and_index,
) -> dict:
    dataframe: pyspark.sql.dataframe.DataFrame = (
        spark_column_pairs_dataframe_for_unexpected_rows_and_index
    )
    return {
        "datasource_name": "spark_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }


@pytest.fixture()
def batch_request_for_spark_unexpected_rows_and_index_multicolumn_sum(
    spark_multicolumn_sum_dataframe_for_unexpected_rows_and_index,
) -> dict:
    dataframe: pyspark.sql.dataframe.DataFrame = (
        spark_multicolumn_sum_dataframe_for_unexpected_rows_and_index
    )
    return {
        "datasource_name": "spark_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }


@pytest.fixture
def expected_unexpected_indices_output() -> list[dict[str, str | int]]:
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
def expected_spark_query_output() -> str:
    return "df.filter(F.expr((animals IS NOT NULL) AND (NOT (animals IN (cat, fish, dog)))))"


def _add_expectations_and_checkpoint(
    data_context: DataContext | FileDataContext,
    checkpoint_config: dict,
    expectations_list: List[ExpectationConfiguration],
    dict_to_update_checkpoint: dict | None = None,
) -> DataContext | FileDataContext:
    """
    Helper method for adding Checkpoint and Expectations to DataContext.

    Args:
        data_context (DataContext): data_context_with_connection_to_metrics_db
        checkpoint_config : Checkpoint to add
        expectations_list : Expectations to add

    Returns:
        DataContext with updated config
    """
    if dict_to_update_checkpoint:
        checkpoint_config["runtime_configuration"] = dict_to_update_checkpoint

    context: DataContext = data_context
    context.add_expectation_suite(expectation_suite_name="metrics_exp")
    animals_suite = context.get_expectation_suite(expectation_suite_name="metrics_exp")
    for expectation in expectations_list:
        animals_suite.add_expectation(expectation_configuration=expectation)
    animals_suite.expectation_suite_name = "metrics_exp"
    context.add_or_update_expectation_suite(
        expectation_suite=animals_suite,
    )
    checkpoint_config = CheckpointConfig(**checkpoint_config)
    context.add_checkpoint(
        **filter_properties_dict(
            properties=checkpoint_config.to_json_dict(),
            clean_falsy=True,
        ),
    )
    # noinspection PyProtectedMember
    context._save_project_config()
    return context


@pytest.mark.filesystem
@pytest.mark.integration
def test_sql_result_format_in_checkpoint_pk_defined_one_expectation_complete_output(
    data_context_with_connection_to_metrics_db: FileDataContext,
    reference_sql_checkpoint_config_for_animal_names_table: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
    expected_sql_query_output: str,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`
        - 1 Expectations added to suite
    """

    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        }
    }

    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_metrics_db,
        checkpoint_config=reference_sql_checkpoint_config_for_animal_names_table,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output

    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output

    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    assert unexpected_index_query == expected_sql_query_output


@pytest.mark.filesystem
@pytest.mark.integration
def test_sql_result_format_in_checkpoint_pk_defined_one_expectation_complete_output_with_query(
    data_context_with_connection_to_metrics_db: FileDataContext,
    reference_sql_checkpoint_config_for_animal_names_table: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
    expected_sql_query_output: str,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`
        - 1 Expectations added to suite
        - return_unexpected_index_query flag set to True
    """

    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
            "return_unexpected_index_query": True,
        }
    }

    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_metrics_db,
        checkpoint_config=reference_sql_checkpoint_config_for_animal_names_table,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output

    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output

    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    assert unexpected_index_query == expected_sql_query_output


@pytest.mark.filesystem
@pytest.mark.integration
def test_sql_result_format_in_checkpoint_pk_defined_column_pair_expectation_complete_output_with_query(
    data_context_with_connection_to_metrics_db: FileDataContext,
    reference_sql_checkpoint_config_for_column_pairs_table: dict,
    expectation_config_expect_column_pair_values_to_be_equal: ExpectationConfiguration,
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        }
    }

    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_metrics_db,
        checkpoint_config=reference_sql_checkpoint_config_for_column_pairs_table,
        expectations_list=[expectation_config_expect_column_pair_values_to_be_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == [
        {"pk_1": 3, "ordered_item": "eraser", "received_item": "desk"},
        {"pk_1": 4, "ordered_item": "eraser", "received_item": "desk"},
        {"pk_1": 5, "ordered_item": "eraser", "received_item": "desk"},
    ]

    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == [
        {"pk_1": 3, "ordered_item": "eraser", "received_item": "desk"},
        {"pk_1": 4, "ordered_item": "eraser", "received_item": "desk"},
        {"pk_1": 5, "ordered_item": "eraser", "received_item": "desk"},
    ]

    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    assert (
        unexpected_index_query
        == "SELECT pk_1, ordered_item, received_item \nFROM column_pairs \nWHERE NOT (ordered_item = received_item AND NOT (ordered_item IS NULL OR received_item IS NULL));"
    )


@pytest.mark.filesystem
@pytest.mark.integration
def test_sql_result_format_in_checkpoint_pk_defined_column_pair_expectation_summary_output(
    data_context_with_connection_to_metrics_db: FileDataContext,
    reference_sql_checkpoint_config_for_column_pairs_table: dict,
    expectation_config_expect_column_pair_values_to_be_equal: ExpectationConfiguration,
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "SUMMARY",
            "unexpected_index_column_names": ["pk_1"],
        }
    }

    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_metrics_db,
        checkpoint_config=reference_sql_checkpoint_config_for_column_pairs_table,
        expectations_list=[expectation_config_expect_column_pair_values_to_be_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list

    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == [
        {"pk_1": 3, "ordered_item": "eraser", "received_item": "desk"},
        {"pk_1": 4, "ordered_item": "eraser", "received_item": "desk"},
        {"pk_1": 5, "ordered_item": "eraser", "received_item": "desk"},
    ]

    unexpected_index_query: str = evrs[0]["results"][0]["result"].get(
        "unexpected_index_query"
    )
    assert not unexpected_index_query


@pytest.mark.filesystem
@pytest.mark.integration
def test_sql_result_format_in_checkpoint_pk_defined_multi_column_sum_expectation_complete_output_with_query(
    data_context_with_connection_to_metrics_db: FileDataContext,
    reference_sql_checkpoint_config_for_multi_column_sum_table: dict,
    expectation_config_expect_multicolumn_sum_to_equal: ExpectationConfiguration,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`
        - 1 Expectations added to suite
        - return_unexpected_index_query flag set to True
    """

    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
            "return_unexpected_index_query": True,
        }
    }

    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_metrics_db,
        checkpoint_config=reference_sql_checkpoint_config_for_multi_column_sum_table,
        expectations_list=[expectation_config_expect_multicolumn_sum_to_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == [
        {"pk_1": 1, "a": 20, "b": 20, "c": 20},
        {"pk_1": 2, "a": 30, "b": 30, "c": 30},
        {"pk_1": 3, "a": 40, "b": 40, "c": 40},
        {"pk_1": 4, "a": 50, "b": 50, "c": 50},
        {"pk_1": 5, "a": 60, "b": 60, "c": 60},
    ]

    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == [
        {"pk_1": 1, "a": 20, "b": 20, "c": 20},
        {"pk_1": 2, "a": 30, "b": 30, "c": 30},
        {"pk_1": 3, "a": 40, "b": 40, "c": 40},
        {"pk_1": 4, "a": 50, "b": 50, "c": 50},
        {"pk_1": 5, "a": 60, "b": 60, "c": 60},
    ]

    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    assert (
        unexpected_index_query
        == "SELECT pk_1, a, b, c \nFROM multi_column_sums \nWHERE 0 + a + b + c != 30;"
    )


@pytest.mark.filesystem
@pytest.mark.integration
def test_sql_result_format_in_checkpoint_pk_defined_multi_column_sum_expectation_summary_output(
    data_context_with_connection_to_metrics_db: FileDataContext,
    reference_sql_checkpoint_config_for_multi_column_sum_table: dict,
    expectation_config_expect_multicolumn_sum_to_equal: ExpectationConfiguration,
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "SUMMARY",
            "unexpected_index_column_names": ["pk_1"],
        }
    }

    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_metrics_db,
        checkpoint_config=reference_sql_checkpoint_config_for_multi_column_sum_table,
        expectations_list=[expectation_config_expect_multicolumn_sum_to_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list

    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == [
        {"pk_1": 1, "a": 20, "b": 20, "c": 20},
        {"pk_1": 2, "a": 30, "b": 30, "c": 30},
        {"pk_1": 3, "a": 40, "b": 40, "c": 40},
        {"pk_1": 4, "a": 50, "b": 50, "c": 50},
        {"pk_1": 5, "a": 60, "b": 60, "c": 60},
    ]
    unexpected_index_query: str = evrs[0]["results"][0]["result"].get(
        "unexpected_index_query"
    )
    assert not unexpected_index_query


@pytest.mark.filesystem
@pytest.mark.integration
def test_sql_result_format_in_checkpoint_pk_defined_one_expectation_complete_output_no_query(
    data_context_with_connection_to_metrics_db: FileDataContext,
    reference_sql_checkpoint_config_for_animal_names_table: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`
        - 1 Expectations added to suite
        - return_unexpected_index_query flag set to False
    """

    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
            "return_unexpected_index_query": False,
        }
    }

    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_metrics_db,
        checkpoint_config=reference_sql_checkpoint_config_for_animal_names_table,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output

    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output

    assert evrs[0]["results"][0]["result"].get("unexpected_index_query") is None


@pytest.mark.filesystem
@pytest.mark.integration
def test_sql_result_format_not_in_checkpoint_passed_into_run_checkpoint_one_expectation_complete_output(
    data_context_with_connection_to_metrics_db: FileDataContext,
    reference_sql_checkpoint_config_for_animal_names_table: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
    expected_sql_query_output: str,
):
    """
    What does this test?
        - unexpected_index_column not defined in Checkpoint config, but passed in at run_checkpoint.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`
        - 1 Expectations added to suite
    """
    # intentionally empty, since we are updating at run_checkpoint()
    dict_to_update_checkpoint: dict = {}
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_metrics_db,
        checkpoint_config=reference_sql_checkpoint_config_for_animal_names_table,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )
    result_format: dict = {
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["pk_1"],
    }
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint", result_format=result_format
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output
    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    assert unexpected_index_query == expected_sql_query_output


@pytest.mark.filesystem
@pytest.mark.integration
def test_sql_result_format_not_in_checkpoint_passed_into_run_checkpoint_one_expectation_complete_output_limit_1(
    data_context_with_connection_to_metrics_db: FileDataContext,
    reference_sql_checkpoint_config_for_animal_names_table: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_sql_query_output: str,
):
    """
    What does this test?
        - unexpected_index_column not defined in Checkpoint config, but passed in at run_checkpoint.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`
        - 1 Expectations added to suite
    """
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_metrics_db,
        checkpoint_config=reference_sql_checkpoint_config_for_animal_names_table,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
    )
    result_format: dict = {
        "result_format": "COMPLETE",
        "partial_unexpected_count": 1,
        "unexpected_index_column_names": ["pk_1"],
    }
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint", result_format=result_format
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == [{"animals": "giraffe", "pk_1": 3}]
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == [{"animals": "giraffe", "pk_1": 3}]
    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    assert unexpected_index_query == expected_sql_query_output


@pytest.mark.filesystem
@pytest.mark.integration
def test_sql_result_format_not_in_checkpoint_passed_into_run_checkpoint_one_expectation_complete_output_incorrect_column(
    data_context_with_connection_to_metrics_db: FileDataContext,
    reference_sql_checkpoint_config_for_animal_names_table: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
):
    """
    What does this test?
        - unexpected_index_column not defined in Checkpoint config, but passed in at run_checkpoint.
        - unexpected_index_column is passed in an incorrect column
    """
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_metrics_db,
        checkpoint_config=reference_sql_checkpoint_config_for_animal_names_table,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
    )

    result_format: dict = {
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["i_dont_exist"],
    }
    with pytest.raises(CheckpointError) as e:
        context.run_checkpoint(
            checkpoint_name="my_checkpoint",
            result_format=result_format,
            runtime_configuration={"catch_exceptions": False},
        )

    assert e.value.message == (
        "Exception occurred while running validation[0] of Checkpoint "
        "'my_checkpoint': Error: The unexpected_index_column: \"i_dont_exist\" in "
        "does not exist in SQL Table. Please check your configuration and try again.."
    )


@pytest.mark.filesystem
@pytest.mark.integration
def test_sql_result_format_in_checkpoint_pk_defined_two_expectation_complete_output(
    data_context_with_connection_to_metrics_db: FileDataContext,
    reference_sql_checkpoint_config_for_animal_names_table: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expectation_config_expect_column_values_to_not_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
    expected_sql_query_output: str,
):
    """
    What does this test?
        - unexpected_index_column not defined in Checkpoint config, but passed in at run_checkpoint.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`
        - 2 Expectations added to suite
    """
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_metrics_db,
        checkpoint_config=reference_sql_checkpoint_config_for_animal_names_table,
        expectations_list=[
            expectation_config_expect_column_values_to_be_in_set,
            expectation_config_expect_column_values_to_not_be_in_set,
        ],
    )
    result_format: dict = {
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["pk_1"],
    }

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint", result_format=result_format
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    # first and second expectations have same results. Although one is "expect_to_be"
    # and the other is "expect_to_not_be", they have opposite value_sets
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output

    second_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][1]["result"][
        "unexpected_index_list"
    ]
    assert second_result_full_list == expected_unexpected_indices_output
    second_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][1]["result"][
        "partial_unexpected_index_list"
    ]
    assert second_result_partial_list == expected_unexpected_indices_output
    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    assert unexpected_index_query == expected_sql_query_output


@pytest.mark.filesystem
@pytest.mark.integration
def test_sql_result_format_in_checkpoint_pk_defined_one_expectation_summary_output(
    data_context_with_connection_to_metrics_db: FileDataContext,
    reference_sql_checkpoint_config_for_animal_names_table: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - SUMMARY output, which means we have `partial_unexpected_index_list` only
        - 1 Expectations added to suite
    """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "SUMMARY",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_metrics_db,
        checkpoint_config=reference_sql_checkpoint_config_for_animal_names_table,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output


@pytest.mark.filesystem
@pytest.mark.integration
def test_sql_result_format_in_checkpoint_pk_defined_one_expectation_basic_output(
    data_context_with_connection_to_metrics_db: FileDataContext,
    reference_sql_checkpoint_config_for_animal_names_table: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - BASIC output, which means we have no unexpected_index_list output
        - 1 Expectations added to suite
    """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "BASIC",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_metrics_db,
        checkpoint_config=reference_sql_checkpoint_config_for_animal_names_table,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list
    first_result_partial_list = evrs[0]["results"][0]["result"].get(
        "partial_unexpected_index_list"
    )
    assert not first_result_partial_list

    assert evrs[0]["results"][0]["result"].get("unexpected_index_query") is None


@pytest.mark.filesystem
@pytest.mark.integration
def test_sql_complete_output_no_id_pk_fallback(
    data_context_with_connection_to_metrics_db: FileDataContext,
    reference_sql_checkpoint_config_for_animal_names_table: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_metrics_db,
        checkpoint_config=reference_sql_checkpoint_config_for_animal_names_table,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert not index_column_names

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list
    first_result_partial_list = evrs[0]["results"][0]["result"].get(
        "partial_unexpected_index_list"
    )
    assert not first_result_partial_list

    unexpected_index_query = evrs[0]["results"][0]["result"].get(
        "unexpected_index_query"
    )
    # query does not contain id_pk column
    assert (
        unexpected_index_query
        == "SELECT animals \nFROM animal_names \nWHERE animals IS NOT NULL AND (animals NOT IN ('cat', 'fish', 'dog'));"
    )


# pandas
@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_pk_defined_one_expectation_complete_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """ """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_pandas_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output
    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    # result_format is COMPLETE
    assert unexpected_index_query == "df.filter(items=[3, 4, 5], axis=0)"


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_pk_defined_one_expectation_complete_output_with_query(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """ """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
            "return_unexpected_index_query": True,
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_pandas_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output
    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    # result_format is COMPLETE
    assert unexpected_index_query == "df.filter(items=[3, 4, 5], axis=0)"


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_pk_defined_one_expectation_complete_output_no_query(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """ """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
            "return_unexpected_index_query": False,
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_pandas_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output
    # return_unexpected_index_query is set to False
    assert evrs[0]["results"][0]["result"].get("unexpected_index_query") is None


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_pk_defined_one_expectation_complete_output_partial_unexpected_count_1(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """ """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
            "partial_unexpected_count": 1,
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_pandas_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    # this is just 1 because we set `partial_unexpected_count` above
    assert first_result_partial_list == [{"animals": "giraffe", "pk_1": 3}]
    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    # result_format is COMPLETE
    assert unexpected_index_query == [3, 4, 5]


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_not_in_checkpoint_passed_into_run_checkpoint_one_expectation_complete_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
    )
    result_format: dict = {
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["pk_1"],
    }
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        result_format=result_format,
        batch_request=batch_request_for_pandas_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output
    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    # result_format is COMPLETE
    assert unexpected_index_query == "df.filter(items=[3, 4, 5], axis=0)"


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_not_in_checkpoint_passed_into_run_checkpoint_one_expectation_summary_output_limit_1(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
):
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
    )
    result_format: dict = {
        "result_format": "SUMMARY",
        "partial_unexpected_count": 1,
        "unexpected_index_column_names": ["pk_1"],
    }
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        result_format=result_format,
        batch_request=batch_request_for_pandas_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == [{"animals": "giraffe", "pk_1": 3}]
    # result_format is SUMMARY
    assert evrs[0]["results"][0]["result"].get("unexpected_index_query") is None


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_not_in_checkpoint_passed_into_run_checkpoint_one_expectation_complete_output_incorrect_column(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["i_dont_exist"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )
    with pytest.raises(CheckpointError) as e:
        context.run_checkpoint(
            checkpoint_name="my_checkpoint",
            expectation_suite_name="metrics_exp",
            batch_request=batch_request_for_pandas_unexpected_rows_and_index,
            runtime_configuration={"catch_exceptions": False},
        )
    assert e.value.message == (
        "Exception occurred while running validation[0] of Checkpoint "
        "'my_checkpoint': Error: The unexpected_index_column \"i_dont_exist\" does "
        "not exist in Dataframe. Please check your configuration and try again.."
    )


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_pk_defined_two_expectation_complete_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expectation_config_expect_column_values_to_not_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[
            expectation_config_expect_column_values_to_be_in_set,
            expectation_config_expect_column_values_to_not_be_in_set,
        ],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_pandas_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    # first and second expectations have same results. Although one is "expect_to_be"
    # and the other is "expect_to_not_be", they have opposite value_sets
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output
    first_result_unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    assert first_result_unexpected_index_query == "df.filter(items=[3, 4, 5], axis=0)"

    second_result_full_list = evrs[0]["results"][1]["result"]["unexpected_index_list"]
    assert second_result_full_list == expected_unexpected_indices_output
    second_result_partial_list = evrs[0]["results"][1]["result"][
        "partial_unexpected_index_list"
    ]
    assert second_result_partial_list == expected_unexpected_indices_output
    second_result_unexpected_index_query: str = evrs[0]["results"][1]["result"][
        "unexpected_index_query"
    ]
    assert second_result_unexpected_index_query == "df.filter(items=[3, 4, 5], axis=0)"


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_pk_defined_one_expectation_summary_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "SUMMARY",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_pandas_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert first_result_partial_list == expected_unexpected_indices_output
    first_result_partial_list = evrs[0]["results"][0]["result"].get(
        "partial_unexpected_index_list"
    )
    assert not first_result_partial_list
    # result_format is SUMMARY
    assert evrs[0]["results"][0]["result"].get("unexpected_index_query") is None


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_not_in_checkpoint_passed_into_run_checkpoint_one_expectation_complete_output(  # noqa: F811 # TODO: review test for duplication
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
    )
    result_format: dict = {
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["pk_1"],
    }
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        result_format=result_format,
        batch_request=batch_request_for_pandas_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    first_result_full_list = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output
    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    # result_format is SUMMARY
    assert unexpected_index_query == "df.filter(items=[3, 4, 5], axis=0)"


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_not_in_checkpoint_passed_into_run_checkpoint_one_expectation_summary_output_limit_1(  # noqa: F811 # TODO: review test for duplication
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
):
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
    )
    result_format: dict = {
        "result_format": "SUMMARY",
        "partial_unexpected_count": 1,
        "unexpected_index_column_names": ["pk_1"],
    }
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        result_format=result_format,
        batch_request=batch_request_for_pandas_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    first_result_partial_list = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == [{"animals": "giraffe", "pk_1": 3}]
    # result_format is SUMMARY
    assert evrs[0]["results"][0]["result"].get("unexpected_index_query") is None


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_not_in_checkpoint_passed_into_run_checkpoint_one_expectation_complete_output_incorrect_column(  # noqa: F811 # TODO: review test for duplication
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["i_dont_exist"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )
    with pytest.raises(CheckpointError) as e:
        context.run_checkpoint(
            checkpoint_name="my_checkpoint",
            expectation_suite_name="metrics_exp",
            batch_request=batch_request_for_pandas_unexpected_rows_and_index,
            runtime_configuration={"catch_exceptions": False},
        )
    assert e.value.message == (
        "Exception occurred while running validation[0] of Checkpoint "
        "'my_checkpoint': Error: The unexpected_index_column \"i_dont_exist\" does "
        "not exist in Dataframe. Please check your configuration and try again.."
    )


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_pk_defined_two_expectation_complete_output(  # noqa: F811 # TODO: review test for duplication
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expectation_config_expect_column_values_to_not_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[
            expectation_config_expect_column_values_to_be_in_set,
            expectation_config_expect_column_values_to_not_be_in_set,
        ],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_pandas_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    # first and second expectations have same results. Although one is "expect_to_be"
    # and the other is "expect_to_not_be", they have opposite value_sets
    first_result_full_list = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output
    first_result_unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    assert first_result_unexpected_index_query == "df.filter(items=[3, 4, 5], axis=0)"
    second_result_full_list = evrs[0]["results"][1]["result"]["unexpected_index_list"]
    assert second_result_full_list == expected_unexpected_indices_output
    second_result_partial_list = evrs[0]["results"][1]["result"][
        "partial_unexpected_index_list"
    ]
    assert second_result_partial_list == expected_unexpected_indices_output
    second_result_unexpected_index_query: str = evrs[0]["results"][1]["result"][
        "unexpected_index_query"
    ]
    # result_format is COMPLETE
    assert second_result_unexpected_index_query == "df.filter(items=[3, 4, 5], axis=0)"


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_pk_defined_one_expectation_summary_output(  # noqa: F811 # TODO: review test for duplication
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "SUMMARY",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_pandas_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    first_result_full_list = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list
    first_result_partial_list = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output
    # result_format is SUMMARY
    assert evrs[0]["results"][0]["result"].get("unexpected_index_query") is None


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_pk_defined_one_expectation_basic_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "BASIC",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_pandas_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    first_result_full_list = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list
    first_result_partial_list = evrs[0]["results"][0]["result"].get(
        "partial_unexpected_index_list"
    )
    assert not first_result_partial_list
    # result_format is BASIC
    assert evrs[0]["results"][0]["result"].get("unexpected_index_query") is None


# spark
@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_result_format_in_checkpoint_pk_defined_one_expectation_complete_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
    expected_spark_query_output: str,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`
        - 1 Expectations added to suite
    """

    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_spark_unexpected_rows_and_index,
        runtime_configuration={"catch_exceptions": False},
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    first_result_full_list = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output
    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    assert unexpected_index_query == expected_spark_query_output


@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_result_format_not_in_checkpoint_passed_into_run_checkpoint_one_expectation_complete_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
    expected_spark_query_output: str,
):
    """
    What does this test?
        - unexpected_index_column not defined in Checkpoint config, but passed in at run_checkpoint.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`
        - 1 Expectations added to suite
    """
    # intentionally empty, since we are updating at run_checkpoint()
    dict_to_update_checkpoint: dict = {}
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )
    result_format: dict = {
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["pk_1"],
    }
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_spark_unexpected_rows_and_index,
        result_format=result_format,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output
    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    assert unexpected_index_query == expected_spark_query_output


@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_result_format_not_in_checkpoint_passed_into_run_checkpoint_one_expectation_complete_output_with_query(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
    expected_spark_query_output: str,
):
    """
    What does this test?
        - unexpected_index_column not defined in Checkpoint config, but passed in at run_checkpoint.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`
        - 1 Expectations added to suite
        - return_unexpected_index_query set to True
    """
    # intentionally empty, since we are updating at run_checkpoint()
    dict_to_update_checkpoint: dict = {}
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )
    result_format: dict = {
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["pk_1"],
        "return_unexpected_index_query": True,
    }
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_spark_unexpected_rows_and_index,
        result_format=result_format,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output

    unexpected_index_query = evrs[0]["results"][0]["result"].get(
        "unexpected_index_query"
    )
    assert unexpected_index_query == expected_spark_query_output


@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_result_format_not_in_checkpoint_passed_into_run_checkpoint_one_expectation_complete_output_no_query(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """
    What does this test?
        - unexpected_index_column not defined in Checkpoint config, but passed in at run_checkpoint.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`
        - 1 Expectations added to suite
        - return_unexpected_index_query set to False
    """
    # intentionally empty, since we are updating at run_checkpoint()
    dict_to_update_checkpoint: dict = {}
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )
    result_format: dict = {
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["pk_1"],
        "return_unexpected_index_query": False,
    }
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_spark_unexpected_rows_and_index,
        result_format=result_format,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output
    assert evrs[0]["results"][0]["result"].get("unexpected_index_query") is None


@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_result_format_not_in_checkpoint_passed_into_run_checkpoint_one_expectation_complete_output_incorrect_column(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
):
    """
    What does this test?
       - unexpected_index_column not defined in Checkpoint config, but passed in at run_checkpoint.
       - unexpected_index_column is passed in an incorrect column
    """
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
    )

    result_format: dict = {
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["i_dont_exist"],
    }
    with pytest.raises(CheckpointError) as e:
        context.run_checkpoint(
            checkpoint_name="my_checkpoint",
            expectation_suite_name="metrics_exp",
            batch_request=batch_request_for_spark_unexpected_rows_and_index,
            result_format=result_format,
            runtime_configuration={"catch_exceptions": False},
        )

    assert e.value.message == (
        "Exception occurred while running validation[0] of Checkpoint "
        "'my_checkpoint': Error: The unexpected_index_column 'i_dont_exist' does not "
        "exist in Spark DataFrame. Please check your configuration and try again.."
    )


@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_result_format_in_checkpoint_pk_defined_two_expectation_complete_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expectation_config_expect_column_values_to_not_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
    expected_spark_query_output: str,
):
    """
    What does this test?
        - unexpected_index_column not defined in Checkpoint config, but passed in at run_checkpoint.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`
        - 2 Expectations added to suite
    """
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[
            expectation_config_expect_column_values_to_be_in_set,
            expectation_config_expect_column_values_to_not_be_in_set,
        ],
    )
    result_format: dict = {
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["pk_1"],
    }

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_spark_unexpected_rows_and_index,
        result_format=result_format,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    # first and second expectations have same results. Although one is "expect_to_be"
    # and the other is "expect_to_not_be", they have opposite value_sets
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output

    second_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][1]["result"][
        "unexpected_index_list"
    ]
    assert second_result_full_list == expected_unexpected_indices_output

    second_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][1]["result"][
        "partial_unexpected_index_list"
    ]
    assert second_result_partial_list == expected_unexpected_indices_output
    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    assert unexpected_index_query == expected_spark_query_output


@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_result_format_in_checkpoint_pk_defined_one_expectation_summary_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - SUMMARY output, which means we have `partial_unexpected_index_list` only
        - 1 Expectations added to suite
    """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "SUMMARY",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_spark_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == expected_unexpected_indices_output
    assert evrs[0]["results"][0]["result"].get("unexpected_index_query") is None


@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_result_format_in_checkpoint_pk_defined_one_expectation_summary_output_limit_1(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - SUMMARY output, which means we have `partial_unexpected_index_list` only
        - 1 Expectations added to suite
        - limit is 1 so we only get 1 output in the `partial_unexpected_index_list`
    """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "SUMMARY",
            "partial_unexpected_count": 1,
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_spark_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == [
        {
            "animals": "giraffe",
            "pk_1": 3,
        }
    ]
    assert evrs[0]["results"][0]["result"].get("unexpected_index_query") is None


@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_result_format_in_checkpoint_pk_defined_one_expectation_basic_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - BASIC output, which means we have no unexpected_index_list output
        - 1 Expectations added to suite
    """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "BASIC",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_spark_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list
    first_result_partial_list = evrs[0]["results"][0]["result"].get(
        "partial_unexpected_index_list"
    )
    assert not first_result_partial_list
    assert evrs[0]["results"][0]["result"].get("unexpected_index_query") is None


@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_result_format_in_checkpoint_one_column_pair_expectation_complete_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index_column_pair: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_pair_values_to_be_equal: ExpectationConfiguration,
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_pair_values_to_be_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_spark_unexpected_rows_and_index_column_pair,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert index_column_names == ["pk_1"]
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert first_result_full_list == [
        {"ordered_item": "eraser", "pk_1": 3, "received_item": "desk"},
        {"ordered_item": "eraser", "pk_1": 4, "received_item": "desk"},
        {"ordered_item": "eraser", "pk_1": 5, "received_item": "desk"},
    ]
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert first_result_partial_list == [
        {"ordered_item": "eraser", "pk_1": 3, "received_item": "desk"},
        {"ordered_item": "eraser", "pk_1": 4, "received_item": "desk"},
        {"ordered_item": "eraser", "pk_1": 5, "received_item": "desk"},
    ]
    unexpected_index_query: List[int] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_query"
    )
    assert (
        unexpected_index_query
        == "df.filter(F.expr(NOT (ordered_item <=> received_item)))"
    )


@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_result_format_in_checkpoint_one_column_pair_expectation_summary_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index_column_pair: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_pair_values_to_be_equal: ExpectationConfiguration,
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "SUMMARY",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_pair_values_to_be_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_spark_unexpected_rows_and_index_column_pair,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert index_column_names == ["pk_1"]
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert first_result_full_list is None
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert first_result_partial_list == [
        {"ordered_item": "eraser", "pk_1": 3, "received_item": "desk"},
        {"ordered_item": "eraser", "pk_1": 4, "received_item": "desk"},
        {"ordered_item": "eraser", "pk_1": 5, "received_item": "desk"},
    ]
    unexpected_index_query: List[int] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_query"
    )
    assert unexpected_index_query is None


@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_result_format_in_checkpoint_one_column_pair_expectation_basic_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index_column_pair: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_pair_values_to_be_equal: ExpectationConfiguration,
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "BASIC",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_pair_values_to_be_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_spark_unexpected_rows_and_index_column_pair,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert index_column_names == ["pk_1"]
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list

    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert not first_result_partial_list

    unexpected_index_query: List[int] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_query"
    )
    assert not unexpected_index_query


@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_result_format_in_checkpoint_one_multicolumn_map_expectation_complete_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index_multicolumn_sum: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_multicolumn_sum_to_equal: ExpectationConfiguration,
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_multicolumn_sum_to_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_spark_unexpected_rows_and_index_multicolumn_sum,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert index_column_names == ["pk_1"]
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert first_result_full_list == [
        {"a": 20, "b": 20, "c": 20, "pk_1": 1},
        {"a": 30, "b": 30, "c": 30, "pk_1": 2},
        {"a": 40, "b": 40, "c": 40, "pk_1": 3},
        {"a": 50, "b": 50, "c": 50, "pk_1": 4},
        {"a": 60, "b": 60, "c": 60, "pk_1": 5},
    ]
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert first_result_partial_list == [
        {"a": 20, "b": 20, "c": 20, "pk_1": 1},
        {"a": 30, "b": 30, "c": 30, "pk_1": 2},
        {"a": 40, "b": 40, "c": 40, "pk_1": 3},
        {"a": 50, "b": 50, "c": 50, "pk_1": 4},
        {"a": 60, "b": 60, "c": 60, "pk_1": 5},
    ]

    unexpected_index_query: List[int] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_query"
    )
    assert unexpected_index_query == (
        "df.filter(F.expr(NOT (((COALESCE(a, 0) + COALESCE(b, 0)) + COALESCE(c, 0)) = "
        "30)))"
    )


@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_result_format_in_checkpoint_one_multicolumn_map_expectation_summary_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index_multicolumn_sum: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_multicolumn_sum_to_equal: ExpectationConfiguration,
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "SUMMARY",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_multicolumn_sum_to_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_spark_unexpected_rows_and_index_multicolumn_sum,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert index_column_names == ["pk_1"]
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert first_result_full_list is None

    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert first_result_partial_list == [
        {"a": 20, "b": 20, "c": 20, "pk_1": 1},
        {"a": 30, "b": 30, "c": 30, "pk_1": 2},
        {"a": 40, "b": 40, "c": 40, "pk_1": 3},
        {"a": 50, "b": 50, "c": 50, "pk_1": 4},
        {"a": 60, "b": 60, "c": 60, "pk_1": 5},
    ]


@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_result_format_in_checkpoint_one_multicolumn_map_expectation_basic_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index_multicolumn_sum: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_multicolumn_sum_to_equal: ExpectationConfiguration,
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "BASIC",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_multicolumn_sum_to_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_spark_unexpected_rows_and_index_multicolumn_sum,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert index_column_names == ["pk_1"]
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert not first_result_partial_list
    unexpected_index_query: List[int] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_query"
    )
    assert not unexpected_index_query


@pytest.mark.filesystem
@pytest.mark.integration
def test_spark_complete_output_no_id_pk_fallback(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_spark_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_spark_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert not index_column_names
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert not first_result_partial_list
    unexpected_index_query: List[int] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_query"
    )
    assert (
        unexpected_index_query
        == "df.filter(F.expr((animals IS NOT NULL) AND (NOT (animals IN (cat, fish, dog)))))"
    )


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_pk_defined_one_expectation_complete_output_partial_unexpected_count_1(  # noqa: F811 # TODO: review test for duplication
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """ """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
            "partial_unexpected_count": 1,
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_pandas_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == expected_unexpected_indices_output
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    # this is just 1 because we set `partial_unexpected_count` above
    assert first_result_partial_list == [{"animals": "giraffe", "pk_1": 3}]


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_named_index_one_index_column(
    in_memory_runtime_context: AbstractDataContext,
    pandas_animals_dataframe_for_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """
    What does this test?
        - DataFrame being passed into Checkpoint has named index 'pk_1', which correspond to unexpected_index_column_names
        - MapMatric calculation happens the same as if `pk_1` was a non-index column
    """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_2"],
        }
    }
    # build our own BatchRequest
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    # setting named index
    updated_dataframe: pd.DataFrame = dataframe.set_index("pk_2")

    batch_request: dict = {
        "datasource_name": "pandas_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": updated_dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_2"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert first_result_full_list == [
        {"animals": "giraffe", "pk_2": "three"},
        {"animals": "lion", "pk_2": "four"},
        {"animals": "zebra", "pk_2": "five"},
    ]
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert first_result_partial_list == [
        {"animals": "giraffe", "pk_2": "three"},
        {"animals": "lion", "pk_2": "four"},
        {"animals": "zebra", "pk_2": "five"},
    ]


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_named_index_one_index_column_wrong_column(
    in_memory_runtime_context: AbstractDataContext,
    pandas_animals_dataframe_for_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """
    What does this test?
        - DataFrame being passed into Checkpoint has named index 'pk_1', which doesn't match the unexpected_index_column_name
    """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_2"],  # I am not the index column
        }
    }
    # build our own BatchRequest
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    # setting named index
    updated_dataframe: pd.DataFrame = dataframe.set_index("pk_1")

    batch_request: dict = {
        "datasource_name": "pandas_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": updated_dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )
    with pytest.raises(CheckpointError) as e:
        context.run_checkpoint(
            checkpoint_name="my_checkpoint",
            expectation_suite_name="metrics_exp",
            batch_request=batch_request,
            runtime_configuration={"catch_exceptions": False},
        )
    assert e.value.message == (
        "Exception occurred while running validation[0] of Checkpoint "
        "'my_checkpoint': Error: The column pk_2 does not exist in the named indices. "
        "Please check your configuration."
    )


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_named_index_two_index_column(
    in_memory_runtime_context: AbstractDataContext,
    pandas_animals_dataframe_for_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """
    What does this test?
        - DataFrame being passed into Checkpoint has two named indices, which correspond to unexpected_index_column_names
        - MapMatric calculation happens the same as if `pk_1` and `pk_2` were non-index columns
    """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1", "pk_2"],
        }
    }
    # build our own BatchRequest
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    # setting named index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1", "pk_2"])

    batch_request: dict = {
        "datasource_name": "pandas_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": updated_dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"][
        "unexpected_index_column_names"
    ]
    assert index_column_names == ["pk_1", "pk_2"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert first_result_full_list == [
        {"animals": "giraffe", "pk_1": 3, "pk_2": "three"},
        {"animals": "lion", "pk_1": 4, "pk_2": "four"},
        {"animals": "zebra", "pk_1": 5, "pk_2": "five"},
    ]
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")

    assert first_result_partial_list == [
        {"animals": "giraffe", "pk_1": 3, "pk_2": "three"},
        {"animals": "lion", "pk_1": 4, "pk_2": "four"},
        {"animals": "zebra", "pk_1": 5, "pk_2": "five"},
    ]


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_named_index_two_index_column_not_set(
    in_memory_runtime_context: AbstractDataContext,
    pandas_animals_dataframe_for_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """
    What does this test?
        - DataFrame being passed into Checkpoint has two named indices, which correspond to unexpected_index_column_names
        - MapMatric calculation happens the same as if `pk_1` and `pk_2` were non-index columns
    """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
        }
    }
    # build our own BatchRequest
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    # setting named index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_2"])

    batch_request: dict = {
        "datasource_name": "pandas_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": updated_dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert not index_column_names

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert first_result_full_list == [("three"), ("four"), ("five")]
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert first_result_partial_list == [("three"), ("four"), ("five")]


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_named_index_two_index_column_not_set(  # noqa: F811 # TODO: review test for duplication
    in_memory_runtime_context: AbstractDataContext,
    pandas_animals_dataframe_for_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """
    What does this test?
        - DataFrame being passed into Checkpoint has two named indices, which correspond to unexpected_index_column_names
        - MapMatric calculation happens the same as if `pk_1` and `pk_2` were non-index columns
    """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
        }
    }
    # build our own BatchRequest
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1", "pk_2"])

    batch_request: dict = {
        "datasource_name": "pandas_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": updated_dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert not index_column_names

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert first_result_full_list == [
        {"animals": "giraffe", "pk_1": 3, "pk_2": "three"},
        {"animals": "lion", "pk_1": 4, "pk_2": "four"},
        {"animals": "zebra", "pk_1": 5, "pk_2": "five"},
    ]
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert first_result_partial_list == [
        {"animals": "giraffe", "pk_1": 3, "pk_2": "three"},
        {"animals": "lion", "pk_1": 4, "pk_2": "four"},
        {"animals": "zebra", "pk_1": 5, "pk_2": "five"},
    ]


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_named_index_different_column_specified_in_result_format(
    in_memory_runtime_context: AbstractDataContext,
    pandas_animals_dataframe_for_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """
    What does this test?
        - DataFrame being passed into Checkpoint has a different named index than then column passed in
        - we also pass in `pk_1`  as unexpected_index_column_names
        - but pk_2 is the actual index
    """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    # build our own BatchRequest
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    # setting named index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_2"])

    batch_request: dict = {
        "datasource_name": "pandas_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": updated_dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    with pytest.raises(CheckpointError) as e:
        context.run_checkpoint(
            checkpoint_name="my_checkpoint",
            expectation_suite_name="metrics_exp",
            batch_request=batch_request,
            runtime_configuration={"catch_exceptions": False},
        )
    assert e.value.message == (
        "Exception occurred while running validation[0] of Checkpoint "
        "'my_checkpoint': Error: The column pk_1 does not exist in the named indices. "
        "Please check your configuration."
    )


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_named_index_two_index_column_set(
    in_memory_runtime_context: AbstractDataContext,
    pandas_animals_dataframe_for_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """
    What does this test?
        - DataFrame being passed into Checkpoint has two named indices, which correspond to unexpected_index_column_names
        - MapMetric calculation happens the same as if `pk_1` and `pk_2` were non-index columns
        - we also pass in `pk_1` and `pk_2`  as unexpected_index_column_names
    """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1", "pk_2"],
        }
    }
    dataframe: pd.DataFrame = pandas_animals_dataframe_for_unexpected_rows_and_index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1", "pk_2"])

    batch_request: dict = {
        "datasource_name": "pandas_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": updated_dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert index_column_names == ["pk_1", "pk_2"]

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert first_result_full_list == [
        {"animals": "giraffe", "pk_1": 3, "pk_2": "three"},
        {"animals": "lion", "pk_1": 4, "pk_2": "four"},
        {"animals": "zebra", "pk_1": 5, "pk_2": "five"},
    ]
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert first_result_partial_list == [
        {"animals": "giraffe", "pk_1": 3, "pk_2": "three"},
        {"animals": "lion", "pk_1": 4, "pk_2": "four"},
        {"animals": "zebra", "pk_1": 5, "pk_2": "five"},
    ]


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_one_expectation_complete_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_values_to_be_in_set: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    """ """
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_pandas_unexpected_rows_and_index,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "unexpected_index_list"
    ]
    assert first_result_full_list == [3, 4, 5]
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == [3, 4, 5]


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_one_column_pair_expectation_complete_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index_column_pair: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_pair_values_to_be_equal: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_pair_values_to_be_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_pandas_unexpected_rows_and_index_column_pair,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert index_column_names == ["pk_1"]
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert first_result_full_list == [
        {"ordered_item": "eraser", "pk_1": 3, "received_item": "desk"},
        {"ordered_item": "eraser", "pk_1": 4, "received_item": "desk"},
        {"ordered_item": "eraser", "pk_1": 5, "received_item": "desk"},
    ]
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert first_result_partial_list == [
        {"ordered_item": "eraser", "pk_1": 3, "received_item": "desk"},
        {"ordered_item": "eraser", "pk_1": 4, "received_item": "desk"},
        {"ordered_item": "eraser", "pk_1": 5, "received_item": "desk"},
    ]
    unexpected_index_query: str = evrs[0]["results"][0]["result"].get(
        "unexpected_index_query"
    )
    assert unexpected_index_query == "df.filter(items=[3, 4, 5], axis=0)"


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_one_column_pair_expectation_complete_output_one_index_column(
    in_memory_runtime_context: AbstractDataContext,
    pandas_column_pairs_dataframe_for_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_pair_values_to_be_equal: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_2"],
        }
    }
    # build our own BatchRequest
    dataframe: pd.DataFrame = (
        pandas_column_pairs_dataframe_for_unexpected_rows_and_index
    )
    # setting named index
    updated_dataframe: pd.DataFrame = dataframe.set_index("pk_2")

    batch_request: dict = {
        "datasource_name": "pandas_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": updated_dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_pair_values_to_be_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert index_column_names == ["pk_2"]
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert first_result_full_list == [
        {"ordered_item": "eraser", "pk_2": "three", "received_item": "desk"},
        {"ordered_item": "eraser", "pk_2": "four", "received_item": "desk"},
        {"ordered_item": "eraser", "pk_2": "five", "received_item": "desk"},
    ]
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert first_result_partial_list == [
        {"ordered_item": "eraser", "pk_2": "three", "received_item": "desk"},
        {"ordered_item": "eraser", "pk_2": "four", "received_item": "desk"},
        {"ordered_item": "eraser", "pk_2": "five", "received_item": "desk"},
    ]
    unexpected_index_query = evrs[0]["results"][0]["result"].get(
        "unexpected_index_query"
    )

    items_str = str(["three", "four", "five"])
    assert unexpected_index_query == f"df.filter(items={items_str}, axis=0)"


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_one_column_pair_expectation_complete_output_two_index_column(
    in_memory_runtime_context: AbstractDataContext,
    pandas_column_pairs_dataframe_for_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_column_pair_values_to_be_equal: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1", "pk_2"],
        }
    }
    # build our own BatchRequest
    dataframe: pd.DataFrame = (
        pandas_column_pairs_dataframe_for_unexpected_rows_and_index
    )
    # setting named index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1", "pk_2"])

    batch_request: dict = {
        "datasource_name": "pandas_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": updated_dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_column_pair_values_to_be_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert index_column_names == ["pk_1", "pk_2"]
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert first_result_full_list == [
        {"ordered_item": "eraser", "pk_1": 3, "pk_2": "three", "received_item": "desk"},
        {"ordered_item": "eraser", "pk_1": 4, "pk_2": "four", "received_item": "desk"},
        {"ordered_item": "eraser", "pk_1": 5, "pk_2": "five", "received_item": "desk"},
    ]
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert first_result_partial_list == [
        {"ordered_item": "eraser", "pk_1": 3, "pk_2": "three", "received_item": "desk"},
        {"ordered_item": "eraser", "pk_1": 4, "pk_2": "four", "received_item": "desk"},
        {"ordered_item": "eraser", "pk_1": 5, "pk_2": "five", "received_item": "desk"},
    ]
    unexpected_index_query: str = evrs[0]["results"][0]["result"].get(
        "unexpected_index_query"
    )
    items_str = str([(3, "three"), (4, "four"), (5, "five")])
    assert unexpected_index_query == f"df.filter(items={items_str}, axis=0)"


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_one_multicolumn_map_expectation_complete_output(
    in_memory_runtime_context: AbstractDataContext,
    batch_request_for_pandas_unexpected_rows_and_index_multicolumn_sum: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_multicolumn_sum_to_equal: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_multicolumn_sum_to_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request_for_pandas_unexpected_rows_and_index_multicolumn_sum,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert index_column_names == ["pk_1"]
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert first_result_full_list == [
        {"a": 20, "b": 20, "c": 20, "pk_1": 1},
        {"a": 30, "b": 30, "c": 30, "pk_1": 2},
        {"a": 40, "b": 40, "c": 40, "pk_1": 3},
        {"a": 50, "b": 50, "c": 50, "pk_1": 4},
        {"a": 60, "b": 60, "c": 60, "pk_1": 5},
    ]
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert first_result_partial_list == [
        {"a": 20, "b": 20, "c": 20, "pk_1": 1},
        {"a": 30, "b": 30, "c": 30, "pk_1": 2},
        {"a": 40, "b": 40, "c": 40, "pk_1": 3},
        {"a": 50, "b": 50, "c": 50, "pk_1": 4},
        {"a": 60, "b": 60, "c": 60, "pk_1": 5},
    ]

    unexpected_index_query: str = evrs[0]["results"][0]["result"][
        "unexpected_index_query"
    ]
    items_str = str([1, 2, 3, 4, 5])
    assert unexpected_index_query == f"df.filter(items={items_str}, axis=0)"


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_one_multicolumn_map_expectation_complete_output_one_index_column(
    in_memory_runtime_context: AbstractDataContext,
    pandas_multicolumn_sum_dataframe_for_unexpected_rows_and_index: dict,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_multicolumn_sum_to_equal: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_2"],
        }
    }
    # build our own BatchRequest
    dataframe: pd.DataFrame = (
        pandas_multicolumn_sum_dataframe_for_unexpected_rows_and_index
    )
    # setting named index
    updated_dataframe: pd.DataFrame = dataframe.set_index("pk_2")

    batch_request: dict = {
        "datasource_name": "pandas_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": updated_dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_multicolumn_sum_to_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert index_column_names == ["pk_2"]
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert first_result_full_list == [
        {"a": 20, "b": 20, "c": 20, "pk_2": "one"},
        {"a": 30, "b": 30, "c": 30, "pk_2": "two"},
        {"a": 40, "b": 40, "c": 40, "pk_2": "three"},
        {"a": 50, "b": 50, "c": 50, "pk_2": "four"},
        {"a": 60, "b": 60, "c": 60, "pk_2": "five"},
    ]
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert first_result_partial_list == [
        {"a": 20, "b": 20, "c": 20, "pk_2": "one"},
        {"a": 30, "b": 30, "c": 30, "pk_2": "two"},
        {"a": 40, "b": 40, "c": 40, "pk_2": "three"},
        {"a": 50, "b": 50, "c": 50, "pk_2": "four"},
        {"a": 60, "b": 60, "c": 60, "pk_2": "five"},
    ]

    unexpected_index_query: str = evrs[0]["results"][0]["result"].get(
        "unexpected_index_query"
    )
    items_str = str(["one", "two", "three", "four", "five"])
    assert unexpected_index_query == f"df.filter(items={items_str}, axis=0)"


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_one_multicolumn_map_expectation_complete_output_two_index_column(
    in_memory_runtime_context: AbstractDataContext,
    pandas_multicolumn_sum_dataframe_for_unexpected_rows_and_index: pd.DataFrame,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    expectation_config_expect_multicolumn_sum_to_equal: ExpectationConfiguration,
    expected_unexpected_indices_output: list[dict[str, str | int]],
):
    dict_to_update_checkpoint: dict = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1", "pk_2"],
        }
    }
    # build our own BatchRequest
    dataframe: pd.DataFrame = (
        pandas_multicolumn_sum_dataframe_for_unexpected_rows_and_index
    )
    # setting named index
    updated_dataframe: pd.DataFrame = dataframe.set_index(["pk_1", "pk_2"])

    batch_request: dict = {
        "datasource_name": "pandas_datasource",
        "data_connector_name": "runtime_data_connector",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
        "runtime_parameters": {
            "batch_data": updated_dataframe,
        },
        "batch_identifiers": {
            "id_key_0": 1234567890,
        },
    }
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=in_memory_runtime_context,
        checkpoint_config=reference_checkpoint_config_for_unexpected_column_names,
        expectations_list=[expectation_config_expect_multicolumn_sum_to_equal],
        dict_to_update_checkpoint=dict_to_update_checkpoint,
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name="metrics_exp",
        batch_request=batch_request,
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    index_column_names: List[str] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_column_names"
    )
    assert index_column_names == ["pk_1", "pk_2"]
    first_result_full_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert first_result_full_list == [
        {"a": 20, "pk_1": 1, "pk_2": "one", "b": 20, "c": 20},
        {"a": 30, "pk_1": 2, "pk_2": "two", "b": 30, "c": 30},
        {"a": 40, "pk_1": 3, "pk_2": "three", "b": 40, "c": 40},
        {"a": 50, "pk_1": 4, "pk_2": "four", "b": 50, "c": 50},
        {"a": 60, "pk_1": 5, "pk_2": "five", "b": 60, "c": 60},
    ]
    first_result_partial_list: List[Dict[str, Any]] = evrs[0]["results"][0][
        "result"
    ].get("partial_unexpected_index_list")
    assert first_result_partial_list == [
        {"a": 20, "pk_1": 1, "pk_2": "one", "b": 20, "c": 20},
        {"a": 30, "pk_1": 2, "pk_2": "two", "b": 30, "c": 30},
        {"a": 40, "pk_1": 3, "pk_2": "three", "b": 40, "c": 40},
        {"a": 50, "pk_1": 4, "pk_2": "four", "b": 50, "c": 50},
        {"a": 60, "pk_1": 5, "pk_2": "five", "b": 60, "c": 60},
    ]

    unexpected_index_query: str = evrs[0]["results"][0]["result"].get(
        "unexpected_index_query"
    )

    items_str = str(
        [
            (1, "one"),
            (2, "two"),
            (3, "three"),
            (4, "four"),
            (5, "five"),
        ]
    )

    assert unexpected_index_query == f"df.filter(items={items_str}, axis=0)"


@pytest.mark.filesystem
@pytest.mark.integration
def test_pandas_result_format_in_checkpoint_one_expectation_complete_output_fluent_batch_request_with_slice(
    empty_data_context: AbstractDataContext,
    reference_checkpoint_config_for_unexpected_column_names: dict,
    pandas_animals_dataframe_for_unexpected_rows_and_index: pd.DataFrame,
):
    context = empty_data_context
    expectation_suite_name = "metrics_exp"
    context.add_expectation_suite(expectation_suite_name=expectation_suite_name)

    data_frame_asset = context.sources.add_pandas(
        name="pandas_datasource"
    ).add_dataframe_asset(name="IN_MEMORY_DATA_ASSET")
    batch_request = data_frame_asset.build_batch_request(
        dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )
    validator.expect_column_values_to_be_unique(column="animals")
    validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint_config_yml = """
name: my_checkpoint
config_version: 1
class_name: Checkpoint
run_name_template: "%Y-%m-foo-bar-template-test"
batch_request:
  datasource_name: pandas_datasource
  data_asset_name: IN_MEMORY_DATA_ASSET
action_list:
    - name: store_validation_result
      action:
        class_name: StoreValidationResultAction
    - name: store_evaluation_params
      action:
        class_name: StoreEvaluationParametersAction
    - name: update_data_docs
      action:
        class_name: UpdateDataDocsAction
runtime_configuration:
  result_format:
    result_format: COMPLETE
"""

    checkpoint_config = CheckpointConfig(**yaml.load(checkpoint_config_yml))

    context.add_checkpoint(**checkpoint_config.to_json_dict())

    result: CheckpointResult

    result = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name=expectation_suite_name,
    )
    assert result.checkpoint_config.batch_request == {
        "datasource_name": "pandas_datasource",
        "data_asset_name": "IN_MEMORY_DATA_ASSET",
    }

    # TODO: <Alex>06/01/2023: For "DataAsset" types containing ephemeral data references, best practices is to supply "batch_request" as argument to "run_checkpoint()" method.</Alex>
    result = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        expectation_suite_name=expectation_suite_name,
        batch_request=batch_request,
    )
    assert result.success
    assert dict(
        sorted(
            list(result.run_results.values())[0]["validation_result"][
                "statistics"
            ].items(),
            key=lambda element: element[0],
        )
    ) == {
        "evaluated_expectations": 1,
        "successful_expectations": 1,
        "success_percent": 100.0,
        "unsuccessful_expectations": 0,
    }


@pytest.mark.timeout(
    2.0  # this test can take longer than the default timeout, try to reduce it
)
@pytest.mark.filesystem
@pytest.mark.parametrize(
    "result_format", ["BOOLEAN_ONLY", {"result_format": "BOOLEAN_ONLY"}]
)
def test_rendered_content_bool_only_respected(
    empty_data_context: AbstractDataContext,
    pandas_animals_dataframe_for_unexpected_rows_and_index: pd.DataFrame,
    result_format: str | dict,
):
    context = empty_data_context
    csv_asset = context.sources.pandas_default.add_dataframe_asset(
        "df",
        dataframe=pandas_animals_dataframe_for_unexpected_rows_and_index,
    )
    batch_request = csv_asset.build_batch_request()
    expectation_suite_name = "test_result_format_suite"
    context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name,
    )
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )
    expectation_validation_result = (
        validator.expect_column_distinct_values_to_contain_set(
            column="animals",
            value_set={"anglerfish"},
        )
    )
    validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint = SimpleCheckpoint(
        name="my_checkpoint",
        data_context=context,
        validations=[
            {
                "expectation_suite_name": expectation_suite_name,
                "batch_request": batch_request,
            }
        ],
        runtime_configuration={
            "result_format": result_format,
        },
    )

    checkpoint_result = checkpoint.run()
    validation_result_identifier = (
        checkpoint_result.list_validation_result_identifiers()[0]
    )
    expectation_validation_result = checkpoint_result.run_results[
        validation_result_identifier
    ]["validation_result"]["results"][0]
    assert expectation_validation_result.result == {}
