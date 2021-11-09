import os
from typing import Set

from great_expectations import DataContext

# noinspection PyProtectedMember
from great_expectations.cli.suite import _suite_edit_workflow
from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationSuite,
    ExpectationSuiteValidationResult,
)
from tests.profile.conftest import get_set_of_columns_and_expectations_from_suite
from tests.render.test_util import run_notebook


def test_notebook_execution_with_pandas_backend(
    titanic_v013_multi_datasource_pandas_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    """
    To set this test up we:

    - create a suite using profiling
    - verify that no validations have happened
    - create the suite edit notebook by hijacking the private cli method

    We then:
    - execute that notebook (Note this will raise various errors like
    CellExecutionError if any cell in the notebook fails
    - create a new context from disk
    - verify that a validation has been run with our expectation suite
    """
    context: DataContext = titanic_v013_multi_datasource_pandas_data_context_with_checkpoints_v1_with_empty_store_stats_enabled
    root_dir: str = context.root_directory
    uncommitted_dir: str = os.path.join(root_dir, "uncommitted")
    expectation_suite_name: str = "warning"

    context.create_expectation_suite(expectation_suite_name=expectation_suite_name)
    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1912",
    }

    # Sanity check test setup
    original_suite: ExpectationSuite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert len(original_suite.expectations) == 0
    assert context.list_expectation_suite_names() == [expectation_suite_name]
    assert context.list_datasources() == [
        {
            "name": "my_datasource",
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            "data_connectors": {
                "my_basic_data_connector": {
                    "module_name": "great_expectations.datasource.data_connector",
                    "base_directory": f"{root_dir}/../data/titanic",
                    "default_regex": {
                        "pattern": "(.*)\\.csv",
                        "group_names": ["data_asset_name"],
                    },
                    "class_name": "InferredAssetFilesystemDataConnector",
                },
                "my_special_data_connector": {
                    "glob_directive": "*.csv",
                    "assets": {
                        "users": {
                            "pattern": "(.+)_(\\d+)_(\\d+)\\.csv",
                            "group_names": ["name", "timestamp", "size"],
                            "class_name": "Asset",
                            "base_directory": f"{root_dir}/../data/titanic",
                            "module_name": "great_expectations.datasource.data_connector.asset",
                        }
                    },
                    "module_name": "great_expectations.datasource.data_connector",
                    "base_directory": f"{root_dir}/../data/titanic",
                    "default_regex": {"pattern": "(.+)\\.csv", "group_names": ["name"]},
                    "class_name": "ConfiguredAssetFilesystemDataConnector",
                },
                "my_other_data_connector": {
                    "glob_directive": "*.csv",
                    "assets": {
                        "users": {
                            "class_name": "Asset",
                            "module_name": "great_expectations.datasource.data_connector.asset",
                        }
                    },
                    "module_name": "great_expectations.datasource.data_connector",
                    "base_directory": f"{root_dir}/../data/titanic",
                    "default_regex": {"pattern": "(.+)\\.csv", "group_names": ["name"]},
                    "class_name": "ConfiguredAssetFilesystemDataConnector",
                },
                "my_runtime_data_connector": {
                    "module_name": "great_expectations.datasource.data_connector",
                    "batch_identifiers": ["pipeline_stage_name", "airflow_run_id"],
                    "class_name": "RuntimeDataConnector",
                },
            },
        },
        {
            "name": "my_additional_datasource",
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": "PandasExecutionEngine",
            },
            "data_connectors": {
                "my_additional_data_connector": {
                    "module_name": "great_expectations.datasource.data_connector",
                    "default_regex": {
                        "pattern": "(.*)\\.csv",
                        "group_names": ["data_asset_name"],
                    },
                    "base_directory": f"{root_dir}/../data/titanic",
                    "class_name": "InferredAssetFilesystemDataConnector",
                }
            },
        },
    ]

    assert context.get_validation_result(expectation_suite_name="warning") == {}

    # Create notebook
    # do not want to actually send usage_message, since the function call is not the result of actual usage
    _suite_edit_workflow(
        context=context,
        expectation_suite_name=expectation_suite_name,
        profile=True,
        usage_event="test_notebook_execution",
        interactive=False,
        no_jupyter=True,
        create_if_not_exist=False,
        datasource_name=None,
        batch_request=batch_request,
        additional_batch_request_args=None,
        suppress_usage_message=True,
        assume_yes=True,
    )
    edit_notebook_path: str = os.path.join(uncommitted_dir, "edit_warning.ipynb")
    assert os.path.isfile(edit_notebook_path)

    run_notebook(
        notebook_path=edit_notebook_path,
        notebook_dir=uncommitted_dir,
        string_to_be_replaced="context.open_data_docs(resource_identifier=validation_result_identifier)",
        replacement_string="",
    )

    # Assertions about output
    context = DataContext(context_root_dir=root_dir)
    obs_validation_result: ExpectationSuiteValidationResult = (
        context.get_validation_result(expectation_suite_name="warning")
    )
    assert obs_validation_result.statistics == {
        "evaluated_expectations": 2,
        "successful_expectations": 2,
        "unsuccessful_expectations": 0,
        "success_percent": 100.0,
    }

    suite: ExpectationSuite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    suite["meta"].pop("citations", None)
    assert suite.expectations == [
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_table_columns_to_match_ordered_list",
                "kwargs": {
                    "column_list": [
                        "Unnamed: 0",
                        "Name",
                        "PClass",
                        "Age",
                        "Sex",
                        "Survived",
                        "SexCode",
                    ]
                },
                "meta": {},
            }
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"max_value": 1313, "min_value": 1313},
                "meta": {},
            }
        ),
    ]

    columns_with_expectations: Set[str]
    expectations_from_suite: Set[str]
    (
        columns_with_expectations,
        expectations_from_suite,
    ) = get_set_of_columns_and_expectations_from_suite(suite=suite)

    expected_expectations: Set[str] = {
        "expect_table_columns_to_match_ordered_list",
        "expect_table_row_count_to_be_between",
    }
    assert columns_with_expectations == set()
    assert expectations_from_suite == expected_expectations
