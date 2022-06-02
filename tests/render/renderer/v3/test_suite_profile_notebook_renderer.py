import os
from typing import Any, List, Set
from unittest import mock

from ruamel.yaml import YAML

from great_expectations import DataContext

# noinspection PyProtectedMember
from great_expectations.cli.suite import _suite_edit_workflow
from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationSuite,
    ExpectationSuiteValidationResult,
)
from great_expectations.core.usage_statistics.anonymizers.types.base import (
    CLISuiteInteractiveFlagCombinations,
)
from great_expectations.render.renderer.v3.suite_profile_notebook_renderer import (
    SuiteProfileNotebookRenderer,
)
from great_expectations.rule_based_profiler import RuleBasedProfiler
from great_expectations.rule_based_profiler.config.base import (
    ruleBasedProfilerConfigSchema,
)
from tests.profile.conftest import get_set_of_columns_and_expectations_from_suite
from tests.render.test_util import find_code_in_notebook, run_notebook

yaml = YAML()


@mock.patch("great_expectations.data_context.DataContext")
def test_suite_notebook_renderer_render_user_configurable_profiler_configuration(
    mock_data_context: mock.MagicMock,
):
    renderer = SuiteProfileNotebookRenderer(
        context=mock_data_context,
        expectation_suite_name="my_expectation_suite",
        profiler_name="",  # No name should signal that UserConfigurableProfiler is necessary
        batch_request={
            "datasource_name": "my_datasource",
            "data_connector_name": "my_basic_data_connector",
            "data_asset_name": "Titanic_1912",
        },
    )
    notebook = renderer.render()

    snippets = [
        # Imports
        """import datetime

import pandas as pd

import great_expectations as ge
import great_expectations.jupyter_ux
from great_expectations.core.batch import BatchRequest
from great_expectations.profile.user_configurable_profiler import (
    UserConfigurableProfiler,
)
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError""",
        # Batch request
        """batch_request = {
    "datasource_name": "my_datasource",
    "data_connector_name": "my_basic_data_connector",
    "data_asset_name": "Titanic_1912",
}""",
        # Profiler instantiation/usage
        """profiler = UserConfigurableProfiler(
    profile_dataset=validator,
    excluded_expectations=None,
    ignored_columns=ignored_columns,
    not_null_only=False,
    primary_or_compound_key=False,
    semantic_types_dict=None,
    table_expectations_only=False,
    value_set_threshold="MANY",
)
suite = profiler.build_suite()""",
    ]

    for snippet in snippets:
        assert find_code_in_notebook(
            notebook, snippet
        ), f"Could not find snippet in Notebook: {snippet}"


def test_notebook_execution_user_configurable_profiler_with_pandas_backend(
    titanic_v013_multi_datasource_pandas_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    """
    To set this test up we:

    - create a suite using User-Configurable Profiler
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
        profiler_name=None,
        usage_event="test_notebook_execution",
        interactive_mode=CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_FALSE_MANUAL_TRUE,
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


@mock.patch("great_expectations.data_context.DataContext")
def test_suite_notebook_renderer_render_rule_based_profiler_configuration(
    mock_data_context: mock.MagicMock,
):
    renderer = SuiteProfileNotebookRenderer(
        context=mock_data_context,
        expectation_suite_name="my_expectation_suite",
        profiler_name="my_profiler",  # Name should signal that RBP from context's profile store is necessary
        batch_request={
            "datasource_name": "my_datasource",
            "data_connector_name": "my_basic_data_connector",
            "data_asset_name": "Titanic_1912",
        },
    )
    notebook = renderer.render()

    snippets = [
        # Imports
        """import datetime

import pandas as pd

import great_expectations as ge
import great_expectations.jupyter_ux
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError""",
        # Batch request
        """batch_request = {
    "datasource_name": "my_datasource",
    "data_connector_name": "my_basic_data_connector",
    "data_asset_name": "Titanic_1912",
}""",
        # Profiler instantiation/usage
        """result = context.run_profiler_with_dynamic_arguments(
    name="my_profiler",
    batch_request=batch_request,
)
_ = validator.expectation_suite.add_expectation_configurations(
    expectation_configurations=result.expectation_configurations
)""",
    ]

    for snippet in snippets:
        assert find_code_in_notebook(
            notebook, snippet
        ), f"Could not find snippet in Notebook: {snippet}"


def test_notebook_execution_rule_based_profiler_with_pandas_backend(
    titanic_v013_multi_datasource_pandas_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
    bobby_columnar_table_multi_batch,
):
    """
    To set this test up we:

    - create a suite using Rule-Based Profiler
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

    # Load profiler configs & loop (run tests for each one)
    yaml_config: str = bobby_columnar_table_multi_batch["profiler_config"]

    # Instantiate Profiler
    profiler_config: dict = yaml.load(yaml_config)

    # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
    deserialized_config: dict = ruleBasedProfilerConfigSchema.load(profiler_config)
    serialized_config: dict = ruleBasedProfilerConfigSchema.dump(deserialized_config)

    # `class_name`/`module_name` are generally consumed through `instantiate_class_from_config`
    # so we need to manually remove those values if we wish to use the **kwargs instantiation pattern
    serialized_config.pop("class_name")
    serialized_config.pop("module_name")

    profiler: RuleBasedProfiler = RuleBasedProfiler(
        **serialized_config,
        data_context=context,
    )

    profiler_name: str = "bobby_user_workflow"

    context.save_profiler(
        profiler=profiler,
        name=profiler_name,
    )

    # Create notebook
    # do not want to actually send usage_message, since the function call is not the result of actual usage
    _suite_edit_workflow(
        context=context,
        expectation_suite_name=expectation_suite_name,
        profile=True,
        profiler_name=profiler_name,
        usage_event="test_notebook_execution",
        interactive_mode=CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_FALSE_MANUAL_TRUE,
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
        "evaluated_expectations": 13,
        "successful_expectations": 13,
        "unsuccessful_expectations": 0,
        "success_percent": 100.0,
    }

    expected_expectation_configurations: List[ExpectationConfiguration] = [
        ExpectationConfiguration(
            **{
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "domain_kwargs": {},
                            "metric_dependencies": None,
                            "metric_name": "table.row_count",
                            "metric_value_kwargs": None,
                        },
                        "num_batches": 1,
                    }
                },
                "kwargs": {"max_value": 1313, "min_value": 1313},
                "expectation_type": "expect_table_row_count_to_be_between",
            }
        ),
        ExpectationConfiguration(
            **{
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "domain_kwargs": {"column": "Unnamed: 0"},
                            "metric_dependencies": None,
                            "metric_name": "column.min",
                            "metric_value_kwargs": None,
                        },
                        "num_batches": 1,
                    }
                },
                "kwargs": {
                    "column": "Unnamed: 0",
                    "max_value": 1,
                    "min_value": 1,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
            }
        ),
        ExpectationConfiguration(
            **{
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "domain_kwargs": {"column": "Unnamed: 0"},
                            "metric_dependencies": None,
                            "metric_name": "column.max",
                            "metric_value_kwargs": None,
                        },
                        "num_batches": 1,
                    }
                },
                "kwargs": {
                    "column": "Unnamed: 0",
                    "max_value": 1313,
                    "min_value": 1313,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
            }
        ),
        ExpectationConfiguration(
            **{
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "domain_kwargs": {"column": "Age"},
                            "metric_dependencies": None,
                            "metric_name": "column.min",
                            "metric_value_kwargs": None,
                        },
                        "num_batches": 1,
                    }
                },
                "kwargs": {
                    "column": "Age",
                    "max_value": 0.17,
                    "min_value": 0.17,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
            }
        ),
        ExpectationConfiguration(
            **{
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "domain_kwargs": {"column": "Age"},
                            "metric_dependencies": None,
                            "metric_name": "column.max",
                            "metric_value_kwargs": None,
                        },
                        "num_batches": 1,
                    }
                },
                "kwargs": {
                    "column": "Age",
                    "max_value": 71.0,
                    "min_value": 71.0,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
            }
        ),
        ExpectationConfiguration(
            **{
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "domain_kwargs": {"column": "Survived"},
                            "metric_dependencies": None,
                            "metric_name": "column.min",
                            "metric_value_kwargs": None,
                        },
                        "num_batches": 1,
                    }
                },
                "kwargs": {
                    "column": "Survived",
                    "max_value": 0,
                    "min_value": 0,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
            }
        ),
        ExpectationConfiguration(
            **{
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "domain_kwargs": {"column": "Survived"},
                            "metric_dependencies": None,
                            "metric_name": "column.max",
                            "metric_value_kwargs": None,
                        },
                        "num_batches": 1,
                    }
                },
                "kwargs": {
                    "column": "Survived",
                    "max_value": 1,
                    "min_value": 1,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
            }
        ),
        ExpectationConfiguration(
            **{
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "domain_kwargs": {"column": "SexCode"},
                            "metric_dependencies": None,
                            "metric_name": "column.min",
                            "metric_value_kwargs": None,
                        },
                        "num_batches": 1,
                    }
                },
                "kwargs": {
                    "column": "SexCode",
                    "max_value": 0,
                    "min_value": 0,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_min_to_be_between",
            }
        ),
        ExpectationConfiguration(
            **{
                "meta": {
                    "profiler_details": {
                        "metric_configuration": {
                            "domain_kwargs": {"column": "SexCode"},
                            "metric_dependencies": None,
                            "metric_name": "column.max",
                            "metric_value_kwargs": None,
                        },
                        "num_batches": 1,
                    }
                },
                "kwargs": {
                    "column": "SexCode",
                    "max_value": 1,
                    "min_value": 1,
                    "mostly": 1.0,
                },
                "expectation_type": "expect_column_max_to_be_between",
            }
        ),
        ExpectationConfiguration(
            **{
                "meta": {},
                "kwargs": {
                    "column": "PClass",
                    "value_set": [
                        "*",
                        "1st",
                        "2nd",
                        "3rd",
                    ],
                },
                "expectation_type": "expect_column_values_to_be_in_set",
            }
        ),
        ExpectationConfiguration(
            **{
                "meta": {},
                "kwargs": {"column": "Sex", "value_set": ["female", "male"]},
                "expectation_type": "expect_column_values_to_be_in_set",
            }
        ),
        ExpectationConfiguration(
            **{
                "meta": {},
                "kwargs": {"column": "Survived", "value_set": [0, 1]},
                "expectation_type": "expect_column_values_to_be_in_set",
            }
        ),
        ExpectationConfiguration(
            **{
                "meta": {},
                "kwargs": {"column": "SexCode", "value_set": [0, 1]},
                "expectation_type": "expect_column_values_to_be_in_set",
            }
        ),
    ]

    suite: ExpectationSuite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )

    expectation_configurations: List[ExpectationConfiguration] = []
    expectation_configuration: ExpectationConfiguration
    for expectation_configuration in suite.expectations:
        kwargs: dict = expectation_configuration.kwargs
        key: str
        value: Any
        kwargs = {
            key: sorted(value) if isinstance(value, (list, set, tuple)) else value
            for key, value in kwargs.items()
        }
        expectation_configuration.kwargs = kwargs
        expectation_configurations.append(expectation_configuration)

    assert expectation_configurations == expected_expectation_configurations
