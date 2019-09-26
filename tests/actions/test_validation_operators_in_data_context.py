import pytest
import json
import os
import shutil

import pandas as pd

import great_expectations as ge
from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.actions.validation_operators import (
    DataContextAwareValidationOperator,
)
from great_expectations.data_context import (
    ConfigOnlyDataContext,
    DataContext,
)
from great_expectations.data_context.types import (
    DataContextConfig,
    DataAssetIdentifier,
    ExpectationSuiteIdentifier,
)
from great_expectations.util import (
    gen_directory_tree_str
)

@pytest.fixture()
def basic_data_context_config_for_validation_operator():
    return DataContextConfig(**{
        "plugins_directory": "plugins/",
        "evaluation_parameter_store_name" : "evaluation_parameter_store",
        "expectations_store" : {
            "class_name": "ExpectationStore",
            "store_backend": {
                "class_name": "InMemoryStoreBackend",
            }
        },
        "datasources": {},
        "stores": {
            # This isn't currently used for Validation Actions, but it's required for DataContext to work.
            "evaluation_parameter_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "InMemoryStoreBackend",
            },
            "warning_validation_result_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "ValidationResultStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                }
            },
            "failure_validation_result_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "ValidationResultStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                }
            }
        },
        "data_docs": {
            "sites": {}
        },
        "validation_operators": {
            "default" : {
                # "module_name" : "great_expectations.actions.validation_operators",
                "class_name" : "DefaultDataContextAwareValidationOperator",
                "process_warnings_and_quarantine_rows_on_error" : True,
                "action_list" : [{
                    "name": "add_warnings_to_store",
                    "result_key": "warning",
                    "action" : {
                        # "module_name" : "great_expectations.actions",
                        "class_name" : "SummarizeAndStoreAction",
                        "target_store_name": "warning_validation_result_store",
                        "summarizer":{
                            "module_name": "tests.test_plugins.fake_actions",
                            "class_name": "TemporaryNoOpSummarizer",
                        },
                    }
                },{
                    "name": "add_failures_to_store",
                    "result_key": "failure",
                    "action" : {
                        # "module_name" : "great_expectations.actions",
                        "class_name" : "SummarizeAndStoreAction",
                        "target_store_name": "failure_validation_result_store",
                        "summarizer":{
                            "module_name": "tests.test_plugins.fake_actions",
                            "class_name": "TemporaryNoOpSummarizer",
                        },
                    }
                },{
                    "name" : "send_slack_message",
                    "result_key": None,
                    "action" : {
                        "module_name" : "great_expectations.actions",
                        "class_name" : "NoOpAction",
                    }
                }]
            }
        }
    })

def test_DefaultDataContextAwareValidationOperator(basic_data_context_config_for_validation_operator, tmp_path_factory, filesystem_csv_4):
    project_path = str(tmp_path_factory.mktemp('great_expectations'))

    data_context = ConfigOnlyDataContext(
        basic_data_context_config_for_validation_operator,
        project_path,
    )

    data_context.add_datasource("my_datasource",
                                class_name="PandasDatasource",
                                base_directory=str(filesystem_csv_4))

    data_context.create_expectation_suite("my_datasource/default/f1", "foo")
    df = data_context.get_batch("my_datasource/default/f1", "foo",
                                batch_kwargs=data_context.yield_batch_kwargs("my_datasource/default/f1"))
    df.expect_column_values_to_be_between(column="x", min_value=1, max_value=9)
    failure_expectations = df.get_expectation_suite(discard_failed_expectations=False)

    df.expect_column_values_to_not_be_null(column="y")
    warning_expectations = df.get_expectation_suite(discard_failed_expectations=False)

    df = data_context.get_batch("my_datasource/default/f1", "foo",
                                batch_kwargs=data_context.yield_batch_kwargs("my_datasource/default/f1"))

    df.expect_column_values_to_be_in_set(column="x", value_set=[1,3,5,7,9])
    quarantine_expectations = df.get_expectation_suite(discard_failed_expectations=False)


    data_asset_identifier = DataAssetIdentifier("my_datasource", "default", "f1")
    data_context.stores["expectations_store"].set(
        ExpectationSuiteIdentifier(
            data_asset_name=data_asset_identifier,
            expectation_suite_name="failure"
        ),
        failure_expectations
    )
    data_context.stores["expectations_store"].set(
        ExpectationSuiteIdentifier(
            data_asset_name=data_asset_identifier,
            expectation_suite_name="warning"
        ),
        warning_expectations
    )
    data_context.stores["expectations_store"].set(
        ExpectationSuiteIdentifier(
            data_asset_name=data_asset_identifier,
            expectation_suite_name="quarantine"
        ),
        quarantine_expectations
    )

    print("W"*80)
    print(json.dumps(warning_expectations, indent=2))
    print(json.dumps(failure_expectations, indent=2))
    print(json.dumps(quarantine_expectations, indent=2))


    my_df = pd.DataFrame({"x": [1,2,3,4,5]})
    my_ge_df = ge.from_pandas(my_df)

    assert data_context.stores["warning_validation_result_store"].list_keys() == []

    results = data_context.run_validation_operator(
        data_asset=my_ge_df,
        data_asset_identifier=data_asset_identifier,
        run_identifier="test-100",
        validation_operator_name="default",
    )
    # results = data_context.run_validation_operator(my_ge_df)

    warning_validation_result_store_keys = data_context.stores["warning_validation_result_store"].list_keys()
    print(warning_validation_result_store_keys)
    assert len(warning_validation_result_store_keys) == 1

    first_validation_result = data_context.stores["warning_validation_result_store"].get(warning_validation_result_store_keys[0])
    print(json.dumps(first_validation_result, indent=2))
    assert data_context.stores["warning_validation_result_store"].get(warning_validation_result_store_keys[0])["success"] == False


    failure_validation_result_store_keys = data_context.stores["failure_validation_result_store"].list_keys()
    print(json.dumps(data_context.stores["failure_validation_result_store"].get(failure_validation_result_store_keys[0]), indent=2))
    assert data_context.stores["failure_validation_result_store"].get(failure_validation_result_store_keys[0])["success"] == True


    # assert False

def test_DefaultDataContextAwareValidationOperator_with_file_structure(tmp_path_factory):
    base_path = str(tmp_path_factory.mktemp('test_DefaultDataContextAwareValidationOperator_with_file_structure__dir'))
    project_path = os.path.join( base_path, "project")
    print(os.getcwd())
    shutil.copytree(
        os.path.join( os.getcwd(), "tests/data_context/fixtures/post_init_project_v0.8.0_A" ),
        project_path,
    )
    print(gen_directory_tree_str(project_path))

    assert gen_directory_tree_str(project_path) =="""\
project/
    data/
        bob-ross/
            README.md
            cluster-paintings.py
            elements-by-episode.csv
    great_expectations/
        .gitignore
        great_expectations.yml
        expectations/
            data__dir/
                default/
                    bob-ross/
                        BasicDatasetProfiler.json
                        failure.json
                        quarantine.json
                        warning.json
        notebooks/
            create_expectations.ipynb
            integrate_validation_into_pipeline.ipynb
        uncommitted/
            documentation/
                local_site/
                    index.html
                    expectations/
                        data__dir/
                            default/
                                bob-ross/
                                    BasicDatasetProfiler.html
                    profiling/
                        data__dir/
                            default/
                                bob-ross/
                                    BasicDatasetProfiler.html
                team_site/
                    index.html
                    expectations/
                        data__dir/
                            default/
                                bob-ross/
                                    BasicDatasetProfiler.html
            validations/
                profiling/
                    data__dir/
                        default/
                            bob-ross/
                                BasicDatasetProfiler.json
"""

    data_context = DataContext(
        context_root_dir=os.path.join(project_path, "great_expectations"),
    )

    my_df = pd.DataFrame({"x": [1,2,3,4,5]})
    my_ge_df = ge.from_pandas(my_df)

    # assert data_context.stores["local_validation_result_store"].list_keys() == []
    validation_store_path = os.path.join(project_path, "great_expectations/uncommitted/validations")
    print(validation_store_path)
    print(gen_directory_tree_str(validation_store_path))
    assert gen_directory_tree_str(validation_store_path) == """\
validations/
    profiling/
        data__dir/
            default/
                bob-ross/
                    BasicDatasetProfiler.json
"""

    data_asset_identifier = DataAssetIdentifier("data__dir", "default", "bob-ross")
    results = data_context.run_validation_operator(
        data_asset=my_ge_df,
        data_asset_identifier=data_asset_identifier,
        run_identifier="test-100",
        validation_operator_name="default",
    )
    # results = data_context.run_validation_operator(my_ge_df)

    print(gen_directory_tree_str(validation_store_path))
    assert gen_directory_tree_str(validation_store_path) == """\
validations/
    profiling/
        data__dir/
            default/
                bob-ross/
                    BasicDatasetProfiler.json
    test-100/
        data__dir/
            default/
                bob-ross/
                    failure.json
                    warning.json
"""