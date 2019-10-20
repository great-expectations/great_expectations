import pytest
import json
import os
import shutil

import pandas as pd

from great_expectations.data_context import (
    ConfigOnlyDataContext,
    DataContext,
)
from great_expectations.data_context.types import (
#     DataContextConfig,
    DataAssetIdentifier,
    ExpectationSuiteIdentifier,
)
from great_expectations.util import (
    gen_directory_tree_str
)

@pytest.fixture()
def basic_data_context_config_for_validation_operator():
    # return DataContextConfig(**{
    return {
        "plugins_directory": "plugins/",
        "evaluation_parameter_store_name" : "evaluation_parameter_store",
        "validations_store_name": "validation_result_store",
        "expectations_store_name": "expectations_store",
        "datasources": {},
        "stores": {
            "expectations_store" : {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                }
            },
            # This isn't currently used for Validation Actions, but it's required for DataContext to work.
            "evaluation_parameter_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "InMemoryEvaluationParameterStore",
            },
            "validation_result_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                }
            },
        },
        "data_docs_sites": {},
        "validation_operators": {
            "store_val_res_and_extract_eval_params" : {
                "class_name" : "ActionListValidationOperator",
                "action_list" : [{
                    "name": "store_validation_result",
                    "action" : {
                        "class_name" : "StoreAction",
                        "target_store_name": "validation_result_store",
                    }
                },{
                    "name": "extract_and_store_eval_parameters",
                    "action" : {
                        "class_name" : "ExtractAndStoreEvaluationParamsAction",
                        "target_store_name": "evaluation_parameter_store",
                    }
                }]
            }
        }
    }
    # })

def test_ActionListValidationOperator(basic_data_context_config_for_validation_operator, tmp_path_factory, filesystem_csv_4):
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

    data_context.save_expectation_suite(failure_expectations, data_asset_name="my_datasource/default/f1",
                                        expectation_suite_name="failure")
    data_context.save_expectation_suite(warning_expectations, data_asset_name="my_datasource/default/f1",
                                        expectation_suite_name="warning")

    print("W"*80)
    print(json.dumps(warning_expectations, indent=2))
    print(json.dumps(failure_expectations, indent=2))

    validator_batch_kwargs = data_context.yield_batch_kwargs("my_datasource/default/f1")
    batch = data_context.get_batch("my_datasource/default/f1",
                                   expectation_suite_name="failure",
                                   batch_kwargs=validator_batch_kwargs
                                   )

    assert data_context.stores["validation_result_store"].list_keys() == []
    # We want to demonstrate running the validation operator with both a pre-built batch (DataAsset) and with
    # a tuple of parameters for get_batch
    operator_result = data_context.run_validation_operator(
        assets_to_validate=[batch, ("my_datasource/default/f1", "warning", validator_batch_kwargs)],
        run_id="test-100",
        validation_operator_name="store_val_res_and_extract_eval_params",
    )
    # results = data_context.run_validation_operator(my_ge_df)

    validation_result_store_keys = data_context.stores["validation_result_store"].list_keys()
    print(validation_result_store_keys)
    assert len(validation_result_store_keys) == 2
    assert operator_result['success']
    assert len(operator_result['details'].keys()) == 2

    first_validation_result = data_context.stores["validation_result_store"].get(validation_result_store_keys[0])
    print(json.dumps(first_validation_result, indent=2))
    assert data_context.stores["validation_result_store"].get(validation_result_store_keys[0])["success"] is True


def test_WarningAndFailureExpectationSuitesValidationOperator_with_file_structure(tmp_path_factory):
    base_path = str(tmp_path_factory.mktemp('test_DefaultDataContextAwareValidationOperator_with_file_structure__dir'))
    project_path = os.path.join( base_path, "project")
    print(os.getcwd())
    shutil.copytree(
        os.path.join( os.getcwd(), "tests/data_context/fixtures/post_init_project_v0.8.0_A" ),
        project_path,
    )
    print(gen_directory_tree_str(project_path))

    assert gen_directory_tree_str(project_path) == """\
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
            pandas/
                create_expectations.ipynb
                validation_playground.ipynb
            spark/
                create_expectations.ipynb
                validation_playground.ipynb
            sql/
                create_expectations.ipynb
                validation_playground.ipynb
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

    data_asset_name = "data__dir/default/bob-ross"
    data_context.create_expectation_suite(data_asset_name=data_asset_name, expectation_suite_name="default")
    batch = data_context.get_batch(data_asset_name=data_asset_name, expectation_suite_name="default",
                                   batch_kwargs=data_context.yield_batch_kwargs(data_asset_name))

    validation_store_path = os.path.join(project_path, "great_expectations/uncommitted/validations")
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
        assets_to_validate=[batch],
        run_id="test-100",
        validation_operator_name="errors_and_warnings_validation_operator",
    )

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