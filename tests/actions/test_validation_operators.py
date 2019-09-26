import pytest
import json
import copy

import pandas as pd

import great_expectations as ge
from great_expectations.actions.validation_operators import (
    DataContextAwareValidationOperator,
    DefaultDataContextAwareValidationOperator,
)
from great_expectations.data_context import (
    ConfigOnlyDataContext,
)
from great_expectations.data_context.types import (
    DataContextConfig,
    DataAssetIdentifier,
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)

@pytest.fixture()
def basic_data_context_config_for_validation_operator():
    return DataContextConfig(**{
        "plugins_directory": "plugins/",
        "evaluation_parameter_store_name" : "evaluation_parameter_store",
        "expectations_store" : {
            "class_name": "ExpectationStore",
            "store_backend": {
                "class_name": "FixedLengthTupleFilesystemStoreBackend",
                "base_directory": "expectations/",
            }
        },
        "datasources": {},
        "stores": {
            # This isn't currently used for Validation Actions, but it's required for DataContext to work.
            "evaluation_parameter_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "InMemoryStoreBackend",
            },
            "validation_result_store" : {
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
        "validation_operators" : {},
    })

def test_validation_operator__run(basic_data_context_config_for_validation_operator, tmp_path_factory, filesystem_csv_4):
    project_path = str(tmp_path_factory.mktemp('great_expectations'))

    # NOTE: This setup is almost identical to test_DefaultDataContextAwareValidationOperator.
    # Consider converting to a single fixture.

    data_context = ConfigOnlyDataContext(
        basic_data_context_config_for_validation_operator,
        project_path,
    )

    data_context.add_datasource("my_datasource",
                                class_name="PandasDatasource",
                                base_directory=str(filesystem_csv_4))

    data_asset_name = "my_datasource/default/f1"
    data_context.create_expectation_suite(data_asset_name, "foo")
    df = data_context.get_batch(data_asset_name,
                                "foo",
                                batch_kwargs=data_context.yield_batch_kwargs(data_asset_name))
    df.expect_column_values_to_be_between(column="x", min_value=1, max_value=9)
    failure_expectations = df.get_expectation_suite(discard_failed_expectations=False)

    df.expect_column_values_to_not_be_null(column="y")
    warning_expectations = df.get_expectation_suite(discard_failed_expectations=False)

    data_context.create_expectation_suite(data_asset_name, "default")
    df = data_context.get_batch(data_asset_name, "default",
                                batch_kwargs=data_context.yield_batch_kwargs(data_asset_name))
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

    # TODO : Abe 2019/09/17 : We can make this config much more concise by subclassing an operator that knows how to define
    # its own actions. Holding off on that for now, but we should consider it before shipping v0.8.0.
    vo = DefaultDataContextAwareValidationOperator(
        data_context=data_context,
        process_warnings_and_quarantine_rows_on_error=True,
        # allow_empty_expectation_suites=True,
        action_list = [{
            "name": "add_warnings_to_store",
            "result_key": "warning",
            "action" : {
                "module_name" : "great_expectations.actions",
                "class_name" : "SummarizeAndStoreAction",
                "target_store_name": "validation_result_store",
                "summarizer":{
                    "module_name": "tests.test_plugins.fake_actions",
                    "class_name": "TemporaryNoOpSummarizer",
                }
            }
        },{
            "name": "add_failures_to_store",
            "result_key": "failure",
            "action" : {
                "module_name" : "great_expectations.actions",
                "class_name" : "SummarizeAndStoreAction",
                "target_store_name": "validation_result_store",
                "summarizer":{
                    "module_name": "tests.test_plugins.fake_actions",
                    "class_name": "TemporaryNoOpSummarizer",
                }
            }
        }],
    )

    my_df = pd.DataFrame({"x": [1,2,3,4,5], "y": [1,2,3,4,None]})
    my_ge_df = ge.from_pandas(my_df)

    assert data_context.stores["validation_result_store"].list_keys() == []

    results = vo.run(
        data_asset=my_ge_df,
        data_asset_identifier=DataAssetIdentifier(
            from_string="DataAssetIdentifier.my_datasource.default.f1"
        ),
        run_identifier="test_100"
    )
    # print(json.dumps(results["validation_results"], indent=2))

    validation_result_store_keys = data_context.stores["validation_result_store"].list_keys() 
    print(validation_result_store_keys)
    assert len(validation_result_store_keys) == 2
    assert ValidationResultIdentifier(from_string="ValidationResultIdentifier.my_datasource.default.f1.warning.test_100") in validation_result_store_keys
    assert ValidationResultIdentifier(from_string="ValidationResultIdentifier.my_datasource.default.f1.failure.test_100") in validation_result_store_keys

    assert data_context.stores["validation_result_store"].get(
        ValidationResultIdentifier(from_string="ValidationResultIdentifier.my_datasource.default.f1.warning.test_100")
    )["success"] == False
    assert data_context.stores["validation_result_store"].get(
        ValidationResultIdentifier(from_string="ValidationResultIdentifier.my_datasource.default.f1.failure.test_100")
    )["success"] == True

    #TODO: One DataSnapshotStores are implemented, add a test for quarantined data

def test__get_or_convert_to_batch_from_identifiers(basic_data_context_config_for_validation_operator, filesystem_csv):

    data_context = ConfigOnlyDataContext(
        basic_data_context_config_for_validation_operator,
        "fake/testing/path/",
    )

    data_context.add_datasource("my_datasource",
                                class_name="PandasDatasource",
                                base_directory=str(filesystem_csv))

    vo = DefaultDataContextAwareValidationOperator(
        data_context=data_context,
        process_warnings_and_quarantine_rows_on_error=True,
        action_list = [{
            "name": "add_warnings_to_store",
            "result_key": "warning",
            "action" : {
                "module_name" : "great_expectations.actions",
                "class_name" : "SummarizeAndStoreAction",
                "target_store_name": "validation_result_store",
                "summarizer":{
                    "module_name": "tests.test_plugins.fake_actions",
                    "class_name": "TemporaryNoOpSummarizer",
                }
            }
        }],
    )

    my_df = pd.DataFrame({"x": [1,2,3,4,5]})
    my_ge_df = ge.from_pandas(my_df)

    my_ge_df_with_identifiers = copy.deepcopy(my_ge_df)
    my_ge_df_with_identifiers._expectation_suite["data_asset_name"] = DataAssetIdentifier("x","y","z")

    # Pass nothing at all
    with pytest.raises(ValueError):
        vo._get_or_convert_to_batch_from_identifiers()

    # Pass a DataAsset that already has good identifiers
    batch = vo._get_or_convert_to_batch_from_identifiers(
        my_ge_df_with_identifiers,
        run_identifier="test-100"
    )
    assert batch.data_asset_identifier == DataAssetIdentifier("x","y","z")
    assert batch.run_id == "test-100"

    # Pass a DataAsset without its own identifiers, but other, sufficient identifiers
    batch = vo._get_or_convert_to_batch_from_identifiers(
        copy.deepcopy(my_ge_df),
        data_asset_identifier=DataAssetIdentifier("x","y","z"),
        run_identifier="test-100"
    )
    assert batch.data_asset_identifier == DataAssetIdentifier("x","y","z")
    assert batch.run_id == "test-100"

    # Pass a DataAsset without sufficient identifiers
    with pytest.raises(ValueError):
        batch = vo._get_or_convert_to_batch_from_identifiers(
            copy.deepcopy(my_ge_df),
            run_identifier="test-100"
        )

    with pytest.raises(ValueError):
        batch = vo._get_or_convert_to_batch_from_identifiers(
            copy.deepcopy(my_ge_df),
            data_asset_identifier=DataAssetIdentifier("x","y","z"),
        )

    # Pass a DataAsset without its own identifiers, but other, sufficient identifiers
    batch = vo._get_or_convert_to_batch_from_identifiers(
        my_ge_df,
        data_asset_id_string="my_datasource/default/f1",
        run_identifier="test-100"
    )
    assert batch.data_asset_identifier == DataAssetIdentifier("my_datasource","default","f1")
    assert batch.run_id == "test-100"

    batch = vo._get_or_convert_to_batch_from_identifiers(
        my_ge_df,
        data_asset_id_string="f1",
        run_identifier="test-100"
    )
    assert batch.data_asset_identifier == DataAssetIdentifier("my_datasource","default","f1")
    assert batch.run_id == "test-100"

    # NOTE: Perhaps we should allow this case?
    # Pass a raw DataFrame without its own identifiers, but other, sufficient identifiers
    # batch = vo._get_or_convert_to_batch_from_identifiers(
    #     my_df,
    #     data_asset_identifier=DataAssetIdentifier("x","y","z"),
    #     run_identifier="test-100"
    # )

    # No DataAsset; sufficient identifiers (typed)
    batch = vo._get_or_convert_to_batch_from_identifiers(
        data_asset_identifier=DataAssetIdentifier("my_datasource","default","f1"),
        run_identifier="test-100"
    )
    assert batch.data_asset_identifier == DataAssetIdentifier("my_datasource","default","f1")
    assert batch.run_id == "test-100"

    # No DataAsset; sufficient identifiers (untyped)
    batch = vo._get_or_convert_to_batch_from_identifiers(
        data_asset_id_string="f1",
        run_identifier="test-100"
    )
    assert batch.data_asset_identifier == DataAssetIdentifier("my_datasource","default","f1")
    assert batch.run_id == "test-100"

    batch = vo._get_or_convert_to_batch_from_identifiers(
        data_asset_id_string="my_datasource/default/f1",
        run_identifier="test-100"
    )
    assert batch.data_asset_identifier == DataAssetIdentifier("my_datasource","default","f1")
    assert batch.run_id == "test-100"

    # No DataAsset; sufficient identifiers
    with pytest.raises(ValueError):
        batch = vo._get_or_convert_to_batch_from_identifiers(
            run_identifier="test-100"
        )

    with pytest.raises(ValueError):
        batch = vo._get_or_convert_to_batch_from_identifiers(
            data_asset_id_string="my_datasource/default/f1",
        )

