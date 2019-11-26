import pytest

import datetime

from great_expectations.core import DataAssetIdentifier
from great_expectations.data_context.types.metrics import ExpectationDefinedMetricIdentifier
from great_expectations.data_context.util import instantiate_class_from_config


from great_expectations.core import ExpectationSuiteValidationResult, ExpectationValidationResult, \
    ExpectationConfiguration, dataAssetIdentifierSchema


@pytest.fixture(params=[
    {
        "class_name": "DatabaseEvaluationParameterStore",
        "store_backend": {
            "credentials": {
                "drivername": "postgresql",
                "username": "postgres",
                "password": "",
                "host": "localhost",
                "port": "5432",
                "database": "test_ci"
            }
        }
    },
    {
        "class_name": "InMemoryEvaluationParameterStore",
        "module_name": "great_expectations.data_context.store"
    }
])
def param_store(request):
    return instantiate_class_from_config(
        config=request.param,
        config_defaults={
            "module_name": "great_expectations.data_context.store",
        },
        runtime_config={}
    )


def test_evaluation_parameter_store_methods(data_context):
    run_id = "20191125T000000.000000Z"
    source_patient_data_results = ExpectationSuiteValidationResult(
        meta={
            "data_asset_name": {
                "datasource": "mydatasource",
                "generator": "mygenerator",
                "generator_asset": "source_patient_data"
            },
            "expectation_suite_name": "default"
        },
        results=[
            ExpectationValidationResult(
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_table_row_count_to_equal",
                    kwargs={
                        "value": 1024,
                    }
                ),
                success=True,
                exception_info={
                    "exception_message": None,
                    "exception_traceback": None,
                    "raised_exception": False},
                result={
                    "observed_value": 1024,
                    "element_count": 1024,
                    "missing_percent": 0.0,
                    "missing_count": 0
                }
            )
        ],
        success=True
    )

    data_context._extract_and_store_parameters_from_validation_results(
        source_patient_data_results,
        data_asset_name=dataAssetIdentifierSchema.load(source_patient_data_results.meta["data_asset_name"]).data,
        expectation_suite_name=source_patient_data_results.meta["expectation_suite_name"],
        run_id=run_id,
    )

    bound_parameters = data_context.evaluation_parameter_store.get_bind_params(run_id)
    assert bound_parameters == {
        'urn:great_expectations:validations:mydatasource/mygenerator/source_patient_data:default:expectations:expect_table_row_count_to_equal:observed_value': 1024
    }
    source_diabetes_data_results = ExpectationSuiteValidationResult(
        meta={
            "data_asset_name": {
                "datasource": "mydatasource",
                "generator": "mygenerator",
                "generator_asset": "source_diabetes_data"
            },
            "expectation_suite_name": "default"
        },
        results=[
            ExpectationValidationResult(
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_column_unique_value_count_to_be_between",
                    kwargs={
                        "column": "patient_nbr",
                        "min": 2048,
                        "max": 2048
                    }
                ),
                success=True,
                exception_info={
                    "exception_message": None,
                    "exception_traceback": None,
                    "raised_exception": False},
                result={
                    "observed_value": 2048,
                    "element_count": 5000,
                    "missing_percent": 0.0,
                    "missing_count": 0
                }
            )
        ],
        success=True
    )

    data_context._extract_and_store_parameters_from_validation_results(
        source_diabetes_data_results,
        data_asset_name=dataAssetIdentifierSchema.load(source_diabetes_data_results.meta["data_asset_name"]).data,
        expectation_suite_name=source_diabetes_data_results.meta["expectation_suite_name"],
        run_id=run_id,
    )
    bound_parameters = data_context.evaluation_parameter_store.get_bind_params(run_id)
    assert bound_parameters == {
        'urn:great_expectations:validations:mydatasource/mygenerator/source_patient_data:default:expectations:expect_table_row_count_to_equal:observed_value': 1024,
        'urn:great_expectations:validations:mydatasource/mygenerator/source_diabetes_data:default:expectations:expect_column_unique_value_count_to_be_between:columns:patient_nbr:observed_value': 2048
    }


def test_database_evaluation_parameter_store_basics(param_store):
    run_id = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")
    metric_identifier = ExpectationDefinedMetricIdentifier(
        run_id=run_id,
        data_asset_name=DataAssetIdentifier(
            datasource="mysource",
            generator="mygenerator",
            generator_asset="asset"
        ),
        expectation_suite_name="warning",
        expectation_type="expect_column_values_to_match_regex",
        metric_name="unexpected_percent",
        metric_kwargs={
            "column": "mycol"
        }
    )
    metric_value = 12.3456789

    param_store.set(metric_identifier, metric_value)
    value = param_store.get(metric_identifier)
    assert value == metric_value


def test_database_evaluation_parameter_store_get_bind_params(param_store):
    # Bind params must be expressed as a string-keyed dictionary.
    # Verify that the param_store supports that
    run_id = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")
    metric_identifier = ExpectationDefinedMetricIdentifier(
        run_id=run_id,
        data_asset_name=DataAssetIdentifier(
            datasource="mysource",
            generator="mygenerator",
            generator_asset="asset"
        ),
        expectation_suite_name="warning",
        expectation_type="expect_column_values_to_match_regex",
        metric_name="unexpected_percent",
        metric_kwargs={
            "column": "mycol",
            "regex": r"^[123]+$"
        }
    )
    metric_value = 12.3456789
    param_store.set(metric_identifier, metric_value)

    metric_identifier = ExpectationDefinedMetricIdentifier(
        run_id=run_id,
        data_asset_name=DataAssetIdentifier(
            datasource="mysource",
            generator="mygenerator",
            generator_asset="asset"
        ),
        expectation_suite_name="warning",
        expectation_type="expect_table_row_count_to_be_between",
        metric_name="observed_value",
        metric_kwargs={}
    )
    metric_value = 512
    param_store.set(metric_identifier, metric_value)

    metric_identifier = ExpectationDefinedMetricIdentifier(
        run_id=run_id,
        data_asset_name=DataAssetIdentifier(
            datasource="mysource",
            generator="mygenerator",
            generator_asset="asset2"
        ),
        expectation_suite_name="warning",
        expectation_type="expect_column_values_to_match_regex",
        metric_name="unexpected_percent",
        metric_kwargs={
            "regex": r"^[123]+$",
            "column": "mycol"
        }
    )
    metric_value = 12.3456789
    param_store.set(metric_identifier, metric_value)

    params = param_store.get_bind_params(run_id)
    assert params == {
        'urn:great_expectations:validations:mysource/mygenerator/asset:warning:expectations'
        ':expect_column_values_to_match_regex:columns:mycol:unexpected_percent': 12.3456789,
        'urn:great_expectations:validations:mysource/mygenerator/asset:warning:expectations'
        ':expect_table_row_count_to_be_between:observed_value': 512,
        'urn:great_expectations:validations:mysource/mygenerator/asset2:warning:expectations'
        ':expect_column_values_to_match_regex:columns:mycol:unexpected_percent': 12.3456789,
    }
