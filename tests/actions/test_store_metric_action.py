from freezegun import freeze_time

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
    RunIdentifier,
)
from great_expectations.core.metric import ValidationMetricIdentifier
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.validation_operators.actions import StoreMetricsAction


@freeze_time("09/26/2019 13:42:41")
def test_StoreMetricsAction(basic_in_memory_data_context_for_validation_operator):
    action = StoreMetricsAction(
        data_context=basic_in_memory_data_context_for_validation_operator,
        requested_metrics={
            "*": [
                "statistics.evaluated_expectations",
                "statistics.successful_expectations",
            ]
        },
        target_store_name="metrics_store",
    )

    run_id = RunIdentifier(run_name="bar")

    validation_result = ExpectationSuiteValidationResult(
        success=False,
        meta={"expectation_suite_name": "foo", "run_id": run_id},
        statistics={"evaluated_expectations": 5, "successful_expectations": 3},
    )

    # Run the action and store our metrics
    action.run(
        validation_result,
        ValidationResultIdentifier.from_object(validation_result),
        data_asset=None,
    )

    validation_result = ExpectationSuiteValidationResult(
        success=False,
        meta={"expectation_suite_name": "foo.warning", "run_id": run_id},
        statistics={"evaluated_expectations": 8, "successful_expectations": 4},
    )

    action.run(
        validation_result,
        ValidationResultIdentifier.from_object(validation_result),
        data_asset=None,
    )

    assert (
        basic_in_memory_data_context_for_validation_operator.stores[
            "metrics_store"
        ].get(
            ValidationMetricIdentifier(
                run_id=run_id,
                data_asset_name=None,
                expectation_suite_identifier=ExpectationSuiteIdentifier("foo"),
                metric_name="statistics.evaluated_expectations",
                metric_kwargs_id=None,
            )
        )
        == 5
    )

    assert (
        basic_in_memory_data_context_for_validation_operator.stores[
            "metrics_store"
        ].get(
            ValidationMetricIdentifier(
                run_id=run_id,
                data_asset_name=None,
                expectation_suite_identifier=ExpectationSuiteIdentifier("foo"),
                metric_name="statistics.successful_expectations",
                metric_kwargs_id=None,
            )
        )
        == 3
    )

    assert (
        basic_in_memory_data_context_for_validation_operator.stores[
            "metrics_store"
        ].get(
            ValidationMetricIdentifier(
                run_id=run_id,
                data_asset_name=None,
                expectation_suite_identifier=ExpectationSuiteIdentifier("foo.warning"),
                metric_name="statistics.evaluated_expectations",
                metric_kwargs_id=None,
            )
        )
        == 8
    )

    assert (
        basic_in_memory_data_context_for_validation_operator.stores[
            "metrics_store"
        ].get(
            ValidationMetricIdentifier(
                run_id=run_id,
                data_asset_name=None,
                expectation_suite_identifier=ExpectationSuiteIdentifier("foo.warning"),
                metric_name="statistics.successful_expectations",
                metric_kwargs_id=None,
            )
        )
        == 4
    )


@freeze_time("09/26/2019 13:42:41")
def test_StoreMetricsAction_column_metric(
    basic_in_memory_data_context_for_validation_operator,
):
    action = StoreMetricsAction(
        data_context=basic_in_memory_data_context_for_validation_operator,
        requested_metrics={
            "*": [
                {
                    "column": {
                        "provider_id": [
                            "expect_column_values_to_be_unique.result.unexpected_count"
                        ]
                    }
                },
                "statistics.evaluated_expectations",
                "statistics.successful_expectations",
            ]
        },
        target_store_name="metrics_store",
    )

    run_id = RunIdentifier(run_name="bar")

    validation_result = ExpectationSuiteValidationResult(
        success=False,
        meta={"expectation_suite_name": "foo", "run_id": run_id},
        results=[
            ExpectationValidationResult(
                meta={},
                result={
                    "element_count": 10,
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_count": 7,
                    "unexpected_percent": 0.0,
                    "unexpected_percent_nonmissing": 0.0,
                    "partial_unexpected_list": [],
                },
                success=True,
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_unique",
                    kwargs={"column": "provider_id", "result_format": "BASIC"},
                ),
                exception_info=None,
            )
        ],
        statistics={"evaluated_expectations": 5, "successful_expectations": 3},
    )

    action.run(
        validation_result,
        ValidationResultIdentifier.from_object(validation_result),
        data_asset=None,
    )

    assert (
        basic_in_memory_data_context_for_validation_operator.stores[
            "metrics_store"
        ].get(
            ValidationMetricIdentifier(
                run_id=run_id,
                data_asset_name=None,
                expectation_suite_identifier=ExpectationSuiteIdentifier("foo"),
                metric_name="expect_column_values_to_be_unique.result.unexpected_count",
                metric_kwargs_id="column=provider_id",
            )
        )
        == 7
    )
