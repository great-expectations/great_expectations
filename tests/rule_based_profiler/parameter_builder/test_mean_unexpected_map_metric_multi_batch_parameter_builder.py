from typing import List, Optional

import pytest

from great_expectations.core.metric_function_types import (
    SummarizationMetricNameSuffixes,
)
from great_expectations.data_context import AbstractDataContext
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.parameter_builder import (
    MeanUnexpectedMapMetricMultiBatchParameterBuilder,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
)

# module level markers
pytestmark = pytest.mark.big


def test_instantiation_mean_unexpected_map_metric_multi_batch_parameter_builder(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: AbstractDataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    parameter_builder: ParameterBuilder = (  # noqa: F841
        MeanUnexpectedMapMetricMultiBatchParameterBuilder(
            name="my_name",
            map_metric_name="column_values.nonnull",
            total_count_parameter_builder_name="my_total_count",
            data_context=data_context,
        )
    )


def test_instantiation_mean_unexpected_map_metric_multi_batch_parameter_builder_required_arguments_absent(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: AbstractDataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    with pytest.raises(TypeError) as excinfo:
        _: ParameterBuilder = MeanUnexpectedMapMetricMultiBatchParameterBuilder(
            name="my_name",
            map_metric_name="column_values.nonnull",
            data_context=data_context,
        )

    assert (
        "__init__() missing 1 required positional argument: 'total_count_parameter_builder_name'"
        in str(excinfo.value)
    )

    with pytest.raises(TypeError) as excinfo:
        _: ParameterBuilder = MeanUnexpectedMapMetricMultiBatchParameterBuilder(
            name="my_name",
            total_count_parameter_builder_name="my_total_count",
            data_context=data_context,
        )

    assert (
        "__init__() missing 1 required positional argument: 'map_metric_name'"
        in str(excinfo.value)
    )


def test_mean_unexpected_map_metric_multi_batch_parameter_builder_bobby_check_serialized_keys_no_evaluation_parameter_builder_configs(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: AbstractDataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    mean_unexpected_map_metric_multi_batch_parameter_builder: ParameterBuilder = (
        MeanUnexpectedMapMetricMultiBatchParameterBuilder(
            name="my_pickup_datetime_count_values_unique_mean_unexpected_map_metric",
            map_metric_name="column_values.nonnull",
            total_count_parameter_builder_name="my_total_count",
            null_count_parameter_builder_name="my_null_count",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=None,
            data_context=data_context,
        )
    )

    # Note: "evaluation_parameter_builder_configs" is not one of "ParameterBuilder" formal property attributes.
    assert set(
        mean_unexpected_map_metric_multi_batch_parameter_builder.to_json_dict().keys()
    ) == {
        "class_name",
        "module_name",
        "name",
        "map_metric_name",
        "total_count_parameter_builder_name",
        "null_count_parameter_builder_name",
        "metric_domain_kwargs",
        "metric_value_kwargs",
        "evaluation_parameter_builder_configs",
    }


def test_mean_unexpected_map_metric_multi_batch_parameter_builder_bobby_check_serialized_keys_with_evaluation_parameter_builder_configs(
    bobby_columnar_table_multi_batch_deterministic_data_context,
):
    data_context: AbstractDataContext = (
        bobby_columnar_table_multi_batch_deterministic_data_context
    )

    my_total_count_metric_multi_batch_parameter_builder_config = ParameterBuilderConfig(
        module_name="great_expectations.rule_based_profiler.parameter_builder",
        class_name="MetricMultiBatchParameterBuilder",
        name="my_total_count",
        metric_name="table.row_count",
        metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
        metric_value_kwargs=None,
        enforce_numeric_metric=False,
        replace_nan_with_zero=False,
        reduce_scalar_metric=True,
        evaluation_parameter_builder_configs=None,
    )
    my_null_count_metric_multi_batch_parameter_builder_config = ParameterBuilderConfig(
        module_name="great_expectations.rule_based_profiler.parameter_builder",
        class_name="MetricMultiBatchParameterBuilder",
        name="my_null_count",
        metric_name=f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
        metric_value_kwargs=None,
        enforce_numeric_metric=False,
        replace_nan_with_zero=False,
        reduce_scalar_metric=True,
        evaluation_parameter_builder_configs=None,
    )

    evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
        my_total_count_metric_multi_batch_parameter_builder_config,
        my_null_count_metric_multi_batch_parameter_builder_config,
    ]
    mean_unexpected_map_metric_multi_batch_parameter_builder: ParameterBuilder = (
        MeanUnexpectedMapMetricMultiBatchParameterBuilder(
            name="my_pickup_datetime_count_values_unique_mean_unexpected_map_metric",
            map_metric_name="column_values.nonnull",
            total_count_parameter_builder_name="my_total_count",
            null_count_parameter_builder_name="my_null_count",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=data_context,
        )
    )

    # Note: "evaluation_parameter_builder_configs" is not one of "ParameterBuilder" formal property attributes.
    assert set(
        mean_unexpected_map_metric_multi_batch_parameter_builder.to_json_dict().keys()
    ) == {
        "class_name",
        "module_name",
        "name",
        "map_metric_name",
        "total_count_parameter_builder_name",
        "null_count_parameter_builder_name",
        "metric_domain_kwargs",
        "metric_value_kwargs",
        "evaluation_parameter_builder_configs",
    }
