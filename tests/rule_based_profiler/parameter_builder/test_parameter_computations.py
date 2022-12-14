import datetime
from typing import Dict, List, Union

import numpy as np
import pandas as pd
import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import IDDict
from great_expectations.rule_based_profiler.attributed_resolved_metrics import (
    AttributedResolvedMetrics,
)
from great_expectations.rule_based_profiler.estimators.bootstrap_numeric_range_estimator import (
    DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
)
from great_expectations.rule_based_profiler.estimators.numeric_range_estimation_result import (
    NumericRangeEstimationResult,
)
from great_expectations.rule_based_profiler.helpers.util import (
    compute_bootstrap_quantiles_point_estimate,
    sanitize_parameter_name,
)

# Allowable tolerance for how closely a bootstrap method approximates the sample
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
)

EFFICACY_TOLERANCE: float = 1.0e-2

# Measure of "closeness" between "actual" and "desired" is computed as: atol + rtol * abs(desired)
# (see "https://numpy.org/doc/stable/reference/generated/numpy.testing.assert_allclose.html" for details).
RTOL: float = 1.0e-7
ATOL: float = 1.0e-2


@pytest.mark.integration
@pytest.mark.slow  # 6.20s
def test_bootstrap_point_estimate_efficacy(
    bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive,
):
    """
    Efficacy means the custom bootstrap mean method approximates the sample +/- efficacy tolerance
    """
    false_positive_rate: np.float64 = (
        bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive[
            "false_positive_rate"
        ]
    )
    distribution_samples: pd.DataFrame = (
        bootstrap_distribution_parameters_and_1000_samples_with_01_false_positive[
            "distribution_samples"
        ]
    )

    distribution_types: pd.Index = distribution_samples.columns
    distribution: str
    numeric_range_estimation_result: NumericRangeEstimationResult
    lower_quantile_point_estimate: np.float64
    upper_quantile_point_estimate: np.float64
    actual_false_positive_rates: Dict[str, Union[float, np.float64]] = {}
    for distribution in distribution_types:
        numeric_range_estimation_result = compute_bootstrap_quantiles_point_estimate(
            metric_values=distribution_samples[distribution],
            false_positive_rate=false_positive_rate,
            n_resamples=DEFAULT_BOOTSTRAP_NUM_RESAMPLES,
            quantile_statistic_interpolation_method="linear",
            quantile_bias_correction=True,
            quantile_bias_std_error_ratio_threshold=2.5e-1,
        )
        lower_quantile_point_estimate = numeric_range_estimation_result.value_range[0]
        upper_quantile_point_estimate = numeric_range_estimation_result.value_range[1]
        actual_false_positive_rates[distribution] = (
            1.0
            - np.sum(
                distribution_samples[distribution].between(
                    lower_quantile_point_estimate, upper_quantile_point_estimate
                )
            )
            / distribution_samples.shape[0]
        )
        # Actual false-positives must be within the efficacy tolerance of desired (configured)
        # false_positive_rate parameter value.
        np.testing.assert_allclose(
            actual=actual_false_positive_rates[distribution],
            desired=false_positive_rate,
            rtol=RTOL,
            atol=EFFICACY_TOLERANCE,
            err_msg=f"Actual value of {actual_false_positive_rates[distribution]} differs from expected value of {false_positive_rate} by more than {ATOL + EFFICACY_TOLERANCE * abs(actual_false_positive_rates[distribution])} tolerance.",
        )


def test_sanitize_parameter_name(
    table_row_count_metric_config,
    table_row_count_aggregate_fn_metric_config,
    table_head_metric_config,
    column_histogram_metric_config,
):
    sanitized_name: str

    sanitized_name = sanitize_parameter_name(
        name=table_row_count_metric_config.metric_name,
        suffix=None,
    )
    assert sanitized_name == "table_row_count"

    sanitized_name = sanitize_parameter_name(
        name=table_row_count_aggregate_fn_metric_config.metric_name,
        suffix=None,
    )
    assert sanitized_name == "table_row_count_aggregate_fn"

    sanitized_name = sanitize_parameter_name(
        name=table_head_metric_config.metric_name,
        suffix=table_head_metric_config.metric_value_kwargs_id,
    )
    assert sanitized_name == "table_head_444fc52627dde82ad1d5c4fe290bfa6b"

    table_head_metric_config._metric_value_kwargs = IDDict({})
    sanitized_name = sanitize_parameter_name(
        name=table_head_metric_config.metric_name,
        suffix=table_head_metric_config.metric_value_kwargs_id,
    )
    assert sanitized_name == "table_head"

    sanitized_name = sanitize_parameter_name(
        name=column_histogram_metric_config.metric_name,
        suffix=column_histogram_metric_config.metric_value_kwargs_id,
    )
    assert sanitized_name == "column_histogram_f78021ff11b53f9d3588655b2b8b4f3e"


@pytest.mark.parametrize(
    "metric_name,metric_values_by_batch_id,",
    [
        pytest.param(
            "my_metric_0",
            {
                "batch_id_0": 1.3,
                "batch_id_1": 1.69e2,
                "batch_id_2": 3.38e2,
                "batch_id_3": 6.5e1,
            },
        ),
        pytest.param(
            "my_metric_1",
            {
                "batch_id_0": None,
                "batch_id_1": None,
                "batch_id_2": None,
                "batch_id_3": None,
            },
        ),
        pytest.param(
            "my_metric_2",
            {
                "batch_id_0": [
                    1.3,
                    3.9,
                    9.1,
                ],
                "batch_id_1": [
                    1.69e2,
                    3.38e2,
                    1.014e3,
                ],
                "batch_id_2": [
                    3.38e2,
                    1.014e3,
                    5.07e3,
                ],
                "batch_id_3": [
                    1.3e1,
                    2.6e1,
                    6.5e1,
                ],
            },
        ),
        pytest.param(
            "my_metric_3",
            {
                "batch_id_0": [
                    1.3,
                    3.9,
                    9.1,
                ],
                "batch_id_1": [
                    None,
                    None,
                    None,
                ],
                "batch_id_2": [
                    3.38e2,
                    1.014e3,
                    5.07e3,
                ],
                "batch_id_3": [
                    None,
                    None,
                    None,
                ],
            },
        ),
        pytest.param(
            "my_metric_4",
            {
                "batch_id_0": datetime.datetime(2019, 1, 4, 0, 12, 12),
                "batch_id_1": datetime.datetime(2019, 2, 21, 0, 12, 12),
                "batch_id_2": datetime.datetime(2019, 3, 20, 0, 12, 12),
                "batch_id_3": datetime.datetime(2019, 7, 13, 0, 12, 12),
            },
        ),
        pytest.param(
            "my_metric_5",
            {
                "batch_id_0": "2019-01-04T00:12:12",
                "batch_id_1": "2019-02-21T00:12:12",
                "batch_id_2": "2019-03-20T00:12:12",
                "batch_id_3": "2019-07-13T00:12:12",
            },
        ),
        pytest.param(
            "my_metric_6",
            {
                "batch_id_0": None,
                "batch_id_1": "unparseable_metric_value_0",
                "batch_id_2": "unparseable_metric_value_1",
                "batch_id_3": None,
            },
        ),
    ],
)
@pytest.mark.unit
def test_sanitize_metric_computation(metric_name: str, metric_values_by_batch_id: dict):
    batch_ids: List[str] = list(metric_values_by_batch_id.keys())
    attributed_resolved_metrics = AttributedResolvedMetrics(
        batch_ids=batch_ids,
        metric_attributes=None,
        metric_values_by_batch_id=None,
    )

    batch_id: str
    for batch_id in batch_ids:
        attributed_resolved_metrics.add_resolved_metric(
            batch_id=batch_id,
            value=metric_values_by_batch_id[batch_id],
        )

    enforce_numeric_metric: bool = True
    replace_nan_with_zero: bool = True
    reduce_scalar_metric: bool = True

    metric_multi_batch_parameter_builder: MetricMultiBatchParameterBuilder = (
        MetricMultiBatchParameterBuilder(
            name="my_parameter_builder",
            metric_name=metric_name,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            single_batch_mode=False,
            enforce_numeric_metric=enforce_numeric_metric,
            replace_nan_with_zero=replace_nan_with_zero,
            reduce_scalar_metric=reduce_scalar_metric,
            evaluation_parameter_builder_configs=None,
            data_context=None,
        )
    )
    if metric_name == "my_metric_6":
        with pytest.raises(ge_exceptions.ProfilerExecutionError) as excinfo:
            metric_multi_batch_parameter_builder._sanitize_metric_computation(
                parameter_builder=metric_multi_batch_parameter_builder,
                metric_name=metric_name,
                attributed_resolved_metrics=attributed_resolved_metrics,
                enforce_numeric_metric=metric_multi_batch_parameter_builder.enforce_numeric_metric,
                replace_nan_with_zero=metric_multi_batch_parameter_builder.replace_nan_with_zero,
                domain=None,
                variables=None,
                parameters=None,
            )

        assert (
            """Applicability of MetricMultiBatchParameterBuilder is restricted to numeric-valued and datetime-valued metrics (value unparseable_metric_value_0 of type "<class 'numpy.str_'>" was computed)."""
            in str(excinfo.value)
        )
    else:
        try:
            metric_multi_batch_parameter_builder._sanitize_metric_computation(
                parameter_builder=metric_multi_batch_parameter_builder,
                metric_name=metric_name,
                attributed_resolved_metrics=attributed_resolved_metrics,
                enforce_numeric_metric=metric_multi_batch_parameter_builder.enforce_numeric_metric,
                replace_nan_with_zero=metric_multi_batch_parameter_builder.replace_nan_with_zero,
                domain=None,
                variables=None,
                parameters=None,
            )
        except ge_exceptions.ProfilerExecutionError:
            pytest.fail()
