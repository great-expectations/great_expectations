import logging
from typing import TYPE_CHECKING, Dict, Optional, Tuple

import altair as alt
import numpy as np
import pandas as pd
from scipy import stats

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_engine.util import (
    is_valid_categorical_partition_object,
    is_valid_partition_object,
)
from great_expectations.expectations.expectation import (
    ColumnAggregateExpectation,
    render_evaluation_parameter_string,
)
from great_expectations.render import (
    AtomicDiagnosticRendererType,
    AtomicPrescriptiveRendererType,
    LegacyDescriptiveRendererType,
    LegacyDiagnosticRendererType,
    LegacyRendererType,
    RenderedAtomicContent,
    RenderedContentBlockContainer,
    RenderedGraphContent,
    RenderedStringTemplateContent,
    renderedAtomicValueSchema,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererSchema,
    RendererTableValue,
    RendererValueType,
)
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.validator.computed_metric import MetricValue  # noqa: TCH001
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.metrics_calculator import MetricsCalculator
from great_expectations.validator.validator import (
    ValidationDependencies,
)

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import AddParamArgs

logger = logging.getLogger(__name__)
logging.captureWarnings(True)


class ExpectColumnKlDivergenceToBeLessThan(ColumnAggregateExpectation):
    """Expect the Kulback-Leibler (KL) divergence (relative entropy) of the specified column with respect to the partition object to be lower than the provided threshold.

    KL divergence compares two distributions. The higher the divergence value (relative entropy), the larger \
    the difference between the two distributions. A relative entropy of zero indicates that the data are \
    distributed identically, when binned according to the provided partition.

    In many practical contexts, choosing a value between 0.5 and 1 will provide a useful test.

    This expectation works on both categorical and continuous partitions. See notes below for details.

    expect_column_kl_divergence_to_be_less_than is a \
    [Column Aggregate Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations).

    Args:
        column (str): \
            The column name.
        partition_object (dict): \
            The expected partition object (see [partition_object](https://docs.greatexpectations.io/docs/reference/expectations/distributional_expectations/#partition-objects)).
        threshold (float): \
            The maximum KL divergence to for which to return success=True. If KL divergence is larger than the \
            provided threshold, the test will return success=False.

    Keyword Args:
        internal_weight_holdout (float between 0 and 1 or None): \
            The amount of weight to split uniformly among zero-weighted partition bins. internal_weight_holdout \
            provides a mechanisms to make the test less strict by assigning positive weights to values \
            observed in \
            the data for which the partition explicitly expected zero weight. With no internal_weight_holdout, \
            any value observed in such a region will cause KL divergence to rise to +Infinity. Defaults to 0.
        tail_weight_holdout (float between 0 and 1 or None): \
            The amount of weight to add to the tails of the histogram. Tail weight holdout is split evenly \
            between (-Infinity, min(partition_object['bins'])) and (max(partition_object['bins']), +Infinity). \
            tail_weight_holdout provides a mechanism to make the test less strict by assigning positive \
            weights to \
            values observed in the data that are not present in the partition. With no tail_weight_holdout, \
            any value observed outside the provided partition_object will cause KL divergence to rise to +Infinity. \
            Defaults to 0.
        bucketize_data (boolean): If True, then continuous data will be bucketized before evaluation. Setting \
            this parameter to false allows evaluation of KL divergence with a None partition object for \
            profiling against discrete data.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        include_config (boolean): \
            If True, then include the expectation config as part of the result object.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, include_config, catch_exceptions, and meta.

    Notes:
        * observed_value field in the result object is customized for this expectation to be a float \
          representing the true KL divergence (relative entropy) or None if the value is calculated \
          as infinity, -infinity, or NaN
        * details.observed_partition in the result object is customized for this expectation to be a \
          dict representing the partition observed in the data
        * details.expected_partition in the result object is customized for this expectation to be a \
          dict representing the partition against which the data were compared, after applying \
          specified weight holdouts

        If the partition_object is categorical, this expectation will expect the values in column to also be \
        categorical.

        * If the column includes values that are not present in the partition, the tail_weight_holdout \
          will be equally split among those values, providing a mechanism to weaken the strictness of the \
          expectation (otherwise, relative entropy would immediately go to infinity).
        * If the partition includes values that are not present in the column, the test will simply include \
          zero weight for that value.

        If the partition_object is continuous, this expectation will discretize the values in the column according \
        to the bins specified in the partition_object, and apply the test to the resulting distribution.

        * The internal_weight_holdout and tail_weight_holdout parameters provide a mechanism to weaken the \
          expectation, since an expected weight of zero would drive relative entropy to be infinite if any data \
          are observed in that interval.
        * If internal_weight_holdout is specified, that value will be distributed equally among any intervals \
          with weight zero in the partition_object.
        * If tail_weight_holdout is specified, that value will be appended to the tails of the bins \
          ((-Infinity, min(bins)) and (max(bins), Infinity).

        If relative entropy/kl divergence goes to infinity for any of the reasons mentioned above, the observed \
        value will be set to None. This is because inf, -inf, Nan, are not json serializable and cause some json \
        parsers to crash when encountered. The python None token will be serialized to null in json.
    """

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "tags": [
            "core expectation",
            "column aggregate expectation",
            "distributional expectation",
        ],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    success_keys = (
        "partition_object",
        "threshold",
        "tail_weight_holdout",
        "internal_weight_holdout",
        "bucketize_data",
    )
    default_kwarg_values = {
        "partition_object": None,
        "threshold": None,
        "tail_weight_holdout": 0,
        "internal_weight_holdout": 0,
        "bucketize_data": True,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }
    args_keys = (
        "column",
        "partition_object",
        "threshold",
    )

    @public_api
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Validates configuration for the Expectation.

        For `expect_column_kl_divergence_to_be_less_than`, `configuraton.kwargs` may contain `min_value` and
        `max_value` whose value is either a number or date.

        The configuration will also be validated using each of the `validate_configuration` methods in its Expectation
        superclass hierarchy.

        Args:
            configuration: The configuration to be validated.

        Raises:
            InvalidExpectationConfigurationError: The configuraton does not contain the values required by the
                Expectation.
        """
        super().validate_configuration(configuration)
        self.validate_metric_value_between_configuration(configuration=configuration)

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationDependencies:
        validation_dependencies: ValidationDependencies = (
            super().get_validation_dependencies(
                configuration, execution_engine, runtime_configuration
            )
        )
        partition_object = configuration.kwargs["partition_object"]
        domain_kwargs = configuration.get_domain_kwargs()
        is_categorical = None
        bins = None
        if partition_object is None:
            if configuration.kwargs.get(
                "bucketize_data", self.default_kwarg_values["bucketize_data"]
            ):
                is_categorical = False
                partition_metric_configuration = MetricConfiguration(
                    metric_name="column.partition",
                    metric_domain_kwargs=domain_kwargs,
                    metric_value_kwargs={
                        "bins": "auto",
                        "allow_relative_error": False,
                    },
                )
                #
                # Note: 20201116 - JPC - the execution engine doesn't provide capability to evaluate
                # validation_dependencies, so we use a validator
                #
                metrics_calculator = MetricsCalculator(
                    execution_engine=execution_engine,
                    show_progress_bars=True,
                )
                resolved_metrics: Dict[
                    Tuple[str, str, str], MetricValue
                ] = metrics_calculator.compute_metrics(
                    metric_configurations=[partition_metric_configuration],
                    runtime_configuration=None,
                    min_graph_edges_pbar_enable=0,
                )

                bins = resolved_metrics[partition_metric_configuration.id]
                hist_metric_configuration = MetricConfiguration(
                    metric_name="column.histogram",
                    metric_domain_kwargs=domain_kwargs,
                    metric_value_kwargs={
                        "bins": tuple(bins),
                    },
                )
                nonnull_configuration = MetricConfiguration(
                    metric_name="column_values.nonnull.count",
                    metric_domain_kwargs=domain_kwargs,
                    metric_value_kwargs=None,
                )
                #
                # NOTE 20201117 - JPC - Would prefer not to include partition_metric_configuration here,
                # since we have already evaluated it, and its result is in the kwargs for the histogram.
                # However, currently the validation_dependencies' configurations are not passed to the _validate method
                #
                validation_dependencies.set_metric_configuration(
                    metric_name="column.partition",
                    metric_configuration=partition_metric_configuration,
                )
                validation_dependencies.set_metric_configuration(
                    metric_name="column.histogram",
                    metric_configuration=hist_metric_configuration,
                )
                validation_dependencies.set_metric_configuration(
                    metric_name="column_values.nonnull.count",
                    metric_configuration=nonnull_configuration,
                )
            else:
                is_categorical = True
                counts_configuration = MetricConfiguration(
                    metric_name="column.value_counts",
                    metric_domain_kwargs=domain_kwargs,
                    metric_value_kwargs={
                        "sort": "value",
                    },
                )
                nonnull_configuration = MetricConfiguration(
                    metric_name="column_values.nonnull.count",
                    metric_domain_kwargs=domain_kwargs,
                )
                validation_dependencies.set_metric_configuration(
                    metric_name="column.value_counts",
                    metric_configuration=counts_configuration,
                )
                validation_dependencies.set_metric_configuration(
                    metric_name="column_values.nonnull.count",
                    metric_configuration=nonnull_configuration,
                )
        if is_categorical is True or is_valid_categorical_partition_object(
            partition_object
        ):
            validation_dependencies.set_metric_configuration(
                metric_name="column.value_counts",
                metric_configuration=MetricConfiguration(
                    metric_name="column.value_counts",
                    metric_domain_kwargs=domain_kwargs,
                    metric_value_kwargs={
                        "sort": "value",
                    },
                ),
            )
            validation_dependencies.set_metric_configuration(
                metric_name="column_values.nonnull.count",
                metric_configuration=MetricConfiguration(
                    metric_name="column_values.nonnull.count",
                    metric_domain_kwargs=domain_kwargs,
                    metric_value_kwargs=None,
                ),
            )
        else:
            if (
                bins is None
            ):  # if the user did not supply a partition_object, so we just computed it
                if not is_valid_partition_object(partition_object):
                    raise ValueError("Invalid partition_object provided")
                bins = partition_object["bins"]

            hist_metric_configuration = MetricConfiguration(
                metric_name="column.histogram",
                metric_domain_kwargs=domain_kwargs,
                metric_value_kwargs={
                    "bins": bins,
                },
            )
            validation_dependencies.set_metric_configuration(
                metric_name="column.histogram",
                metric_configuration=hist_metric_configuration,
            )
            nonnull_configuration = MetricConfiguration(
                metric_name="column_values.nonnull.count",
                metric_domain_kwargs=domain_kwargs,
                metric_value_kwargs=None,
            )
            validation_dependencies.set_metric_configuration(
                metric_name="column_values.nonnull.count",
                metric_configuration=nonnull_configuration,
            )
            below_partition = MetricConfiguration(
                metric_name="column_values.between.count",
                metric_domain_kwargs=domain_kwargs,
                metric_value_kwargs={"max_value": bins[0], "strict_max": True},
            )
            validation_dependencies.set_metric_configuration(
                metric_name="below_partition", metric_configuration=below_partition
            )
            above_partition = MetricConfiguration(
                metric_name="column_values.between.count",
                metric_domain_kwargs=domain_kwargs,
                metric_value_kwargs={"min_value": bins[-1], "strict_min": True},
            )
            validation_dependencies.set_metric_configuration(
                metric_name="above_partition", metric_configuration=above_partition
            )

        return validation_dependencies

    def _validate(  # noqa: C901, PLR0912, PLR0915
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        bucketize_data = configuration.kwargs.get(
            "bucketize_data", self.default_kwarg_values["bucketize_data"]
        )
        partition_object = configuration.kwargs.get(
            "partition_object", self.default_kwarg_values["partition_object"]
        )
        threshold = configuration.kwargs.get(
            "threshold", self.default_kwarg_values["threshold"]
        )
        tail_weight_holdout = configuration.kwargs.get(
            "tail_weight_holdout", self.default_kwarg_values["tail_weight_holdout"]
        )
        internal_weight_holdout = configuration.kwargs.get(
            "internal_weight_holdout",
            self.default_kwarg_values["internal_weight_holdout"],
        )
        if partition_object is None:
            if bucketize_data:
                # in this case, we have requested a partition, histogram using said partition, and nonnull count
                bins = list(metrics["column.partition"])
                weights = list(
                    np.array(metrics["column.histogram"])
                    / metrics["column_values.nonnull.count"]
                )
                tail_weights = (1 - sum(weights)) / 2
                partition_object = {
                    "bins": bins,
                    "weights": weights,
                    "tail_weights": [tail_weights, tail_weights],
                }
            else:
                partition_object = {
                    "values": list(metrics["column.value_counts"].index),
                    "weights": list(
                        np.array(metrics["column.value_counts"])
                        / metrics["column_values.nonnull.count"]
                    ),
                }

        if not is_valid_partition_object(partition_object):
            raise ValueError("Invalid partition object.")

        if threshold is not None and (
            (not isinstance(threshold, (int, float))) or (threshold < 0)
        ):
            raise ValueError(
                "Threshold must be specified, greater than or equal to zero."
            )

        if (
            (not isinstance(tail_weight_holdout, (int, float)))
            or (tail_weight_holdout < 0)
            or (tail_weight_holdout > 1)
        ):
            raise ValueError("tail_weight_holdout must be between zero and one.")

        if (
            (not isinstance(internal_weight_holdout, (int, float)))
            or (internal_weight_holdout < 0)
            or (internal_weight_holdout > 1)
        ):
            raise ValueError("internal_weight_holdout must be between zero and one.")

        if tail_weight_holdout != 0 and "tail_weights" in partition_object:
            raise ValueError(
                "tail_weight_holdout must be 0 when using tail_weights in partition object"
            )

        # TODO: add checks for duplicate values in is_valid_categorical_partition_object
        if is_valid_categorical_partition_object(partition_object):
            if internal_weight_holdout > 0:
                raise ValueError(
                    "Internal weight holdout cannot be used for discrete data."
                )

            # Data are expected to be discrete, use value_counts
            observed_weights = (
                metrics["column.value_counts"] / metrics["column_values.nonnull.count"]
            )
            expected_weights = pd.Series(
                partition_object["weights"],
                index=partition_object["values"],
                name="expected",
            )
            # Sort not available before pandas 0.23.0
            # test_df = pd.concat([expected_weights, observed_weights], axis=1, sort=True)
            test_df = pd.concat([expected_weights, observed_weights], axis=1)

            na_counts = test_df.isnull().sum()

            # Handle NaN: if we expected something that's not there, it's just not there.
            pk = test_df["count"].fillna(0)
            # Handle NaN: if something's there that was not expected,
            # substitute the relevant value for tail_weight_holdout
            if na_counts["expected"] > 0:
                # Scale existing expected values
                test_df["expected"] *= 1 - tail_weight_holdout
                # Fill NAs with holdout.
                qk = test_df["expected"].fillna(
                    tail_weight_holdout / na_counts["expected"]
                )
            else:
                qk = test_df["expected"]

            kl_divergence = stats.entropy(pk, qk)

            if np.isinf(kl_divergence) or np.isnan(kl_divergence):
                observed_value = None
            else:
                observed_value = kl_divergence

            if threshold is None:
                success = True
            else:
                success = kl_divergence <= threshold

            return_obj = {
                "success": success,
                "result": {
                    "observed_value": observed_value,
                    "details": {
                        "observed_partition": {
                            "values": test_df.index.tolist(),
                            "weights": pk.tolist(),
                        },
                        "expected_partition": {
                            "values": test_df.index.tolist(),
                            "weights": qk.tolist(),
                        },
                    },
                },
            }

        else:
            # Data are expected to be continuous; discretize first
            if bucketize_data is False:
                raise ValueError(
                    "KL Divergence cannot be computed with a continuous partition object and the bucketize_data "
                    "parameter set to false."
                )
            # Build the histogram first using expected bins so that the largest bin is >=
            nonnull_count = metrics["column_values.nonnull.count"]
            hist = np.array(metrics["column.histogram"])

            # Add in the frequencies observed above or below the provided partition
            below_partition = metrics["below_partition"]
            above_partition = metrics["above_partition"]

            # Observed Weights is just the histogram values divided by the total number of observations
            observed_weights = hist / nonnull_count

            # Adjust expected_weights to account for tail_weight and internal_weight
            if "tail_weights" in partition_object:
                partition_tail_weight_holdout = np.sum(partition_object["tail_weights"])
            else:
                partition_tail_weight_holdout = 0  # noqa: F841 # unused

            expected_weights = np.array(partition_object["weights"]) * (
                1 - tail_weight_holdout - internal_weight_holdout
            )

            # Assign internal weight holdout values if applicable
            if internal_weight_holdout > 0:
                zero_count = len(expected_weights) - np.count_nonzero(expected_weights)
                if zero_count > 0:
                    for index, value in enumerate(expected_weights):
                        if value == 0:
                            expected_weights[index] = (
                                internal_weight_holdout / zero_count
                            )

            # Assign tail weight holdout if applicable
            # We need to check cases to only add tail weight holdout if it makes sense based on the provided partition.
            if (partition_object["bins"][0] == -np.inf) and (
                partition_object["bins"][-1]
            ) == np.inf:
                if tail_weight_holdout > 0:
                    raise ValueError(
                        "tail_weight_holdout cannot be used for partitions with infinite endpoints."
                    )
                if "tail_weights" in partition_object:
                    raise ValueError(
                        "There can be no tail weights for partitions with one or both endpoints at infinity"
                    )

                # Remove -inf and inf
                expected_bins = partition_object["bins"][1:-1]

                comb_expected_weights = expected_weights
                # Set aside tail weights
                expected_tail_weights = np.concatenate(
                    ([expected_weights[0]], [expected_weights[-1]])
                )
                # Remove tail weights
                expected_weights = expected_weights[1:-1]

                comb_observed_weights = observed_weights
                # Set aside tail weights
                observed_tail_weights = np.concatenate(
                    ([observed_weights[0]], [observed_weights[-1]])
                )
                # Remove tail weights
                observed_weights = observed_weights[1:-1]

            elif partition_object["bins"][0] == -np.inf:
                if "tail_weights" in partition_object:
                    raise ValueError(
                        "There can be no tail weights for partitions with one or both endpoints at infinity"
                    )

                # Remove -inf
                expected_bins = partition_object["bins"][1:]

                comb_expected_weights = np.concatenate(
                    (expected_weights, [tail_weight_holdout])
                )
                # Set aside left tail weight and holdout
                expected_tail_weights = np.concatenate(
                    ([expected_weights[0]], [tail_weight_holdout])
                )
                # Remove left tail weight from main expected_weights
                expected_weights = expected_weights[1:]

                comb_observed_weights = np.concatenate(
                    (
                        observed_weights,
                        [above_partition / nonnull_count],
                    )
                )
                # Set aside left tail weight and above partition weight
                observed_tail_weights = np.concatenate(
                    (
                        [observed_weights[0]],
                        [above_partition / nonnull_count],
                    )
                )
                # Remove left tail weight from main observed_weights
                observed_weights = observed_weights[1:]

            elif partition_object["bins"][-1] == np.inf:
                if "tail_weights" in partition_object:
                    raise ValueError(
                        "There can be no tail weights for partitions with one or both endpoints at infinity"
                    )

                # Remove inf
                expected_bins = partition_object["bins"][:-1]

                comb_expected_weights = np.concatenate(
                    ([tail_weight_holdout], expected_weights)
                )
                # Set aside right tail weight and holdout
                expected_tail_weights = np.concatenate(
                    ([tail_weight_holdout], [expected_weights[-1]])
                )
                # Remove right tail weight from main expected_weights
                expected_weights = expected_weights[:-1]

                comb_observed_weights = np.concatenate(
                    (
                        [below_partition / nonnull_count],
                        observed_weights,
                    )
                )
                # Set aside right tail weight and below partition weight
                observed_tail_weights = np.concatenate(
                    (
                        [below_partition / nonnull_count],
                        [observed_weights[-1]],
                    )
                )
                # Remove right tail weight from main observed_weights
                observed_weights = observed_weights[:-1]
            else:
                # No need to remove -inf or inf
                expected_bins = partition_object["bins"]

                if "tail_weights" in partition_object:
                    tail_weights = partition_object["tail_weights"]
                    # Tack on tail weights
                    comb_expected_weights = np.concatenate(
                        ([tail_weights[0]], expected_weights, [tail_weights[1]])
                    )
                    # Tail weights are just tail_weights
                    expected_tail_weights = np.array(tail_weights)
                else:
                    comb_expected_weights = np.concatenate(
                        (
                            [tail_weight_holdout / 2],
                            expected_weights,
                            [tail_weight_holdout / 2],
                        )
                    )
                    # Tail weights are just tail_weight holdout divided equally to both tails
                    expected_tail_weights = np.concatenate(
                        ([tail_weight_holdout / 2], [tail_weight_holdout / 2])
                    )

                comb_observed_weights = np.concatenate(
                    (
                        [below_partition / nonnull_count],
                        observed_weights,
                        [above_partition / nonnull_count],
                    )
                )
                # Tail weights are just the counts on either side of the partition
                observed_tail_weights = (
                    np.concatenate(([below_partition], [above_partition]))
                    / nonnull_count
                )

                # Main expected_weights and main observed weights had no tail_weights, so nothing needs to be removed.

            # TODO: VERIFY THAT THIS STILL WORKS BASED ON CHANGE TO HIST
            # comb_expected_weights = np.array(comb_expected_weights).astype(float)
            # comb_observed_weights = np.array(comb_observed_weights).astype(float)

            kl_divergence = stats.entropy(comb_observed_weights, comb_expected_weights)

            if np.isinf(kl_divergence) or np.isnan(kl_divergence):
                observed_value = None
            else:
                observed_value = kl_divergence

            if threshold is None:
                success = True
            else:
                success = kl_divergence <= threshold

            return_obj = {
                "success": success,
                "result": {
                    "observed_value": observed_value,
                    "details": {
                        "observed_partition": {
                            # return expected_bins, since we used those bins to compute the observed_weights
                            "bins": expected_bins,
                            "weights": observed_weights.tolist(),
                            "tail_weights": observed_tail_weights.tolist(),
                        },
                        "expected_partition": {
                            "bins": expected_bins,
                            "weights": expected_weights.tolist(),
                            "tail_weights": expected_tail_weights.tolist(),
                        },
                    },
                },
            }

        return return_obj

    @classmethod
    def _get_kl_divergence_chart(cls, partition_object, header=None):  # noqa: PLR0912
        weights = partition_object["weights"]

        if len(weights) > 60:  # noqa: PLR2004
            expected_distribution = cls._get_kl_divergence_partition_object_table(
                partition_object, header=header
            )
        else:
            chart_pixel_width = (len(weights) / 60.0) * 500
            if chart_pixel_width < 250:  # noqa: PLR2004
                chart_pixel_width = 250
            chart_container_col_width = round((len(weights) / 60.0) * 6)
            if chart_container_col_width < 4:  # noqa: PLR2004
                chart_container_col_width = 4
            elif chart_container_col_width >= 5:  # noqa: PLR2004
                chart_container_col_width = 6
            elif chart_container_col_width >= 4:  # noqa: PLR2004
                chart_container_col_width = 5

            mark_bar_args = {}
            if len(weights) == 1:
                mark_bar_args["size"] = 20

            if partition_object.get("bins"):
                bins = partition_object["bins"]
                bins_x1 = [round(value, 1) for value in bins[:-1]]
                bins_x2 = [round(value, 1) for value in bins[1:]]

                df = pd.DataFrame(
                    {
                        "bin_min": bins_x1,
                        "bin_max": bins_x2,
                        "fraction": weights,
                    }
                )

                bars = (
                    alt.Chart(df)
                    .mark_bar()
                    .encode(
                        x="bin_min:O",
                        x2="bin_max:O",
                        y="fraction:Q",
                        tooltip=["bin_min", "bin_max", "fraction"],
                    )
                    .properties(width=chart_pixel_width, height=400, autosize="fit")
                )

                chart = bars.to_dict()
            elif partition_object.get("values"):
                is_boolean_list = all(
                    isinstance(value, bool) for value in partition_object["values"]
                )
                if is_boolean_list:
                    values = [str(value) for value in partition_object["values"]]
                else:
                    values = partition_object["values"]

                df = pd.DataFrame({"values": values, "fraction": weights})

                bars = (
                    alt.Chart(df)
                    .mark_bar()
                    .encode(
                        x="values:N", y="fraction:Q", tooltip=["values", "fraction"]
                    )
                    .properties(width=chart_pixel_width, height=400, autosize="fit")
                )
                chart = bars.to_dict()

            if header:
                expected_distribution = RenderedGraphContent(
                    **{
                        "content_block_type": "graph",
                        "graph": chart,
                        "header": header,
                        "styling": {
                            "classes": [
                                f"col-{str(chart_container_col_width)}",
                                "mt-2",
                                "pl-1",
                                "pr-1",
                            ],
                            "parent": {"styles": {"list-style-type": "none"}},
                        },
                    }
                )
            else:
                expected_distribution = RenderedGraphContent(
                    **{
                        "content_block_type": "graph",
                        "graph": chart,
                        "styling": {
                            "classes": [
                                f"col-{str(chart_container_col_width)}",
                                "mt-2",
                                "pl-1",
                                "pr-1",
                            ],
                            "parent": {"styles": {"list-style-type": "none"}},
                        },
                    }
                )
        return expected_distribution

    @classmethod
    def _atomic_kl_divergence_chart_template(cls, partition_object: dict) -> tuple:
        weights = partition_object.get("weights", [])

        chart_pixel_width = (len(weights) / 60.0) * 500
        if chart_pixel_width < 250:  # noqa: PLR2004
            chart_pixel_width = 250
        chart_container_col_width = round((len(weights) / 60.0) * 6)
        if chart_container_col_width < 4:  # noqa: PLR2004
            chart_container_col_width = 4
        elif chart_container_col_width >= 5:  # noqa: PLR2004
            chart_container_col_width = 6
        elif chart_container_col_width >= 4:  # noqa: PLR2004
            chart_container_col_width = 5

        mark_bar_args = {}
        if len(weights) == 1:
            mark_bar_args["size"] = 20

        chart = {}
        if partition_object.get("bins"):
            bins = partition_object["bins"]
            bins_x1 = [round(value, 1) for value in bins[:-1]]
            bins_x2 = [round(value, 1) for value in bins[1:]]

            df = pd.DataFrame(
                {
                    "bin_min": bins_x1,
                    "bin_max": bins_x2,
                    "fraction": weights,
                }
            )

            bars = (
                alt.Chart(df)
                .mark_bar()
                .encode(
                    x="bin_min:O",
                    x2="bin_max:O",
                    y="fraction:Q",
                    tooltip=["bin_min", "bin_max", "fraction"],
                )
                .properties(width=chart_pixel_width, height=400, autosize="fit")
            )

            chart = bars.to_dict()
        elif partition_object.get("values"):
            is_boolean_list = all(
                isinstance(value, bool) for value in partition_object["values"]
            )
            if is_boolean_list:
                values = [str(value) for value in partition_object["values"]]
            else:
                values = partition_object["values"]

            df = pd.DataFrame({"values": values, "fraction": weights})

            bars = (
                alt.Chart(df)
                .mark_bar()
                .encode(x="values:N", y="fraction:Q", tooltip=["values", "fraction"])
                .properties(width=chart_pixel_width, height=400, autosize="fit")
            )
            chart = bars.to_dict()

        return chart, chart_container_col_width

    @classmethod
    def _get_kl_divergence_partition_object_table(cls, partition_object, header=None):
        table_rows = []
        fractions = partition_object["weights"]

        if partition_object.get("bins"):
            bins = partition_object["bins"]

            for idx, fraction in enumerate(fractions):
                if idx == len(fractions) - 1:
                    table_rows.append(
                        [
                            f"[{num_to_str(bins[idx])} - {num_to_str(bins[idx + 1])}]",
                            num_to_str(fraction),
                        ]
                    )
                else:
                    table_rows.append(
                        [
                            f"[{num_to_str(bins[idx])} - {num_to_str(bins[idx + 1])})",
                            num_to_str(fraction),
                        ]
                    )
        else:
            values = partition_object["values"]
            table_rows = [
                [value, num_to_str(fractions[idx])] for idx, value in enumerate(values)
            ]

        if header:
            return {
                "content_block_type": "table",
                "header": header,
                "header_row": ["Interval", "Fraction"]
                if partition_object.get("bins")
                else ["Value", "Fraction"],
                "table": table_rows,
                "styling": {
                    "classes": ["table-responsive"],
                    "body": {
                        "classes": [
                            "table",
                            "table-sm",
                            "table-bordered",
                            "mt-2",
                            "mb-2",
                        ],
                    },
                    "parent": {
                        "classes": ["show-scrollbars", "p-2"],
                        "styles": {
                            "list-style-type": "none",
                            "overflow": "auto",
                            "max-height": "80vh",
                        },
                    },
                },
            }
        else:
            return {
                "content_block_type": "table",
                "header_row": ["Interval", "Fraction"]
                if partition_object.get("bins")
                else ["Value", "Fraction"],
                "table": table_rows,
                "styling": {
                    "classes": ["table-responsive"],
                    "body": {
                        "classes": [
                            "table",
                            "table-sm",
                            "table-bordered",
                            "mt-2",
                            "mb-2",
                        ],
                    },
                    "parent": {
                        "classes": ["show-scrollbars", "p-2"],
                        "styles": {
                            "list-style-type": "none",
                            "overflow": "auto",
                            "max-height": "80vh",
                        },
                    },
                },
            }

    @classmethod
    def _atomic_partition_object_table_template(cls, partition_object: dict):
        table_rows = []
        fractions = partition_object["weights"]

        if partition_object.get("bins"):
            bins = partition_object["bins"]

            for idx, fraction in enumerate(fractions):
                interval_start = num_to_str(bins[idx])
                interval_end = num_to_str(bins[idx + 1])
                interval_closing_symbol = "]" if idx == (len(fractions) - 1) else ")"
                table_rows.append(
                    [
                        RendererTableValue(
                            schema=RendererSchema(type=RendererValueType.STRING),
                            value=f"[{interval_start} - {interval_end}{interval_closing_symbol}",
                        ),
                        RendererTableValue(
                            schema=RendererSchema(type=RendererValueType.STRING),
                            value=num_to_str(fraction),
                        ),
                    ]
                )
        else:
            values = partition_object["values"]
            table_rows = [
                [
                    RendererTableValue(
                        schema=RendererSchema(type=RendererValueType.STRING),
                        value=str(value),
                    ),
                    RendererTableValue(
                        schema=RendererSchema(type=RendererValueType.STRING),
                        value=num_to_str(fractions[idx]),
                    ),
                ]
                for idx, value in enumerate(values)
            ]

        interval_or_value = "Interval" if partition_object.get("bins") else "Value"

        header_row = [
            RendererTableValue(
                schema=RendererSchema(type=RendererValueType.STRING),
                value=interval_or_value,
            ),
            RendererTableValue(
                schema=RendererSchema(type=RendererValueType.STRING), value="Fraction"
            ),
        ]

        return header_row, table_rows

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("mostly", RendererValueType.NUMBER),
            ("threshold", RendererValueType.NUMBER),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        expected_partition_object = renderer_configuration.kwargs.get(
            "partition_object", {}
        )
        weights = expected_partition_object.get("weights", [])

        # generate template string for header
        if not expected_partition_object:
            template_str = "can match any distribution."
        else:
            template_str = (
                "Kullback-Leibler (KL) divergence with respect to the following distribution must be "
                "lower than $threshold."
            )

        # optionally, add column name
        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

        # generate table or chart depending on number of weights
        if len(weights) > 60:  # noqa: PLR2004
            (
                renderer_configuration.header_row,
                renderer_configuration.table,
            ) = cls._atomic_partition_object_table_template(
                partition_object=expected_partition_object
            )
        else:
            renderer_configuration.graph, _ = cls._atomic_kl_divergence_chart_template(
                partition_object=expected_partition_object
            )

        renderer_configuration.template_str = template_str

        return renderer_configuration

    @classmethod
    @renderer(renderer_type=AtomicPrescriptiveRendererType.SUMMARY)
    @render_evaluation_parameter_string
    def _prescriptive_summary(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> RenderedAtomicContent:
        renderer_configuration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )
        renderer_configuration = cls._prescriptive_template(
            renderer_configuration=renderer_configuration
        )

        if renderer_configuration.graph:
            value_obj = renderedAtomicValueSchema.load(
                {
                    "header": {
                        "schema": {"type": "StringValueType"},
                        "value": {
                            "template": renderer_configuration.template_str,
                            "params": renderer_configuration.params.dict(),
                        },
                    },
                    "graph": renderer_configuration.graph,
                    "meta_notes": renderer_configuration.meta_notes,
                    "schema": {"type": "GraphType"},
                }
            )
            value_type = "GraphType"
        else:
            header_row = [value.dict() for value in renderer_configuration.header_row]
            table = []
            for row in renderer_configuration.table:
                table.append([value.dict() for value in row])
            value_obj = renderedAtomicValueSchema.load(
                {
                    "header": {
                        "schema": {"type": "StringValueType"},
                        "value": {
                            "template": renderer_configuration.template_str,
                            "params": renderer_configuration.params.dict(),
                        },
                    },
                    "header_row": header_row,
                    "table": table,
                    "meta_notes": renderer_configuration.meta_notes,
                    "schema": {"type": "TableType"},
                }
            )
            value_type = "TableType"

        return RenderedAtomicContent(
            name="atomic.prescriptive.summary",
            value=value_obj,
            value_type=value_type,
        )

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = (
            False if runtime_configuration.get("include_column_name") is False else True
        )
        _ = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column",
                "partition_object",
                "threshold",
                "row_condition",
                "condition_parser",
            ],
        )

        expected_distribution = None
        if not params.get("partition_object"):
            template_str = "can match any distribution."
        else:
            template_str = (
                "Kullback-Leibler (KL) divergence with respect to the following distribution must be "
                "lower than $threshold."
            )
            expected_distribution = cls._get_kl_divergence_chart(
                params.get("partition_object")
            )

        if include_column_name:
            template_str = f"$column {template_str}"

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = f"{conditional_template_str}, then {template_str}"
            params.update(conditional_params)

        expectation_string_obj = {
            "content_block_type": "string_template",
            "string_template": {"template": template_str, "params": params},
        }

        if expected_distribution:
            return [expectation_string_obj, expected_distribution]
        else:
            return [expectation_string_obj]

    @classmethod
    def _atomic_diagnostic_observed_value_template(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        observed_partition_object = result.result.get("details", {}).get(
            "observed_partition", {}
        )
        weights = observed_partition_object.get("weights", [])

        observed_value = (
            num_to_str(result.result.get("observed_value"))
            if result.result.get("observed_value")
            else result.result.get("observed_value")
        )
        header_template_str = "KL Divergence: $observed_value"
        header_params_with_json_schema = {
            "observed_value": {
                "schema": {"type": "string"},
                "value": str(observed_value)
                if observed_value
                else "None (-infinity, infinity, or NaN)",
            }
        }

        chart = None
        chart_container_col_width = None
        distribution_table_header_row = None
        distribution_table_rows = None

        if len(weights) > 60:  # noqa: PLR2004
            (
                distribution_table_header_row,
                distribution_table_rows,
            ) = cls._atomic_partition_object_table_template(
                partition_object=observed_partition_object
            )
        else:
            chart, chart_container_col_width = cls._atomic_kl_divergence_chart_template(
                partition_object=observed_partition_object
            )

        return (
            header_template_str,
            header_params_with_json_schema,
            chart,
            chart_container_col_width,
            distribution_table_header_row,
            distribution_table_rows,
        )

    @classmethod
    @renderer(renderer_type=AtomicDiagnosticRendererType.OBSERVED_VALUE)
    def _atomic_diagnostic_observed_value(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        if not result.result.get("details"):
            value_obj = renderedAtomicValueSchema.load(
                {
                    "template": "--",
                    "params": {},
                    "schema": {"type": "StringValueType"},
                }
            )
            return RenderedAtomicContent(
                name="atomic.diagnostic.observed_value",
                value=value_obj,
                value_type="StringValueType",
            )

        (
            header_template_str,
            header_params_with_json_schema,
            chart,
            chart_container_col_width,
            distribution_table_header_row,
            distribution_table_rows,
        ) = cls._atomic_diagnostic_observed_value_template(
            configuration,
            result,
            runtime_configuration,
        )

        if chart is not None:
            value_obj = renderedAtomicValueSchema.load(
                {
                    "header": {
                        "schema": {"type": "StringValueType"},
                        "value": {
                            "template": header_template_str,
                            "params": header_params_with_json_schema,
                        },
                    },
                    "graph": chart,
                    "schema": {"type": "GraphType"},
                }
            )
            value_type = "GraphType"
        else:
            value_obj = renderedAtomicValueSchema.load(
                {
                    "header": {
                        "schema": {"type": "StringValueType"},
                        "value": {
                            "template": header_template_str,
                            "params": header_params_with_json_schema,
                        },
                    },
                    "header_row": distribution_table_header_row,
                    "table": distribution_table_rows,
                    "schema": {"type": "TableType"},
                }
            )
            value_type = "TableType"

        return RenderedAtomicContent(
            name="atomic.diagnostic.observed_value",
            value=value_obj,
            value_type=value_type,
        )

    @classmethod
    @renderer(renderer_type=LegacyDiagnosticRendererType.OBSERVED_VALUE)
    def _diagnostic_observed_value_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        if not result.result.get("details"):
            return "--"

        observed_partition_object = result.result["details"]["observed_partition"]
        observed_distribution = cls._get_kl_divergence_chart(observed_partition_object)

        observed_value = (
            num_to_str(result.result.get("observed_value"))
            if result.result.get("observed_value")
            else result.result.get("observed_value")
        )

        observed_value_content_block = RenderedStringTemplateContent(
            **{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "KL Divergence: $observed_value",
                    "params": {
                        "observed_value": str(observed_value)
                        if observed_value
                        else "None (-infinity, infinity, or NaN)",
                    },
                    "styling": {"classes": ["mb-2"]},
                },
            }
        )

        return RenderedContentBlockContainer(
            **{
                "content_block_type": "content_block_container",
                "content_blocks": [observed_value_content_block, observed_distribution],
            }
        )

    @classmethod
    @renderer(renderer_type=LegacyDescriptiveRendererType.HISTOGRAM)
    def _descriptive_histogram_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        assert result, "Must pass in result."
        observed_partition_object = result.result["details"]["observed_partition"]
        weights = observed_partition_object["weights"]
        if len(weights) > 60:  # noqa: PLR2004
            return None

        header = RenderedStringTemplateContent(
            **{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "Histogram",
                    "tooltip": {
                        "content": "expect_column_kl_divergence_to_be_less_than"
                    },
                    "tag": "h6",
                },
            }
        )

        return cls._get_kl_divergence_chart(observed_partition_object, header)
