from typing import Dict, Optional

import altair as alt
import numpy as np
import pandas as pd
from scipy import stats as stats

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_engine.util import (
    build_categorical_partition_object,
    build_continuous_partition_object,
)
from great_expectations.expectations.expectation import TableExpectation
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import (
    RenderedContentBlockContainer,
    RenderedGraphContent,
    RenderedStringTemplateContent,
)
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.validator.validation_graph import MetricConfiguration


class ExpectColumnKlDivergenceToBeLessThan(TableExpectation):
    """Expect the Kulback-Leibler (KL) divergence (relative entropy) of the specified column with respect to the \
            partition object to be lower than the provided threshold.

            KL divergence compares two distributions. The higher the divergence value (relative entropy), the larger
            the \
            difference between the two distributions. A relative entropy of zero indicates that the data are \
            distributed identically, `when binned according to the provided partition`.

            In many practical contexts, choosing a value between 0.5 and 1 will provide a useful test.

            This expectation works on both categorical and continuous partitions. See notes below for details.

            ``expect_column_kl_divergence_to_be_less_than`` is a \
            :func:`column_aggregate_expectation <great_expectations.dataset.MetaDataset.column_aggregate_expectation>`.

            Args:
                column (str): \
                    The column name.
                partition_object (dict): \
                    The expected partition object (see :ref:`partition_object`).
                threshold (float): \
                    The maximum KL divergence to for which to return `success=True`. If KL divergence is larger than
                    the\
                    provided threshold, the test will return `success=False`.

            Keyword Args:
                internal_weight_holdout (float between 0 and 1 or None): \
                    The amount of weight to split uniformly among zero-weighted partition bins.
                    internal_weight_holdout \
                    provides a mechanisms to make the test less strict by assigning positive weights to values
                    observed in \
                    the data for which the partition explicitly expected zero weight. With no internal_weight_holdout, \
                    any value observed in such a region will cause KL divergence to rise to +Infinity.\
                    Defaults to 0.
                tail_weight_holdout (float between 0 and 1 or None): \
                    The amount of weight to add to the tails of the histogram. Tail weight holdout is split evenly
                    between\
                    (-Infinity, min(partition_object['bins'])) and (max(partition_object['bins']), +Infinity). \
                    tail_weight_holdout provides a mechanism to make the test less strict by assigning positive
                    weights to \
                    values observed in the data that are not present in the partition. With no tail_weight_holdout, \
                    any value observed outside the provided partition_object will cause KL divergence to rise to
                    +Infinity.\
                    Defaults to 0.
                bucketize_data (boolean): If True, then continuous data will be bucketized before evaluation. Setting
                    this parameter to false allows evaluation of KL divergence with a None partition object for
                    profiling
                    against discrete data.


            Other Parameters:
                result_format (str or None): \
                    Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                    For more detail, see :ref:`result_format <result_format>`.
                include_config (boolean): \
                    If True, then include the expectation config as part of the result object. \
                    For more detail, see :ref:`include_config`.
                catch_exceptions (boolean or None): \
                    If True, then catch exceptions and include them as part of the result object. \
                    For more detail, see :ref:`catch_exceptions`.
                meta (dict or None): \
                    A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
                    modification. For more detail, see :ref:`meta`.

            Returns:
                An ExpectationSuiteValidationResult

                Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
                :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

            Notes:
                These fields in the result object are customized for this expectation:
                ::

                    {
                      "observed_value": (float) The true KL divergence (relative entropy) or None if the value is \
                      calculated as infinity, -infinity, or NaN
                      "details": {
                        "observed_partition": (dict) The partition observed in the data
                        "expected_partition": (dict) The partition against which the data were compared,
                                                after applying specified weight holdouts.
                      }
                    }

                If the partition_object is categorical, this expectation will expect the values in column to also be \
                categorical.

                    * If the column includes values that are not present in the partition, the tail_weight_holdout
                    will be \
                    equally split among those values, providing a mechanism to weaken the strictness of the
                    expectation \
                    (otherwise, relative entropy would immediately go to infinity).
                    * If the partition includes values that are not present in the column, the test will simply
                    include \
                    zero weight for that value.

                If the partition_object is continuous, this expectation will discretize the values in the column
                according \
                to the bins specified in the partition_object, and apply the test to the resulting distribution.

                    * The internal_weight_holdout and tail_weight_holdout parameters provide a mechanism to weaken the \
                    expectation, since an expected weight of zero would drive relative entropy to be infinite if any
                    data \
                    are observed in that interval.
                    * If internal_weight_holdout is specified, that value will be distributed equally among any
                    intervals \
                    with weight zero in the partition_object.
                    * If tail_weight_holdout is specified, that value will be appended to the tails of the bins \
                    ((-Infinity, min(bins)) and (max(bins), Infinity).

              If relative entropy/kl divergence goes to infinity for any of the reasons mentioned above, the observed
              value\
              will be set to None. This is because inf, -inf, Nan, are not json serializable and cause some json
              parsers to\
              crash when encountered. The python None token will be serialized to null in json.

            See also:
                :func:`expect_column_chisquare_test_p_value_to_be_greater_than \
                <great_expectations.dataset.dataset.Dataset.expect_column_unique_value_count_to_be_between>`

                :func:`expect_column_bootstrapped_ks_test_p_value_to_be_greater_than \
                <great_expectations.dataset.dataset.Dataset.expect_column_unique_value_count_to_be_between>`

            """

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

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies = dict()
        partition_object = configuration.kwargs["partition_object"]
        if partition_object is None:
            if configuration.kwargs["bucketize_data"]:
                partition_metric_configuration = MetricConfiguration(
                    "column.partition",
                    metric_domain_kwargs=configuration.get_domain_kwargs(),
                    metric_value_kwargs={
                        "bins": "auto",
                        "allow_relative_error": configuration.kwargs[
                            "allow_relative_error"
                        ],
                    },
                )
                bins = execution_engine.resolve_metrics(
                    [partition_metric_configuration]
                )[partition_metric_configuration.id]
                hist_metric_configuration = MetricConfiguration(
                    "column.histogram",
                    metric_domain_kwargs=configuration.get_domain_kwargs(),
                    metric_value_kwargs={"bins": tuple(bins),},
                )
                nonnull_configuration = MetricConfiguration(
                    "column_values.null.unexpected_count",
                    metric_domain_kwargs=configuration.get_domain_kwargs(),
                    metric_value_kwargs=dict(),
                )
                dependencies["column.histogram"] = hist_metric_configuration
                dependencies[
                    "column_values.null.unexpected_count"
                ] = nonnull_configuration
            else:
                dependencies[
                    "categorical_partition"
                ] = build_categorical_partition_object(
                    execution_engine=execution_engine,
                    domain_kwargs=configuration.get_domain_kwargs(),
                )

    def _validates(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        bucketize_data = configuration.kwargs["bucketize_data"]
        partition_object = configuration.kwargs["partition_object"]
        threshold = configuration.kwargs["threshold"]
        if partition_object is None:
            if bucketize_data:
                partition_object = build_continuous_partition_object(
                    dataset=self, column=column
                )
            else:
                partition_object = build_categorical_partition_object(
                    dataset=self, column=column
                )

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
            observed_weights = self.get_column_value_counts(
                column
            ) / self.get_column_nonnull_count(column)
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
            hist = np.array(
                self.get_column_hist(column, tuple(partition_object["bins"]))
            )
            # np.histogram(column, partition_object['bins'], density=False)
            bin_edges = partition_object["bins"]
            # Add in the frequencies observed above or below the provided partition
            # below_partition = len(np.where(column < partition_object['bins'][0])[0])
            # above_partition = len(np.where(column > partition_object['bins'][-1])[0])
            below_partition = self.get_column_count_in_range(
                column, max_val=partition_object["bins"][0]
            )
            above_partition = self.get_column_count_in_range(
                column, min_val=partition_object["bins"][-1], strict_min=True
            )

            # Observed Weights is just the histogram values divided by the total number of observations
            observed_weights = np.array(hist) / self.get_column_nonnull_count(column)

            # Adjust expected_weights to account for tail_weight and internal_weight
            if "tail_weights" in partition_object:
                partition_tail_weight_holdout = np.sum(partition_object["tail_weights"])
            else:
                partition_tail_weight_holdout = 0

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
                        [above_partition / self.get_column_nonnull_count(column)],
                    )
                )
                # Set aside left tail weight and above partition weight
                observed_tail_weights = np.concatenate(
                    (
                        [observed_weights[0]],
                        [above_partition / self.get_column_nonnull_count(column)],
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
                        [below_partition / self.get_column_nonnull_count(column)],
                        observed_weights,
                    )
                )
                # Set aside right tail weight and below partition weight
                observed_tail_weights = np.concatenate(
                    (
                        [below_partition / self.get_column_nonnull_count(column)],
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
                        [below_partition / self.get_column_nonnull_count(column)],
                        observed_weights,
                        [above_partition / self.get_column_nonnull_count(column)],
                    )
                )
                # Tail weights are just the counts on either side of the partition
                observed_tail_weights = np.concatenate(
                    ([below_partition], [above_partition])
                ) / self.get_column_nonnull_count(column)

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
    def _get_kl_divergence_chart(cls, partition_object, header=None):
        weights = partition_object["weights"]

        if len(weights) > 60:
            expected_distribution = cls._get_kl_divergence_partition_object_table(
                partition_object, header=header
            )
        else:
            chart_pixel_width = (len(weights) / 60.0) * 500
            if chart_pixel_width < 250:
                chart_pixel_width = 250
            chart_container_col_width = round((len(weights) / 60.0) * 6)
            if chart_container_col_width < 4:
                chart_container_col_width = 4
            elif chart_container_col_width >= 5:
                chart_container_col_width = 6
            elif chart_container_col_width >= 4:
                chart_container_col_width = 5

            mark_bar_args = {}
            if len(weights) == 1:
                mark_bar_args["size"] = 20

            if partition_object.get("bins"):
                bins = partition_object["bins"]
                bins_x1 = [round(value, 1) for value in bins[:-1]]
                bins_x2 = [round(value, 1) for value in bins[1:]]

                df = pd.DataFrame(
                    {"bin_min": bins_x1, "bin_max": bins_x2, "fraction": weights,}
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

                chart = bars.to_json()
            elif partition_object.get("values"):
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
                chart = bars.to_json()

            if header:
                expected_distribution = RenderedGraphContent(
                    **{
                        "content_block_type": "graph",
                        "graph": chart,
                        "header": header,
                        "styling": {
                            "classes": [
                                "col-" + str(chart_container_col_width),
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
                                "col-" + str(chart_container_col_width),
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
    def _get_kl_divergence_partition_object_table(cls, partition_object, header=None):
        table_rows = []
        fractions = partition_object["weights"]

        if partition_object.get("bins"):
            bins = partition_object["bins"]

            for idx, fraction in enumerate(fractions):
                if idx == len(fractions) - 1:
                    table_rows.append(
                        [
                            "[{} - {}]".format(
                                num_to_str(bins[idx]), num_to_str(bins[idx + 1])
                            ),
                            num_to_str(fraction),
                        ]
                    )
                else:
                    table_rows.append(
                        [
                            "[{} - {})".format(
                                num_to_str(bins[idx]), num_to_str(bins[idx + 1])
                            ),
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
    @renderer(renderer_type="renderer.prescriptive")
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
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
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
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
    @renderer(renderer_type="renderer.diagnostic.observed_value")
    def _diagnostic_observed_value_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs
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
    @renderer(renderer_type="renderer.descriptive.histogram")
    def _descriptive_histogram_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs
    ):
        assert result, "Must pass in result."
        observed_partition_object = result.result["details"]["observed_partition"]
        weights = observed_partition_object["weights"]
        if len(weights) > 60:
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
