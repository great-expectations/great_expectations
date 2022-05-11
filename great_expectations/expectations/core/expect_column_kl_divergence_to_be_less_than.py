from typing import Any, Dict, Optional, Tuple

import altair as alt
import numpy as np
import pandas as pd
from scipy import stats as stats

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_engine.util import (
    is_valid_categorical_partition_object,
    is_valid_partition_object,
)
from great_expectations.expectations.expectation import ColumnExpectation
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import (
    RenderedAtomicContent,
    RenderedContentBlockContainer,
    RenderedGraphContent,
    RenderedStringTemplateContent,
    renderedAtomicValueSchema,
)
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validation_graph import ValidationGraph
from great_expectations.validator.validator import Validator


class ExpectColumnKlDivergenceToBeLessThan(ColumnExpectation):
    'Expect the Kulback-Leibler (KL) divergence (relative entropy) of the specified column with respect to the             partition object to be lower than the provided threshold.\n\n            KL divergence compares two distributions. The higher the divergence value (relative entropy), the larger\n            the             difference between the two distributions. A relative entropy of zero indicates that the data are             distributed identically, `when binned according to the provided partition`.\n\n            In many practical contexts, choosing a value between 0.5 and 1 will provide a useful test.\n\n            This expectation works on both categorical and continuous partitions. See notes below for details.\n\n            ``expect_column_kl_divergence_to_be_less_than`` is a             :func:`column_aggregate_expectation <great_expectations.dataset.MetaDataset.column_aggregate_expectation>`.\n\n            Args:\n                column (str):                     The column name.\n                partition_object (dict):                     The expected partition object (see :ref:`partition_object`).\n                threshold (float):                     The maximum KL divergence to for which to return `success=True`. If KL divergence is larger than\n                    the                    provided threshold, the test will return `success=False`.\n\n            Keyword Args:\n                internal_weight_holdout (float between 0 and 1 or None):                     The amount of weight to split uniformly among zero-weighted partition bins.\n                    internal_weight_holdout                     provides a mechanisms to make the test less strict by assigning positive weights to values\n                    observed in                     the data for which the partition explicitly expected zero weight. With no internal_weight_holdout,                     any value observed in such a region will cause KL divergence to rise to +Infinity.                    Defaults to 0.\n                tail_weight_holdout (float between 0 and 1 or None):                     The amount of weight to add to the tails of the histogram. Tail weight holdout is split evenly\n                    between                    (-Infinity, min(partition_object[\'bins\'])) and (max(partition_object[\'bins\']), +Infinity).                     tail_weight_holdout provides a mechanism to make the test less strict by assigning positive\n                    weights to                     values observed in the data that are not present in the partition. With no tail_weight_holdout,                     any value observed outside the provided partition_object will cause KL divergence to rise to\n                    +Infinity.                    Defaults to 0.\n                bucketize_data (boolean): If True, then continuous data will be bucketized before evaluation. Setting\n                    this parameter to false allows evaluation of KL divergence with a None partition object for\n                    profiling\n                    against discrete data.\n\n\n            Other Parameters:\n                result_format (str or None):                     Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                    For more detail, see :ref:`result_format <result_format>`.\n                include_config (boolean):                     If True, then include the expectation config as part of the result object.                     For more detail, see :ref:`include_config`.\n                catch_exceptions (boolean or None):                     If True, then catch exceptions and include them as part of the result object.                     For more detail, see :ref:`catch_exceptions`.\n                meta (dict or None):                     A JSON-serializable dictionary (nesting allowed) that will be included in the output without                     modification. For more detail, see :ref:`meta`.\n\n            Returns:\n                An ExpectationSuiteValidationResult\n\n                Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n                :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n            Notes:\n                These fields in the result object are customized for this expectation:\n                ::\n\n                    {\n                      "observed_value": (float) The true KL divergence (relative entropy) or None if the value is                       calculated as infinity, -infinity, or NaN\n                      "details": {\n                        "observed_partition": (dict) The partition observed in the data\n                        "expected_partition": (dict) The partition against which the data were compared,\n                                                after applying specified weight holdouts.\n                      }\n                    }\n\n                If the partition_object is categorical, this expectation will expect the values in column to also be                 categorical.\n\n                    * If the column includes values that are not present in the partition, the tail_weight_holdout\n                    will be                     equally split among those values, providing a mechanism to weaken the strictness of the\n                    expectation                     (otherwise, relative entropy would immediately go to infinity).\n                    * If the partition includes values that are not present in the column, the test will simply\n                    include                     zero weight for that value.\n\n                If the partition_object is continuous, this expectation will discretize the values in the column\n                according                 to the bins specified in the partition_object, and apply the test to the resulting distribution.\n\n                    * The internal_weight_holdout and tail_weight_holdout parameters provide a mechanism to weaken the                     expectation, since an expected weight of zero would drive relative entropy to be infinite if any\n                    data                     are observed in that interval.\n                    * If internal_weight_holdout is specified, that value will be distributed equally among any\n                    intervals                     with weight zero in the partition_object.\n                    * If tail_weight_holdout is specified, that value will be appended to the tails of the bins                     ((-Infinity, min(bins)) and (max(bins), Infinity).\n\n              If relative entropy/kl divergence goes to infinity for any of the reasons mentioned above, the observed\n              value              will be set to None. This is because inf, -inf, Nan, are not json serializable and cause some json\n              parsers to              crash when encountered. The python None token will be serialized to null in json.\n\n            See also:\n                :func:`expect_column_chisquare_test_p_value_to_be_greater_than                 <great_expectations.dataset.dataset.Dataset.expect_column_unique_value_count_to_be_between>`\n\n                :func:`expect_column_bootstrapped_ks_test_p_value_to_be_greater_than                 <great_expectations.dataset.dataset.Dataset.expect_column_unique_value_count_to_be_between>`\n\n'
    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "column aggregate expectation"],
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
    args_keys = ("column", "partition_object", "threshold")

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that\n        necessary configuration arguments have been provided for the validation of the expectation.\n\n        Args:\n            configuration (OPTIONAL[ExpectationConfiguration]):                 An optional Expectation Configuration entry that will be used to configure the expectation\n        Returns:\n            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully\n        "
        super().validate_configuration(configuration)
        self.validate_metric_value_between_configuration(configuration=configuration)

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        all_dependencies = super().get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )
        dependencies = all_dependencies["metrics"]
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
                    "column.partition",
                    metric_domain_kwargs=domain_kwargs,
                    metric_value_kwargs={"bins": "auto", "allow_relative_error": False},
                )
                validator = Validator(execution_engine=execution_engine)
                graph = ValidationGraph()
                validator.build_metric_dependency_graph(
                    graph=graph,
                    execution_engine=execution_engine,
                    metric_configuration=partition_metric_configuration,
                    configuration=configuration,
                )
                resolved_metrics: Dict[(Tuple[(str, str, str)], Any)] = {}
                validator.resolve_validation_graph(
                    graph=graph, metrics=resolved_metrics
                )
                bins = resolved_metrics[partition_metric_configuration.id]
                hist_metric_configuration = MetricConfiguration(
                    "column.histogram",
                    metric_domain_kwargs=domain_kwargs,
                    metric_value_kwargs={"bins": tuple(bins)},
                )
                nonnull_configuration = MetricConfiguration(
                    "column_values.nonnull.count",
                    metric_domain_kwargs=domain_kwargs,
                    metric_value_kwargs=None,
                )
                dependencies["column.partition"] = partition_metric_configuration
                dependencies["column.histogram"] = hist_metric_configuration
                dependencies["column_values.nonnull.count"] = nonnull_configuration
            else:
                is_categorical = True
                counts_configuration = MetricConfiguration(
                    "column.value_counts",
                    metric_domain_kwargs=domain_kwargs,
                    metric_value_kwargs={"sort": "value"},
                )
                nonnull_configuration = MetricConfiguration(
                    "column_values.nonnull.count", metric_domain_kwargs=domain_kwargs
                )
                dependencies["column.value_counts"] = counts_configuration
                dependencies["column_values.nonnull.count"] = nonnull_configuration
        if (is_categorical is True) or is_valid_categorical_partition_object(
            partition_object
        ):
            dependencies["column.value_counts"] = MetricConfiguration(
                "column.value_counts",
                metric_domain_kwargs=domain_kwargs,
                metric_value_kwargs={"sort": "value"},
            )
            dependencies["column_values.nonnull.count"] = MetricConfiguration(
                "column_values.nonnull.count", domain_kwargs
            )
        else:
            if bins is None:
                if not is_valid_partition_object(partition_object):
                    raise ValueError("Invalid partition_object provided")
                bins = partition_object["bins"]
            hist_metric_configuration = MetricConfiguration(
                "column.histogram",
                metric_domain_kwargs=domain_kwargs,
                metric_value_kwargs={"bins": bins},
            )
            nonnull_configuration = MetricConfiguration(
                "column_values.nonnull.count",
                metric_domain_kwargs=domain_kwargs,
                metric_value_kwargs=None,
            )
            dependencies["column.histogram"] = hist_metric_configuration
            dependencies["column_values.nonnull.count"] = nonnull_configuration
            below_partition = MetricConfiguration(
                "column_values.between.count",
                metric_domain_kwargs=domain_kwargs,
                metric_value_kwargs={"max_value": bins[0], "strict_max": True},
            )
            above_partition = MetricConfiguration(
                "column_values.between.count",
                metric_domain_kwargs=domain_kwargs,
                metric_value_kwargs={"min_value": bins[(-1)], "strict_min": True},
            )
            dependencies["below_partition"] = below_partition
            dependencies["above_partition"] = above_partition
        return all_dependencies

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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
        if (threshold is not None) and (
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
        if (tail_weight_holdout != 0) and ("tail_weights" in partition_object):
            raise ValueError(
                "tail_weight_holdout must be 0 when using tail_weights in partition object"
            )
        if is_valid_categorical_partition_object(partition_object):
            if internal_weight_holdout > 0:
                raise ValueError(
                    "Internal weight holdout cannot be used for discrete data."
                )
            observed_weights = (
                metrics["column.value_counts"] / metrics["column_values.nonnull.count"]
            )
            expected_weights = pd.Series(
                partition_object["weights"],
                index=partition_object["values"],
                name="expected",
            )
            test_df = pd.concat([expected_weights, observed_weights], axis=1)
            na_counts = test_df.isnull().sum()
            pk = test_df["count"].fillna(0)
            if na_counts["expected"] > 0:
                test_df["expected"] *= 1 - tail_weight_holdout
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
            if bucketize_data is False:
                raise ValueError(
                    "KL Divergence cannot be computed with a continuous partition object and the bucketize_data parameter set to false."
                )
            nonnull_count = metrics["column_values.nonnull.count"]
            hist = np.array(metrics["column.histogram"])
            below_partition = metrics["below_partition"]
            above_partition = metrics["above_partition"]
            observed_weights = hist / nonnull_count
            if "tail_weights" in partition_object:
                partition_tail_weight_holdout = np.sum(partition_object["tail_weights"])
            else:
                partition_tail_weight_holdout = 0
            expected_weights = np.array(partition_object["weights"]) * (
                (1 - tail_weight_holdout) - internal_weight_holdout
            )
            if internal_weight_holdout > 0:
                zero_count = len(expected_weights) - np.count_nonzero(expected_weights)
                if zero_count > 0:
                    for (index, value) in enumerate(expected_weights):
                        if value == 0:
                            expected_weights[index] = (
                                internal_weight_holdout / zero_count
                            )
            if (partition_object["bins"][0] == (-np.inf)) and (
                partition_object["bins"][(-1)] == np.inf
            ):
                if tail_weight_holdout > 0:
                    raise ValueError(
                        "tail_weight_holdout cannot be used for partitions with infinite endpoints."
                    )
                if "tail_weights" in partition_object:
                    raise ValueError(
                        "There can be no tail weights for partitions with one or both endpoints at infinity"
                    )
                expected_bins = partition_object["bins"][1:(-1)]
                comb_expected_weights = expected_weights
                expected_tail_weights = np.concatenate(
                    ([expected_weights[0]], [expected_weights[(-1)]])
                )
                expected_weights = expected_weights[1:(-1)]
                comb_observed_weights = observed_weights
                observed_tail_weights = np.concatenate(
                    ([observed_weights[0]], [observed_weights[(-1)]])
                )
                observed_weights = observed_weights[1:(-1)]
            elif partition_object["bins"][0] == (-np.inf):
                if "tail_weights" in partition_object:
                    raise ValueError(
                        "There can be no tail weights for partitions with one or both endpoints at infinity"
                    )
                expected_bins = partition_object["bins"][1:]
                comb_expected_weights = np.concatenate(
                    (expected_weights, [tail_weight_holdout])
                )
                expected_tail_weights = np.concatenate(
                    ([expected_weights[0]], [tail_weight_holdout])
                )
                expected_weights = expected_weights[1:]
                comb_observed_weights = np.concatenate(
                    (observed_weights, [(above_partition / nonnull_count)])
                )
                observed_tail_weights = np.concatenate(
                    ([observed_weights[0]], [(above_partition / nonnull_count)])
                )
                observed_weights = observed_weights[1:]
            elif partition_object["bins"][(-1)] == np.inf:
                if "tail_weights" in partition_object:
                    raise ValueError(
                        "There can be no tail weights for partitions with one or both endpoints at infinity"
                    )
                expected_bins = partition_object["bins"][:(-1)]
                comb_expected_weights = np.concatenate(
                    ([tail_weight_holdout], expected_weights)
                )
                expected_tail_weights = np.concatenate(
                    ([tail_weight_holdout], [expected_weights[(-1)]])
                )
                expected_weights = expected_weights[:(-1)]
                comb_observed_weights = np.concatenate(
                    ([(below_partition / nonnull_count)], observed_weights)
                )
                observed_tail_weights = np.concatenate(
                    ([(below_partition / nonnull_count)], [observed_weights[(-1)]])
                )
                observed_weights = observed_weights[:(-1)]
            else:
                expected_bins = partition_object["bins"]
                if "tail_weights" in partition_object:
                    tail_weights = partition_object["tail_weights"]
                    comb_expected_weights = np.concatenate(
                        ([tail_weights[0]], expected_weights, [tail_weights[1]])
                    )
                    expected_tail_weights = np.array(tail_weights)
                else:
                    comb_expected_weights = np.concatenate(
                        (
                            [(tail_weight_holdout / 2)],
                            expected_weights,
                            [(tail_weight_holdout / 2)],
                        )
                    )
                    expected_tail_weights = np.concatenate(
                        ([(tail_weight_holdout / 2)], [(tail_weight_holdout / 2)])
                    )
                comb_observed_weights = np.concatenate(
                    (
                        [(below_partition / nonnull_count)],
                        observed_weights,
                        [(above_partition / nonnull_count)],
                    )
                )
                observed_tail_weights = (
                    np.concatenate(([below_partition], [above_partition]))
                    / nonnull_count
                )
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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
                bins_x1 = [round(value, 1) for value in bins[:(-1)]]
                bins_x2 = [round(value, 1) for value in bins[1:]]
                df = pd.DataFrame(
                    {"bin_min": bins_x1, "bin_max": bins_x2, "fraction": weights}
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        weights = partition_object.get("weights")
        if weights is None:
            weights = []
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
        chart = ""
        if partition_object.get("bins"):
            bins = partition_object["bins"]
            bins_x1 = [round(value, 1) for value in bins[:(-1)]]
            bins_x2 = [round(value, 1) for value in bins[1:]]
            df = pd.DataFrame(
                {"bin_min": bins_x1, "bin_max": bins_x2, "fraction": weights}
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
                .encode(x="values:N", y="fraction:Q", tooltip=["values", "fraction"])
                .properties(width=chart_pixel_width, height=400, autosize="fit")
            )
            chart = bars.to_json()
        return (chart, chart_container_col_width)

    @classmethod
    def _get_kl_divergence_partition_object_table(cls, partition_object, header=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        table_rows = []
        fractions = partition_object["weights"]
        if partition_object.get("bins"):
            bins = partition_object["bins"]
            for (idx, fraction) in enumerate(fractions):
                if idx == (len(fractions) - 1):
                    table_rows.append(
                        [
                            f"[{num_to_str(bins[idx])} - {num_to_str(bins[(idx + 1)])}]",
                            num_to_str(fraction),
                        ]
                    )
                else:
                    table_rows.append(
                        [
                            f"[{num_to_str(bins[idx])} - {num_to_str(bins[(idx + 1)])})",
                            num_to_str(fraction),
                        ]
                    )
        else:
            values = partition_object["values"]
            table_rows = [
                [value, num_to_str(fractions[idx])]
                for (idx, value) in enumerate(values)
            ]
        if header:
            return {
                "content_block_type": "table",
                "header": header,
                "header_row": (
                    ["Interval", "Fraction"]
                    if partition_object.get("bins")
                    else ["Value", "Fraction"]
                ),
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
                        ]
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
                "header_row": (
                    ["Interval", "Fraction"]
                    if partition_object.get("bins")
                    else ["Value", "Fraction"]
                ),
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
                        ]
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        table_rows = []
        fractions = partition_object["weights"]
        if partition_object.get("bins"):
            bins = partition_object["bins"]
            for (idx, fraction) in enumerate(fractions):
                interval_start = num_to_str(bins[idx])
                interval_end = num_to_str(bins[(idx + 1)])
                interval_closing_symbol = "]" if (idx == (len(fractions) - 1)) else ")"
                table_rows.append(
                    [
                        {
                            "type": "string",
                            "value": f"[{interval_start} - {interval_end}{interval_closing_symbol}",
                        },
                        {"type": "string", "value": num_to_str(fraction)},
                    ]
                )
        else:
            values = partition_object["values"]
            table_rows = [
                [
                    {"schema": {"type": "string"}, "value": str(value)},
                    {"schema": {"type": "string"}, "value": num_to_str(fractions[idx])},
                ]
                for (idx, value) in enumerate(values)
            ]
        header_row = [
            {
                "schema": {"type": "string"},
                "value": ("Interval" if partition_object.get("bins") else "Value"),
            },
            {"schema": {"type": "string"}, "value": "Fraction"},
        ]
        return (header_row, table_rows)

    @classmethod
    def _atomic_prescriptive_template(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if (include_column_name is not None) else True
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
        header_params_with_json_schema = {
            "column": {"schema": {"type": "string"}, "value": params.get("column")},
            "mostly": {"schema": {"type": "number"}, "value": params.get("mostly")},
            "threshold": {
                "schema": {"type": "number"},
                "value": params.get("threshold"),
            },
            "row_condition": {
                "schema": {"type": "string"},
                "value": params.get("row_condition"),
            },
            "condition_parser": {
                "schema": {"type": "string"},
                "value": params.get("condition_parser"),
            },
        }
        expected_partition_object = params.get("partition_object")
        if expected_partition_object is None:
            expected_partition_object = {}
        weights = expected_partition_object.get("weights")
        if weights is None:
            weights = []
        chart = None
        chart_container_col_width = None
        distribution_table_header_row = None
        distribution_table_rows = None
        if not expected_partition_object:
            header_template_str = "can match any distribution."
        else:
            header_template_str = "Kullback-Leibler (KL) divergence with respect to the following distribution must be lower than $threshold."
        if include_column_name:
            header_template_str = f"$column {header_template_str}"
        if len(weights) > 60:
            (
                distribution_table_header_row,
                distribution_table_rows,
            ) = cls._atomic_partition_object_table_template(
                partition_object=expected_partition_object
            )
        else:
            (
                chart,
                chart_container_col_width,
            ) = cls._atomic_kl_divergence_chart_template(
                partition_object=expected_partition_object
            )
        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(
                params["row_condition"], with_schema=True
            )
            header_template_str = (
                f"{conditional_template_str}, then {header_template_str}"
            )
            header_params_with_json_schema.update(conditional_params)
        return (
            header_template_str,
            header_params_with_json_schema,
            chart,
            chart_container_col_width,
            distribution_table_header_row,
            distribution_table_rows,
            styling,
        )

    @classmethod
    @renderer(renderer_type="atomic.prescriptive.summary")
    @render_evaluation_parameter_string
    def _prescriptive_summary(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Rendering function that is utilized by GE Cloud Front-end\n        "
        (
            header_template_str,
            header_params_with_json_schema,
            chart,
            _,
            distribution_table_header_row,
            distribution_table_rows,
            _,
        ) = cls._atomic_prescriptive_template(
            configuration, result, language, runtime_configuration, **kwargs
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
            name="atomic.prescriptive.summary", value=value_obj, value_type=value_type
        )

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if (include_column_name is not None) else True
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
            template_str = "Kullback-Leibler (KL) divergence with respect to the following distribution must be lower than $threshold."
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
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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
                "value": (
                    str(observed_value)
                    if observed_value
                    else "None (-infinity, infinity, or NaN)"
                ),
            }
        }
        chart = None
        chart_container_col_width = None
        distribution_table_header_row = None
        distribution_table_rows = None
        if len(weights) > 60:
            (
                distribution_table_header_row,
                distribution_table_rows,
            ) = cls._atomic_partition_object_table_template(
                partition_object=observed_partition_object
            )
        else:
            (
                chart,
                chart_container_col_width,
            ) = cls._atomic_kl_divergence_chart_template(
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
    @renderer(renderer_type="atomic.diagnostic.observed_value")
    def _atomic_diagnostic_observed_value(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if not result.result.get("details"):
            value_obj = renderedAtomicValueSchema.load(
                {"template": "--", "params": {}, "schema": {"type": "StringValueType"}}
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
            configuration, result, language, runtime_configuration, **kwargs
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
    @renderer(renderer_type="renderer.diagnostic.observed_value")
    def _diagnostic_observed_value_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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
                        "observed_value": (
                            str(observed_value)
                            if observed_value
                            else "None (-infinity, infinity, or NaN)"
                        )
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
        **kwargs,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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
