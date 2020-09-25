
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine

from great_expectations.execution_engine.util import (
    build_categorical_partition_object,
    build_continuous_partition_object,
    is_valid_categorical_partition_object,
    is_valid_partition_object,
)

from ..expectation import (
    ColumnMapDatasetExpectation,
    DatasetExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
    _format_map_output,
)

from scipy import stats

from ..registry import extract_metrics


class ExpectColumnKLDivergenceToBeLessThan(DatasetExpectation):
    """Expect the Kulback-Leibler (KL) divergence (relative entropy) of the specified column with respect to the \
           partition object to be lower than the provided threshold.

           KL divergence compares two distributions. The higher the divergence value (relative entropy), the larger the \
           difference between the two distributions. A relative entropy of zero indicates that the data are \
           distributed identically, `when binned according to the provided partition`.

           In many practical contexts, choosing a value between 0.5 and 1 will provide a useful test.

           This expectation works on both categorical and continuous partitions. See notes below for details.

           ``expect_column_kl_divergence_to_be_less_than`` is a \
           :func:`column_aggregate_expectation
           <great_expectations.execution_engine.MetaExecutionEngine.column_aggregate_expectation>`.

           Args:
               column (str): \
                   The column name.
               partition_object (dict): \
                   The expected partition object (see :ref:`partition_object`).
               threshold (float): \
                   The maximum KL divergence to for which to return `success=True`. If KL divergence is larger than the\
                   provided threshold, the test will return `success=False`.

           Keyword Args:
               internal_weight_holdout (float between 0 and 1 or None): \
                   The amount of weight to split uniformly among zero-weighted partition bins. internal_weight_holdout \
                   provides a mechanisms to make the test less strict by assigning positive weights to values observed in \
                   the data for which the partition explicitly expected zero weight. With no internal_weight_holdout, \
                   any value observed in such a region will cause KL divergence to rise to +Infinity.\
                   Defaults to 0.
               tail_weight_holdout (float between 0 and 1 or None): \
                   The amount of weight to add to the tails of the histogram. Tail weight holdout is split evenly between\
                   (-Infinity, min(partition_object['bins'])) and (max(partition_object['bins']), +Infinity). \
                   tail_weight_holdout provides a mechanism to make the test less strict by assigning positive weights to \
                   values observed in the data that are not present in the partition. With no tail_weight_holdout, \
                   any value observed outside the provided partition_object will cause KL divergence to rise to +Infinity.\
                   Defaults to 0.
               bucketize_data (boolean): If True, then continuous data will be bucketized before evaluation. Setting
                   this parameter to false allows evaluation of KL divergence with a None partition object for profiling
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

                   * If the column includes values that are not present in the partition, the tail_weight_holdout will be \
                   equally split among those values, providing a mechanism to weaken the strictness of the expectation \
                   (otherwise, relative entropy would immediately go to infinity).
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

             If relative entropy/kl divergence goes to infinity for any of the reasons mentioned above, the observed value\
             will be set to None. This is because inf, -inf, Nan, are not json serializable and cause some json parsers to\
             crash when encountered. The python None token will be serialized to null in json.

           See also:
               :func:`expect_column_chisquare_test_p_value_to_be_greater_than \
               <great_expectations.execution_engine.execution_engine.ExecutionEngine
               .expect_column_unique_value_count_to_be_between>`

               :func:`expect_column_bootstrapped_ks_test_p_value_to_be_greater_than \
               <great_expectations.execution_engine.execution_engine.ExecutionEngine
               .expect_column_unique_value_count_to_be_between>`

           """
    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = ("column.aggregate.kl_divergence",)
    success_keys = ("partition_object", "threshold", "tail_weight_holdout", "internal_weight_holdout","bucketize_data",)

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "internal_weight_holdout": None,
        "threshold": None,
        "partition_object": None,
        "tail_weight_holdout": None,
        "bucketize_data": None,
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    """ A Column Aggregate Metric Decorator for the KL Divergence"""
    @PandasExecutionEngine.metric(
        metric_name="column.aggregate.kl_divergence",
        metric_domain_keys=DatasetExpectation.domain_keys,
        metric_value_keys=(),
        metric_dependencies=tuple(),
        filter_column_isnull=True,
    )
    def _pandas_kl_divergence(
        self,
        batches: Dict[str, Batch],
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: dict,
        runtime_configuration: dict = None,
    ):
        """KL Divergence Metric Function"""
        series = execution_engine.get_domain_dataframe(
            domain_kwargs=metric_domain_kwargs, batches=batches
        )

        if partition_object is None:
            if bucketize_data:
                partition_object = build_continuous_partition_object(
                    dataset=self, column=series
                )
            else:
                partition_object = build_categorical_partition_object(
                    dataset=self, column=series
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

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        neccessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """
        min_val = None
        max_val = None

        # Setting up a configuration
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration

        # Ensuring basic configuration parameters are properly set
        try:
            assert (
                "column" in configuration.kwargs
            ), "'column' parameter is required for column map expectations"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        # Validating that Minimum and Maximum values are of the proper format and type
        if "min_value" in configuration.kwargs:
            min_val = configuration.kwargs["min_value"]

        if "max_value" in configuration.kwargs:
            max_val = configuration.kwargs["max_value"]

        try:
            # Ensuring Proper interval has been provided
            assert min_val or max_val, "min_value and max_value cannot both be None"
            assert min_val is None or isinstance(
                min_val, (float, int)
            ), "Provided min threshold must be a number"
            assert max_val is None or isinstance(
                max_val, (float, int)
            ), "Provided max threshold must be a number"

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        if min_val is not None and max_val is not None and min_val > max_val:
            raise InvalidExpectationConfigurationError(
                "Minimum Threshold cannot be larger than Maximum Threshold"
            )

        return True

    @Expectation.validates(metric_dependencies=metric_dependencies)
    def _validates(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        """Validates the given data against the set boundaries for mean to ensure it lies within proper range"""
        # Obtaining dependencies used to validate the expectation
        validation_dependencies = self.get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )["metrics"]
        # Extracting metrics
        metric_vals = extract_metrics(
            validation_dependencies, metrics, configuration, runtime_configuration
        )

        # Runtime configuration has preference
        if runtime_configuration:
            result_format = runtime_configuration.get(
                "result_format",
                configuration.kwargs.get(
                    "result_format", self.default_kwarg_values.get("result_format")
                ),
            )
        else:
            result_format = configuration.kwargs.get(
                "result_format", self.default_kwarg_values.get("result_format")
            )
        column_mean = metric_vals.get("column.aggregate.mean")

        # Obtaining components needed for validation
        min_value = self.get_success_kwargs(configuration).get("min_value")
        strict_min = self.get_success_kwargs(configuration).get("strict_min")
        max_value = self.get_success_kwargs(configuration).get("max_value")
        strict_max = self.get_success_kwargs(configuration).get("strict_max")

        # Checking if mean lies between thresholds
        if min_value is not None:
            if strict_min:
                above_min = column_mean > min_value
            else:
                above_min = column_mean >= min_value
        else:
            above_min = True

        if max_value is not None:
            if strict_max:
                below_max = column_mean < max_value
            else:
                below_max = column_mean <= max_value
        else:
            below_max = True

        success = above_min and below_max

        return {"success": success, "result": {"observed_value": column_mean}}





















    def expect_column_kl_divergence_to_be_less_than(
        self,
        column,
        partition_object=None,
        threshold=None,
        tail_weight_holdout=0,
        internal_weight_holdout=0,
        bucketize_data=True,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):

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

    ###
    #
    # Column pairs
    #
    ###
