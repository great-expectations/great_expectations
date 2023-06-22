from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Dict, List, Optional, Set

import numpy as np

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.domain import Domain  # noqa: TCH001
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.helpers.util import (
    NP_EPSILON,
    _is_iterable_of_numeric_dtypes,
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricSingleBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    RAW_PARAMETER_KEY,
    ParameterContainer,
    ParameterNode,
)
from great_expectations.types.attributes import Attributes
from great_expectations.validator.computed_metric import MetricValue  # noqa: TCH001

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )


class HistogramSingleBatchParameterBuilder(MetricSingleBatchParameterBuilder):
    """
    Compute histogram using specified metric for one Batch of data.
    """

    exclude_field_names: ClassVar[
        Set[str]
    ] = MetricSingleBatchParameterBuilder.exclude_field_names | {
        "column_partition_metric_single_batch_parameter_builder_config",
        "metric_name",
        "metric_domain_kwargs",
        "metric_value_kwargs",
        "enforce_numeric_metric",
        "replace_nan_with_zero",
        "reduce_scalar_metric",
    }

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        bins: str = "uniform",
        n_bins: int = 10,
        allow_relative_error: bool = False,
        evaluation_parameter_builder_configs: Optional[
            List[ParameterBuilderConfig]
        ] = None,
        data_context: Optional[AbstractDataContext] = None,
    ) -> None:
        """
        Args:
            name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            bins: Partitioning strategy (one of "uniform", "ntile", "quantile", "percentile", or "auto"); please refer
            to "ColumnPartition" (great_expectations/expectations/metrics/column_aggregate_metrics/column_partition.py).
            n_bins: Number of bins for histogram computation (ignored and recomputed if "bins" argument is "auto").
            allow_relative_error: Used for partitioning strategy values that involve quantiles (all except "uniform").
            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective
            ParameterBuilder objects' outputs available (as fully-qualified parameter names) is pre-requisite.
            These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".
            data_context: AbstractDataContext associated with this ParameterBuilder
        """

        self._column_partition_metric_single_batch_parameter_builder_config = (
            ParameterBuilderConfig(
                module_name="great_expectations.rule_based_profiler.parameter_builder",
                class_name="MetricSingleBatchParameterBuilder",
                name="column_partition_metric_single_batch_parameter_builder",
                metric_name="column.partition",
                metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
                metric_value_kwargs={
                    "bins": bins,
                    "n_bins": n_bins,
                    "allow_relative_error": allow_relative_error,
                },
                enforce_numeric_metric=False,
                replace_nan_with_zero=False,
                reduce_scalar_metric=False,
                evaluation_parameter_builder_configs=None,
            )
        )

        if evaluation_parameter_builder_configs is None:
            evaluation_parameter_builder_configs = [
                self._column_partition_metric_single_batch_parameter_builder_config,
            ]

        super().__init__(
            name=name,
            metric_name=None,
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            enforce_numeric_metric=False,
            replace_nan_with_zero=False,
            reduce_scalar_metric=False,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=data_context,
        )

    def _build_parameters(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> Attributes:
        """
        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and details.

        Returns:
            Attributes object, containing computed parameter values and parameter computation details metadata.
        """
        fully_qualified_column_partition_metric_single_batch_parameter_builder_name: str = f"{RAW_PARAMETER_KEY}{self._column_partition_metric_single_batch_parameter_builder_config.name}"
        # Obtain "column.partition" from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        column_partition_parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=fully_qualified_column_partition_metric_single_batch_parameter_builder_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        bins: MetricValue | None = column_partition_parameter_node[
            FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY
        ]

        if bins is None:
            raise gx_exceptions.ProfilerExecutionError(
                message=f"""Partitioning values for {self.__class__.__name__} by \
{self._column_partition_metric_single_batch_parameter_builder_config.name} into bins encountered empty or non-existent \
elements.
"""
            )

        if not _is_iterable_of_numeric_dtypes(bins):
            raise gx_exceptions.ProfilerExecutionError(
                message=f"""Partitioning values for {self.__class__.__name__} by \
{self._column_partition_metric_single_batch_parameter_builder_config.name} did not yield bins of supported data type.
"""
            )

        # Only unique "bins" are necessary (hence, "n_bins" is potentially lowered to fit data distribution).
        bins = sorted(set(bins))

        column_values_nonnull_count_metric_single_batch_parameter_builder = MetricSingleBatchParameterBuilder(
            name="column_values_nonnull_count_metric_single_batch_parameter_builder",
            metric_name="column_values.nonnull.count",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            enforce_numeric_metric=False,
            replace_nan_with_zero=False,
            reduce_scalar_metric=False,
            evaluation_parameter_builder_configs=None,
            data_context=self.data_context,
        )
        column_values_nonnull_count_metric_single_batch_parameter_builder.build_parameters(
            domain=domain,
            variables=variables,
            parameters=parameters,
            batch_list=self.batch_list,
            batch_request=self.batch_request,
            runtime_configuration=runtime_configuration,
        )
        # Obtain "column_values.nonnull.count" from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        column_values_nonnull_count_parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=column_values_nonnull_count_metric_single_batch_parameter_builder.raw_fully_qualified_parameter_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        self.metric_name = "column.histogram"
        self.metric_value_kwargs = {
            "bins": tuple(bins),
        }

        # Compute metric value for one Batch object.
        super().build_parameters(
            domain=domain,
            variables=variables,
            parameters=parameters,
            parameter_computation_impl=super()._build_parameters,
            runtime_configuration=runtime_configuration,
        )

        # Retrieve metric values for one Batch object.
        parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.raw_fully_qualified_parameter_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        weights: np.ndarray = np.asarray(
            parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY]
        ) / (
            column_values_nonnull_count_parameter_node[
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY
            ]
            + NP_EPSILON
        )
        tail_weights: float = (1.0 - sum(weights)) / 2.0

        partition_object: dict = {
            "bins": bins,
            "weights": weights.tolist(),
            "tail_weights": [tail_weights, tail_weights],
        }
        details: dict = parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY]

        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: partition_object,
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: details,
            }
        )
