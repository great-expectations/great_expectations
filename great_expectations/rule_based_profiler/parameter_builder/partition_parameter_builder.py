from typing import Dict, List, Optional, Set, Union

import numpy as np

from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricSingleBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.types import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    PARAMETER_KEY,
    Domain,
    ParameterContainer,
    ParameterNode,
)
from great_expectations.types.attributes import Attributes


class PartitionParameterBuilder(MetricSingleBatchParameterBuilder):
    """
    Compute histogram/partition using specified metric (depending on bucketizaiton directive) for one Batch od data.
    """

    exclude_field_names: Set[
        str
    ] = MetricSingleBatchParameterBuilder.exclude_field_names | {
        "column_partition_metric_single_batch_parameter_builder_config",
        "column_value_counts_metric_single_batch_parameter_builder_config",
        "column_values_nonnull_count_metric_single_batch_parameter_builder_config",
    }

    def __init__(
        self,
        name: str,
        bucketize_data: Union[str, bool] = True,
        json_serialize: Union[str, bool] = True,
        data_context: Optional["BaseDataContext"] = None,  # noqa: F821
    ) -> None:
        """
        Args:
            name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            bucketize_data: If True (default), then data is continuous (non-categorical); hence, must bucketize it.
            json_serialize: If True (default), convert computed value to JSON prior to saving results.
            data_context: BaseDataContext associated with this ParameterBuilder
        """

        self._column_partition_metric_single_batch_parameter_builder_config: ParameterBuilderConfig = ParameterBuilderConfig(
            module_name="great_expectations.rule_based_profiler.parameter_builder",
            class_name="MetricSingleBatchParameterBuilder",
            name="column_partition_metric_single_batch_parameter_builder",
            metric_name="column.partition",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs={
                "bins": "auto",
                "allow_relative_error": False,
            },
            enforce_numeric_metric=False,
            replace_nan_with_zero=False,
            reduce_scalar_metric=False,
            evaluation_parameter_builder_configs=None,
            json_serialize=False,
        )
        self._column_value_counts_metric_single_batch_parameter_builder_config: ParameterBuilderConfig = ParameterBuilderConfig(
            module_name="great_expectations.rule_based_profiler.parameter_builder",
            class_name="MetricSingleBatchParameterBuilder",
            name="column_value_counts_metric_single_batch_parameter_builder",
            metric_name="column.value_counts",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs={
                "sort": "value",
            },
            enforce_numeric_metric=False,
            replace_nan_with_zero=False,
            reduce_scalar_metric=False,
            evaluation_parameter_builder_configs=None,
            json_serialize=False,
        )
        self._column_values_nonnull_count_metric_single_batch_parameter_builder_config: ParameterBuilderConfig = ParameterBuilderConfig(
            module_name="great_expectations.rule_based_profiler.parameter_builder",
            class_name="MetricSingleBatchParameterBuilder",
            name="column_values_nonnull_count_metric_single_batch_parameter_builder",
            metric_name="column_values.nonnull.count",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            enforce_numeric_metric=False,
            replace_nan_with_zero=False,
            reduce_scalar_metric=False,
            evaluation_parameter_builder_configs=None,
            json_serialize=False,
        )

        evaluation_parameter_builder_configs: Optional[List[ParameterBuilderConfig]] = [
            self._column_partition_metric_single_batch_parameter_builder_config,
            self._column_value_counts_metric_single_batch_parameter_builder_config,
            self._column_values_nonnull_count_metric_single_batch_parameter_builder_config,
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
            json_serialize=json_serialize,
            data_context=data_context,
        )

        self._bucketize_data = bucketize_data

    @property
    def bucketize_data(self) -> Union[str, bool]:
        return self._bucketize_data

    def _build_parameters(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        recompute_existing_parameter_values: bool = False,
    ) -> Attributes:
        """
        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and details.

        Returns:
            Attributes object, containing computed parameter values and parameter computation details metadata.
        """
        # Obtain bucketize_data directive from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        bucketize_data = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.bucketize_data,
            expected_return_type=bool,
            variables=variables,
            parameters=parameters,
        )

        is_categorical: bool = not bucketize_data

        fully_qualified_column_partition_metric_single_batch_parameter_builder_name: str = f"{PARAMETER_KEY}{self._column_partition_metric_single_batch_parameter_builder_config.name}"
        # Obtain "column.partition" from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        column_partition_parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=fully_qualified_column_partition_metric_single_batch_parameter_builder_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )
        bins: list = column_partition_parameter_node[
            FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY
        ]

        if bins is None:
            is_categorical = True
        else:
            is_categorical = is_categorical or not np.all(np.diff(bins) > 0.0)

        fully_qualified_column_values_nonnull_count_metric_parameter_builder_name: str = f"{PARAMETER_KEY}{self._column_values_nonnull_count_metric_single_batch_parameter_builder_config.name}"
        # Obtain "column_values.nonnull.count" from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        column_values_nonnull_count_parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=fully_qualified_column_values_nonnull_count_metric_parameter_builder_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        partition_object: dict
        details: dict

        weights: list

        if is_categorical:
            fully_qualified_column_value_counts_metric_single_batch_parameter_builder_name: str = f"{PARAMETER_KEY}{self._column_value_counts_metric_single_batch_parameter_builder_config.name}"
            # Obtain "column.value_counts" from "rule state" (i.e., variables and parameters); from instance variable otherwise.
            column_value_counts_parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=fully_qualified_column_value_counts_metric_single_batch_parameter_builder_name,
                expected_return_type=None,
                variables=variables,
                parameters=parameters,
            )

            values: list = list(
                column_value_counts_parameter_node[
                    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY
                ].index
            )
            weights = list(
                np.asarray(
                    column_value_counts_parameter_node[
                        FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY
                    ]
                )
                / column_values_nonnull_count_parameter_node[
                    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY
                ]
            )

            partition_object = {
                "values": values,
                "weights": weights,
            }
            details = column_value_counts_parameter_node[
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY
            ]
        else:
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
                json_serialize=False,
                recompute_existing_parameter_values=recompute_existing_parameter_values,
            )

            # Retrieve metric values for one Batch object.
            parameter_node: ParameterNode = (
                get_parameter_value_and_validate_return_type(
                    domain=domain,
                    parameter_reference=self.fully_qualified_parameter_name,
                    expected_return_type=None,
                    variables=variables,
                    parameters=parameters,
                )
            )

            # in this case, we have requested a partition, histogram using said partition, and nonnull count
            bins = list(bins)
            weights = list(
                np.asarray(parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY])
                / column_values_nonnull_count_parameter_node[
                    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY
                ]
            )
            tail_weights: float = (1.0 - sum(weights)) / 2.0

            partition_object = {
                "bins": bins,
                "weights": weights,
                "tail_weights": [tail_weights, tail_weights],
            }
            details = parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY]

        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: partition_object,
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: details,
            }
        )
