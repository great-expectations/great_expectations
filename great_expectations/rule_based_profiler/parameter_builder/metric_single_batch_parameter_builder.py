from typing import Dict, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.types import (
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    Domain,
    ParameterContainer,
    ParameterNode,
)
from great_expectations.types.attributes import Attributes


class MetricSingleBatchParameterBuilder(MetricMultiBatchParameterBuilder):
    """
    A Single-Batch-only implementation for obtaining a resolved (evaluated) metric, using domain_kwargs, value_kwargs,
    and metric_name as arguments.
    """

    def __init__(
        self,
        name: str,
        metric_name: Optional[str] = None,
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        enforce_numeric_metric: Union[str, bool] = False,
        replace_nan_with_zero: Union[str, bool] = False,
        reduce_scalar_metric: Union[str, bool] = True,
        evaluation_parameter_builder_configs: Optional[
            List[ParameterBuilderConfig]
        ] = None,
        json_serialize: Union[str, bool] = True,
        data_context: Optional["BaseDataContext"] = None,  # noqa: F821
    ) -> None:
        """
        Args:
            name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            metric_name: the name of a metric used in MetricConfiguration (must be a supported and registered metric)
            metric_domain_kwargs: used in MetricConfiguration
            metric_value_kwargs: used in MetricConfiguration
            enforce_numeric_metric: used in MetricConfiguration to insure that metric computations return numeric values
            replace_nan_with_zero: if False (default), then if the computed metric gives NaN, then exception is raised;
            otherwise, if True, then if the computed metric gives NaN, then it is converted to the 0.0 (float) value.
            reduce_scalar_metric: if True (default), then reduces computation of 1-dimensional metric to scalar value.
            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective
            ParameterBuilder objects' outputs available (as fully-qualified parameter names) is pre-requisite.
            These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".
            json_serialize: If True (default), convert computed value to JSON prior to saving results.
            data_context: BaseDataContext associated with this ParameterBuilder
        """
        super().__init__(
            name=name,
            metric_name=metric_name,
            metric_domain_kwargs=metric_domain_kwargs,
            metric_value_kwargs=metric_value_kwargs,
            enforce_numeric_metric=enforce_numeric_metric,
            replace_nan_with_zero=replace_nan_with_zero,
            reduce_scalar_metric=reduce_scalar_metric,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            json_serialize=json_serialize,
            data_context=data_context,
        )

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
        batch_ids: Optional[List[str]] = self.get_batch_ids(
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        num_batch_ids: int = len(batch_ids)
        if num_batch_ids != 1:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""Utilizing a {self.__class__.__name__} requires exactly one Batch of data to be available
({num_batch_ids} Batch identifiers found).
"""
            )

        # Compute metric value for one Batch object (expressed as list of Batch objects).
        super().build_parameters(
            domain=domain,
            variables=variables,
            parameters=parameters,
            parameter_computation_impl=super()._build_parameters,
            json_serialize=False,
            recompute_existing_parameter_values=recompute_existing_parameter_values,
        )

        # Retrieve metric values for one Batch object (expressed as list of Batch objects).
        parameter_node: ParameterNode = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.fully_qualified_parameter_name,
            expected_return_type=None,
            variables=variables,
            parameters=parameters,
        )

        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: None
                if parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY] is None
                else parameter_node[FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY][0],
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: parameter_node[
                    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY
                ],
            }
        )
