from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.rule_based_profiler.parameter_builder.parameter_builder import (
    MetricComputationDetails,
    MetricComputationResult,
    ParameterBuilder,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.rule_based_profiler.util import (
    get_parameter_value_and_validate_return_type,
)


class MetricMultiBatchParameterBuilder(ParameterBuilder):
    """
    A Single/Multi-Batch implementation for obtaining a resolved (evaluated) metric, using domain_kwargs, value_kwargs,
    and metric_name as arguments.
    """

    def __init__(
        self,
        name: str,
        metric_name: str,
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        enforce_numeric_metric: Union[str, bool] = False,
        replace_nan_with_zero: Union[str, bool] = False,
        reduce_scalar_metric: Union[str, bool] = True,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
    ):
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
            batch_list: explicitly passed Batch objects for parameter computation (take precedence over batch_request).
            batch_request: specified in ParameterBuilder configuration to get Batch objects for parameter computation.
            data_context: DataContext
        """
        super().__init__(
            name=name,
            data_context=data_context,
            batch_list=batch_list,
            batch_request=batch_request,
        )

        self._metric_name = metric_name
        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

        self._enforce_numeric_metric = enforce_numeric_metric
        self._replace_nan_with_zero = replace_nan_with_zero

        self._reduce_scalar_metric = reduce_scalar_metric

    @property
    def fully_qualified_parameter_name(self) -> str:
        return f"$parameter.{self.name}"

    """
    Full getter/setter accessors for needed properties are for configuring MetricMultiBatchParameterBuilder dynamically.
    """

    @property
    def metric_name(self) -> str:
        return self._metric_name

    @property
    def metric_domain_kwargs(self) -> Optional[Union[str, dict]]:
        return self._metric_domain_kwargs

    @property
    def metric_value_kwargs(self) -> Optional[Union[str, dict]]:
        return self._metric_value_kwargs

    @metric_value_kwargs.setter
    def metric_value_kwargs(self, value: Optional[Union[str, dict]]) -> None:
        self._metric_value_kwargs = value

    @property
    def enforce_numeric_metric(self) -> Union[str, bool]:
        return self._enforce_numeric_metric

    @property
    def replace_nan_with_zero(self) -> Union[str, bool]:
        return self._replace_nan_with_zero

    @property
    def reduce_scalar_metric(self) -> Union[str, bool]:
        return self._reduce_scalar_metric

    def _build_parameters(
        self,
        parameter_container: ParameterContainer,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> Tuple[Any, dict]:
        """
        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and optional
        details.

        return: Tuple containing computed_parameter_value and parameter_computation_details metadata.
        """
        metric_computation_result: MetricComputationResult = self.get_metrics(
            metric_name=self.metric_name,
            metric_domain_kwargs=self.metric_domain_kwargs,
            metric_value_kwargs=self.metric_value_kwargs,
            enforce_numeric_metric=self.enforce_numeric_metric,
            replace_nan_with_zero=self.replace_nan_with_zero,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        metric_values: np.ndarray = metric_computation_result.metric_values
        details: MetricComputationDetails = metric_computation_result.details

        # Obtain reduce_scalar_metric from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        reduce_scalar_metric: bool = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.reduce_scalar_metric,
            expected_return_type=bool,
            variables=variables,
            parameters=parameters,
        )

        # As a simplification, apply reduction to scalar in case of one-dimensional metric (for convenience).
        if reduce_scalar_metric and metric_values.shape[1] == 1:
            metric_values = metric_values[:, 0]

        return (
            metric_values,
            details,
        )
