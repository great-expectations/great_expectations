from typing import Dict, List, Optional, Union

import numpy as np

from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder
from great_expectations.rule_based_profiler.types import (
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    Domain,
    MetricComputationDetails,
    MetricComputationResult,
    ParameterContainer,
)
from great_expectations.types.attributes import Attributes


class MetricMultiBatchParameterBuilder(ParameterBuilder):
    """
    A Single/Multi-Batch implementation for obtaining a resolved (evaluated) metric, using domain_kwargs, value_kwargs,
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
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            json_serialize=json_serialize,
            data_context=data_context,
        )

        self._metric_name = metric_name
        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

        self._enforce_numeric_metric = enforce_numeric_metric
        self._replace_nan_with_zero = replace_nan_with_zero

        self._reduce_scalar_metric = reduce_scalar_metric

    @property
    def metric_name(self) -> str:
        return self._metric_name

    @metric_name.setter
    def metric_name(self, value: str) -> None:
        self._metric_name = value

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
        details: MetricComputationDetails = metric_computation_result.details

        # Obtain reduce_scalar_metric from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        reduce_scalar_metric: bool = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.reduce_scalar_metric,
            expected_return_type=bool,
            variables=variables,
            parameters=parameters,
        )

        if len(metric_computation_result.attributed_resolved_metrics) == 1:
            # As a simplification, apply reduction to scalar in case of one-dimensional metric (for convenience).
            if (
                reduce_scalar_metric
                and isinstance(
                    metric_computation_result.attributed_resolved_metrics[
                        0
                    ].conditioned_metric_values,
                    np.ndarray,
                )
                and metric_computation_result.attributed_resolved_metrics[
                    0
                ].conditioned_metric_values.shape[1]
                == 1
            ):
                return Attributes(
                    {
                        FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: metric_computation_result.attributed_resolved_metrics[
                            0
                        ].conditioned_metric_values[
                            :, 0
                        ],
                        FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY: metric_computation_result.attributed_resolved_metrics[
                            0
                        ].conditioned_attributed_metric_values,
                        FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: details,
                    }
                )

            return Attributes(
                {
                    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: metric_computation_result.attributed_resolved_metrics[
                        0
                    ].conditioned_metric_values,
                    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY: metric_computation_result.attributed_resolved_metrics[
                        0
                    ].conditioned_attributed_metric_values,
                    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: details,
                }
            )

        return Attributes(
            {
                FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY: metric_computation_result.attributed_resolved_metrics,
                FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY: metric_computation_result.attributed_resolved_metrics,
                FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY: details,
            }
        )
