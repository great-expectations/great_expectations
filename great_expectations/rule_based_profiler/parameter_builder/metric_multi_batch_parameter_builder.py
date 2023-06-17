from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Union

import numpy as np

from great_expectations.core.domain import Domain  # noqa: TCH001
from great_expectations.rule_based_profiler.config import (
    ParameterBuilderConfig,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.metric_computation_result import (
    MetricComputationDetails,  # noqa: TCH001
    MetricComputationResult,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder
from great_expectations.rule_based_profiler.parameter_container import (
    FULLY_QUALIFIED_PARAMETER_NAME_ATTRIBUTED_VALUE_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    ParameterContainer,
)
from great_expectations.types.attributes import Attributes

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )


class MetricMultiBatchParameterBuilder(ParameterBuilder):
    """
    A Single/Multi-Batch implementation for obtaining a resolved (evaluated) metric, using domain_kwargs, value_kwargs,
    and metric_name as arguments.
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        metric_name: str,
        metric_domain_kwargs: Optional[Union[str, dict]] = None,
        metric_value_kwargs: Optional[Union[str, dict]] = None,
        single_batch_mode: Union[str, bool] = False,
        enforce_numeric_metric: Union[str, bool] = False,
        replace_nan_with_zero: Union[str, bool] = False,
        reduce_scalar_metric: Union[str, bool] = True,
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
            metric_name: the name of a metric used in MetricConfiguration (must be a supported and registered metric)
            metric_domain_kwargs: used in MetricConfiguration
            metric_value_kwargs: used in MetricConfiguration
            single_batch_mode: Facilitates "MetricSingleBatchParameterBuilder" subclasses in leveraging this class.
            enforce_numeric_metric: used in MetricConfiguration to insure that metric computations return numeric values
            replace_nan_with_zero: if False (default), then if the computed metric gives NaN, then exception is raised;
            otherwise, if True, then if the computed metric gives NaN, then it is converted to the 0.0 (float) value.
            reduce_scalar_metric: if True (default), then reduces computation of 1-dimensional metric to scalar value.
            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective
            ParameterBuilder objects' outputs available (as fully-qualified parameter names) is pre-requisite.
            These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".
            data_context: AbstractDataContext associated with this ParameterBuilder
        """
        super().__init__(
            name=name,
            evaluation_parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=data_context,
        )

        self._metric_name = metric_name
        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

        self._single_batch_mode = single_batch_mode

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
    def single_batch_mode(self) -> Union[str, bool]:
        return self._single_batch_mode

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
        runtime_configuration: Optional[dict] = None,
    ) -> Attributes:
        """
        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and details.

        Returns:
            Attributes object, containing computed parameter values and parameter computation details metadata.
        """
        # Obtain single_batch_mode from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        single_batch_mode: bool = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.single_batch_mode,
            expected_return_type=bool,
            variables=variables,
            parameters=parameters,
        )

        limit: Optional[int] = 1 if single_batch_mode else None

        metric_computation_result: MetricComputationResult = self.get_metrics(
            metric_name=self.metric_name,
            metric_domain_kwargs=self.metric_domain_kwargs,
            metric_value_kwargs=self.metric_value_kwargs,
            limit=limit,
            enforce_numeric_metric=self.enforce_numeric_metric,
            replace_nan_with_zero=self.replace_nan_with_zero,
            runtime_configuration=runtime_configuration,
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
                ].conditioned_metric_values.ndim
                > 1
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
