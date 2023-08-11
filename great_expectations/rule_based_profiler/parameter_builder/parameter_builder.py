from __future__ import annotations

import copy
import datetime
import decimal
import itertools
import logging
import numbers
from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch import Batch, BatchRequestBase  # noqa: TCH001
from great_expectations.core.domain import Domain  # noqa: TCH001
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.rule_based_profiler.attributed_resolved_metrics import (
    AttributedResolvedMetrics,
)
from great_expectations.rule_based_profiler.builder import Builder
from great_expectations.rule_based_profiler.config import (
    ParameterBuilderConfig,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.helpers.util import (
    build_metric_domain_kwargs,
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_batch_ids as get_batch_ids_from_batch_list_or_batch_request,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_validator as get_validator_using_batch_list_or_batch_request,
)
from great_expectations.rule_based_profiler.metric_computation_result import (
    MetricComputationResult,
    MetricValues,
)
from great_expectations.rule_based_profiler.parameter_container import (
    PARAMETER_KEY,
    RAW_PARAMETER_KEY,
    ParameterContainer,
    build_parameter_container,
    get_fully_qualified_parameter_names,
)
from great_expectations.types.attributes import Attributes
from great_expectations.util import is_parseable_date
from great_expectations.validator.computed_metric import MetricValue  # noqa: TCH001
from great_expectations.validator.exception_info import ExceptionInfo  # noqa: TCH001
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validation_graph import (
    ValidationGraph,  # noqa: TCH001
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ParameterBuilder(ABC, Builder):
    """
    A ParameterBuilder implementation provides support for building Expectation Configuration Parameters suitable for
    use in other ParameterBuilders or in ConfigurationBuilders as part of profiling.

    A ParameterBuilder is configured as part of a ProfilerRule. Its primary interface is the `build_parameters` method.

    As part of a ProfilerRule, the following configuration will create a new parameter for each domain returned by the
    domain_builder, with an associated id.

        ```
        parameter_builders:
          - name: my_parameter_builder
            class_name: MetricMultiBatchParameterBuilder
            metric_name: column.mean
        ```
    """

    exclude_field_names: ClassVar[Set[str]] = Builder.exclude_field_names | {
        "evaluation_parameter_builders",
    }

    def __init__(
        self,
        name: str,
        evaluation_parameter_builder_configs: Optional[
            List[ParameterBuilderConfig]
        ] = None,
        data_context: Optional[AbstractDataContext] = None,
    ) -> None:
        """
        The ParameterBuilder will build ParameterNode objects for a Domain from the Rule.

        Args:
            name: the name of this parameter builder -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            evaluation_parameter_builder_configs: ParameterBuilder configurations, executing and making whose respective
            ParameterBuilder objects' outputs available (as fully-qualified parameter names) is pre-requisite.
            These "ParameterBuilder" configurations help build parameters needed for this "ParameterBuilder".
            data_context: AbstractDataContext associated with ParameterBuilder
        """
        super().__init__(data_context=data_context)

        self._name = name

        self._evaluation_parameter_builder_configs = (
            evaluation_parameter_builder_configs
        )

        self._evaluation_parameter_builders = init_rule_parameter_builders(
            parameter_builder_configs=evaluation_parameter_builder_configs,
            data_context=self._data_context,
        )

    def build_parameters(  # noqa: PLR0913
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        parameter_computation_impl: Optional[Callable] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> None:
        """
        Args:
            domain: "Domain" object that is context for execution of this "ParameterBuilder" object.
            variables: attribute name/value pairs
            parameters: Dictionary of "ParameterContainer" objects corresponding to all "Domain" objects in memory.
            parameter_computation_impl: Object containing desired "ParameterBuilder" implementation.
            batch_list: Explicit list of "Batch" objects to supply data at runtime.
            batch_request: Explicit batch_request used to supply data at runtime.
            runtime_configuration: Additional run-time settings (see "Validator.DEFAULT_RUNTIME_CONFIGURATION").
        """
        runtime_configuration = runtime_configuration or {}

        fully_qualified_parameter_names: List[
            str
        ] = get_fully_qualified_parameter_names(
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        # recompute_existing_parameter_values: If "True", recompute value if "fully_qualified_parameter_name" exists.
        recompute_existing_parameter_values: bool = runtime_configuration.get(
            "recompute_existing_parameter_values", False
        )

        if (
            recompute_existing_parameter_values
            or self.raw_fully_qualified_parameter_name
            not in fully_qualified_parameter_names
            or self.json_serialized_fully_qualified_parameter_name
            not in fully_qualified_parameter_names
        ):
            self.set_batch_list_if_null_batch_request(
                batch_list=batch_list,
                batch_request=batch_request,
            )

            self.resolve_evaluation_dependencies(
                domain=domain,
                variables=variables,
                parameters=parameters,
                fully_qualified_parameter_names=fully_qualified_parameter_names,
                runtime_configuration=runtime_configuration,
            )

            if parameter_computation_impl is None:
                parameter_computation_impl = self._build_parameters

            parameter_computation_result: Attributes = parameter_computation_impl(
                domain=domain,
                variables=variables,
                parameters=parameters,
                runtime_configuration=runtime_configuration,
            )

            parameter_values: Dict[str, Any] = {
                self.raw_fully_qualified_parameter_name: parameter_computation_result,
                self.json_serialized_fully_qualified_parameter_name: convert_to_json_serializable(
                    data=parameter_computation_result
                ),
            }

            build_parameter_container(
                parameter_container=parameters[domain.id],
                parameter_values=parameter_values,
            )

    def resolve_evaluation_dependencies(  # noqa: PLR0913
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        fully_qualified_parameter_names: Optional[List[str]] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> None:
        """
        This method computes ("resolves") pre-requisite ("evaluation") dependencies (i.e., results of executing other
        "ParameterBuilder" objects), whose output(s) are needed by specified "ParameterBuilder" object to operate.
        """
        # Step-1: Check if any "evaluation_parameter_builders" are configured for specified "ParameterBuilder" object.
        evaluation_parameter_builders: List[
            ParameterBuilder
        ] = self.evaluation_parameter_builders

        if not evaluation_parameter_builders:
            return

        # Step-2: Obtain all fully-qualified parameter names ("variables" and "parameter" keys) in namespace of "Domain"
        # (fully-qualified parameter names are stored in "ParameterNode" objects of "ParameterContainer" of "Domain"
        # when "ParameterBuilder.build_parameters()" is executed for "ParameterBuilder.fully_qualified_parameter_name");
        # this list contains "raw" (for internal calculations) and "JSON-serialized" fully-qualified parameter names.
        if fully_qualified_parameter_names is None:
            fully_qualified_parameter_names = get_fully_qualified_parameter_names(
                domain=domain,
                variables=variables,
                parameters=parameters,
            )

        # Step-3: Check presence of fully-qualified parameter names of "ParameterBuilder" objects, obtained by iterating
        # over evaluation dependencies.  Execute "ParameterBuilder.build_parameters()" if not in "Domain" scoped list.
        evaluation_parameter_builder: ParameterBuilder
        for evaluation_parameter_builder in evaluation_parameter_builders:
            if (
                evaluation_parameter_builder.raw_fully_qualified_parameter_name
                not in fully_qualified_parameter_names
                or evaluation_parameter_builder.json_serialized_fully_qualified_parameter_name
                not in fully_qualified_parameter_names
            ):
                evaluation_parameter_builder.set_batch_list_if_null_batch_request(
                    batch_list=self.batch_list,
                    batch_request=self.batch_request,
                )

                evaluation_parameter_builder.build_parameters(
                    domain=domain,
                    variables=variables,
                    parameters=parameters,
                    runtime_configuration=runtime_configuration,
                )

    @abstractmethod
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
        pass

    @property
    def name(self) -> str:
        return self._name

    @property
    def evaluation_parameter_builders(
        self,
    ) -> Optional[List[ParameterBuilder]]:
        return self._evaluation_parameter_builders

    @property
    def evaluation_parameter_builder_configs(
        self,
    ) -> Optional[List[ParameterBuilderConfig]]:
        return self._evaluation_parameter_builder_configs

    @property
    def raw_fully_qualified_parameter_name(self) -> str:
        """
        This fully-qualified parameter name references "raw" "ParameterNode" output (including "Numpy" "dtype" values).
        """
        return f"{RAW_PARAMETER_KEY}{self.name}"

    @property
    def json_serialized_fully_qualified_parameter_name(self) -> str:
        """
        This fully-qualified parameter name references "JSON-serialized" "ParameterNode" output.
        """
        return f"{PARAMETER_KEY}{self.name}"

    def get_validator(
        self,
        domain: Optional[Domain] = None,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> Optional[Validator]:
        return get_validator_using_batch_list_or_batch_request(
            purpose="parameter_builder",
            data_context=self.data_context,
            batch_list=self.batch_list,
            batch_request=self.batch_request,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

    def get_batch_ids(
        self,
        limit: Optional[int] = None,
        domain: Optional[Domain] = None,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> Optional[List[str]]:
        return get_batch_ids_from_batch_list_or_batch_request(
            data_context=self.data_context,
            batch_list=self.batch_list,
            batch_request=self.batch_request,
            limit=limit,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

    def get_metrics(  # noqa: PLR0913
        self,
        metric_name: str,
        metric_domain_kwargs: Optional[
            Union[Union[str, dict], List[Union[str, dict]]]
        ] = None,
        metric_value_kwargs: Optional[
            Union[Union[str, dict], List[Union[str, dict]]]
        ] = None,
        limit: Optional[int] = None,
        enforce_numeric_metric: Union[str, bool] = False,
        replace_nan_with_zero: Union[str, bool] = False,
        runtime_configuration: Optional[dict] = None,
        domain: Optional[Domain] = None,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> MetricComputationResult:
        """
        General multi-batch metric computation facility.

        Computes specified metric (can be multi-dimensional, numeric, non-numeric, or mixed) and conditions (or
        "sanitizes") result according to two criteria: enforcing metric output to be numeric and handling NaN values.
        :param metric_name: Name of metric of interest, being computed.
        :param metric_domain_kwargs: Metric Domain Kwargs is an essential parameter of the MetricConfiguration object.
        :param metric_value_kwargs: Metric Value Kwargs is an essential parameter of the MetricConfiguration object.
        :param limit: Optional limit on number of "Batch" objects requested (supports single-Batch scenarios).
        :param enforce_numeric_metric: Flag controlling whether or not metric output must be numerically-valued.
        :param replace_nan_with_zero: Directive controlling how NaN metric values, if encountered, should be handled.
        :param runtime_configuration: Additional run-time settings (see "Validator.DEFAULT_RUNTIME_CONFIGURATION").
        :param domain: "Domain" object scoping "$variable"/"$parameter"-style references in configuration and runtime.
        :param variables: Part of the "rule state" available for "$variable"-style references.
        :param parameters: Part of the "rule state" available for "$parameter"-style references.
        :return: "MetricComputationResult" object, containing both: data samples in the format "N x R^m", where "N"
        (most significant dimension) is the number of measurements (e.g., one per "Batch" of data), while "R^m" is the
        multi-dimensional metric, whose values are being estimated, and details (to be used for metadata purposes).
        """
        if not metric_name:
            raise gx_exceptions.ProfilerExecutionError(
                message=f"""Utilizing "{self.__class__.__name__}.get_metrics()" requires valid "metric_name" to be \
specified (empty "metric_name" value detected)."""
            )

        batch_ids: Optional[List[str]] = self.get_batch_ids(
            limit=limit,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        if not batch_ids:
            raise gx_exceptions.ProfilerExecutionError(
                message=f"Utilizing a {self.__class__.__name__} requires a non-empty list of Batch identifiers."
            )

        """
        Compute metrics, corresponding to multiple "MetricConfiguration" directives, together, rather than individually.

        As a strategy, since "metric_domain_kwargs" changes depending on "batch_id", "metric_value_kwargs" serves as
        identifying entity (through "AttributedResolvedMetrics") for accessing resolved metrics (computation results).

        All "MetricConfiguration" directives are generated by combining each metric_value_kwargs" with
        "metric_domain_kwargs" for all "batch_ids" (where every "metric_domain_kwargs" represents separate "batch_id").
        Then, all "MetricConfiguration" objects, collected into list as container, are resolved simultaneously.
        """

        # Step-1: Gather "metric_domain_kwargs" (corresponding to "batch_ids").

        domain_kwargs: dict = build_metric_domain_kwargs(
            batch_id=None,
            metric_domain_kwargs=metric_domain_kwargs,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        batch_id: str

        metric_domain_kwargs = [
            copy.deepcopy(
                build_metric_domain_kwargs(
                    batch_id=batch_id,
                    metric_domain_kwargs=copy.deepcopy(domain_kwargs),
                    domain=domain,
                    variables=variables,
                    parameters=parameters,
                )
            )
            for batch_id in batch_ids
        ]

        # Step-2: Gather "metric_value_kwargs" (caller may require same metric computed for multiple arguments).

        if not isinstance(metric_value_kwargs, list):
            metric_value_kwargs = [metric_value_kwargs]

        value_kwargs_cursor: dict
        metric_value_kwargs = [
            # Obtain value kwargs from "rule state" (i.e., variables and parameters); from instance variable otherwise.
            get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=value_kwargs_cursor,
                expected_return_type=None,
                variables=variables,
                parameters=parameters,
            )
            for value_kwargs_cursor in metric_value_kwargs
        ]

        # Step-3: Generate "MetricConfiguration" directives for all "metric_domain_kwargs"/"metric_value_kwargs" pairs.

        domain_kwargs_cursor: dict
        kwargs_combinations: List[List[dict]] = [
            [domain_kwargs_cursor, value_kwargs_cursor]
            for value_kwargs_cursor in metric_value_kwargs
            for domain_kwargs_cursor in metric_domain_kwargs
        ]

        metrics_to_resolve: List[MetricConfiguration]

        kwargs_pair_cursor: List[dict, dict]
        metrics_to_resolve = [
            MetricConfiguration(
                metric_name=metric_name,
                metric_domain_kwargs=kwargs_pair_cursor[0],
                metric_value_kwargs=kwargs_pair_cursor[1],
            )
            for kwargs_pair_cursor in kwargs_combinations
        ]

        # Step-4: Resolve all metrics in one operation simultaneously.

        # The Validator object used for metric calculation purposes.
        validator: Validator = self.get_validator(
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        graph: ValidationGraph = (
            validator.metrics_calculator.build_metric_dependency_graph(
                metric_configurations=metrics_to_resolve,
                runtime_configuration=runtime_configuration,
            )
        )

        resolved_metrics: Dict[Tuple[str, str, str], MetricValue]
        aborted_metrics_info: Dict[
            Tuple[str, str, str],
            Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]],
        ]
        (
            resolved_metrics,
            aborted_metrics_info,
        ) = validator.metrics_calculator.resolve_validation_graph_and_handle_aborted_metrics_info(
            graph=graph,
            runtime_configuration=runtime_configuration,
            min_graph_edges_pbar_enable=0,
        )

        # Step-5: Map resolved metrics to their attributes for identification and recovery by receiver.

        attributed_resolved_metrics_map: Dict[str, AttributedResolvedMetrics] = {}

        resolved_metric_value: MetricValue
        attributed_resolved_metrics: AttributedResolvedMetrics
        metric_configuration: MetricConfiguration
        for metric_configuration in metrics_to_resolve:
            attributed_resolved_metrics = attributed_resolved_metrics_map.get(
                metric_configuration.metric_value_kwargs_id
            )
            if attributed_resolved_metrics is None:
                attributed_resolved_metrics = AttributedResolvedMetrics(
                    batch_ids=batch_ids,
                    metric_attributes=Attributes(
                        metric_configuration.metric_value_kwargs
                    ),
                    metric_values_by_batch_id=None,
                )
                attributed_resolved_metrics_map[
                    metric_configuration.metric_value_kwargs_id
                ] = attributed_resolved_metrics

            if metric_configuration.id in resolved_metrics:
                resolved_metric_value = resolved_metrics[metric_configuration.id]
                attributed_resolved_metrics.add_resolved_metric(
                    batch_id=metric_configuration.metric_domain_kwargs["batch_id"],
                    value=resolved_metric_value,
                )
            else:
                logger.warning(
                    f"{metric_configuration.id[0]} was not found in the resolved Metrics for ParameterBuilder."
                )
                continue

        # Step-6: Convert scalar metric values to vectors to enable uniformity of processing in subsequent operations.

        metric_attributes_id: str
        for (
            metric_attributes_id,
            attributed_resolved_metrics,
        ) in attributed_resolved_metrics_map.items():
            if (
                isinstance(
                    attributed_resolved_metrics.conditioned_metric_values,
                    np.ndarray,
                )
                and attributed_resolved_metrics.conditioned_metric_values.ndim == 1
            ):
                attributed_resolved_metrics.metric_values_by_batch_id = {
                    batch_id: [resolved_metric_value]
                    for batch_id, resolved_metric_value in attributed_resolved_metrics.attributed_metric_values.items()
                }
                attributed_resolved_metrics_map[
                    metric_attributes_id
                ] = attributed_resolved_metrics

        # Step-7: Apply numeric/hygiene flags (e.g., "enforce_numeric_metric", "replace_nan_with_zero") to results.

        for (
            metric_attributes_id,
            attributed_resolved_metrics,
        ) in attributed_resolved_metrics_map.items():
            self._sanitize_metric_computation(
                parameter_builder=self,
                metric_name=metric_name,
                attributed_resolved_metrics=attributed_resolved_metrics,
                enforce_numeric_metric=enforce_numeric_metric,
                replace_nan_with_zero=replace_nan_with_zero,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )

        # Step-8: Build and return result to receiver (apply simplifications to cases of single "metric_value_kwargs").

        details: dict = {
            "metric_configuration": {
                "metric_name": metric_name,
                "domain_kwargs": domain_kwargs,
                "metric_value_kwargs": metric_value_kwargs[0]
                if len(metric_value_kwargs) == 1
                else metric_value_kwargs,
            },
            "num_batches": len(batch_ids),
        }
        return MetricComputationResult(
            attributed_resolved_metrics=list(attributed_resolved_metrics_map.values()),
            details=details,
        )

    @staticmethod
    def _sanitize_metric_computation(  # noqa: PLR0913
        parameter_builder: ParameterBuilder,
        metric_name: str,
        attributed_resolved_metrics: AttributedResolvedMetrics,
        enforce_numeric_metric: Union[str, bool] = False,
        replace_nan_with_zero: Union[str, bool] = False,
        domain: Optional[Domain] = None,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> None:
        """
        This method conditions (or "sanitizes") data samples in the format "N x R^m", where "N" (most significant
        dimension) is the number of measurements (e.g., one per Batch of data), while "R^m" is the multi-dimensional
        metric, whose values are being estimated.  The "conditioning" operations are:
        1. If "enforce_numeric_metric" flag is set, raise an error if a non-numeric value is found in sample vectors.
        2. Further, if a NaN is encountered in a sample vectors and "replace_nan_with_zero" is True, then replace those
        NaN values with the 0.0 floating point number; if "replace_nan_with_zero" is False, then raise an error.
        """
        # Obtain enforce_numeric_metric from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        enforce_numeric_metric = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=enforce_numeric_metric,
            expected_return_type=bool,
            variables=variables,
            parameters=parameters,
        )

        # Obtain replace_nan_with_zero from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        replace_nan_with_zero = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=replace_nan_with_zero,
            expected_return_type=bool,
            variables=variables,
            parameters=parameters,
        )

        if not (enforce_numeric_metric or replace_nan_with_zero):
            return

        metric_values_by_batch_id: Dict[str, MetricValue] = {}

        # noinspection PyTypeChecker
        conditioned_attributed_metric_values: Dict[str, MetricValues] = dict(
            filter(
                lambda element: element[1] is not None,
                attributed_resolved_metrics.conditioned_attributed_metric_values.items(),
            )
        )
        batch_id: str
        metric_values: MetricValues
        for (
            batch_id,
            metric_values,
        ) in conditioned_attributed_metric_values.items():
            batch_metric_values: MetricValues = []

            metric_value_shape: tuple = metric_values.shape

            # Generate all permutations of indexes for accessing every element of the multi-dimensional metric.
            metric_value_shape_idx: int
            axes: List[np.ndarray] = [
                np.indices(dimensions=(metric_value_shape_idx,))[0]
                for metric_value_shape_idx in metric_value_shape
            ]
            metric_value_indices: List[tuple] = list(itertools.product(*tuple(axes)))

            metric_value_idx: tuple
            for metric_value_idx in metric_value_indices:
                metric_value: MetricValue = metric_values[metric_value_idx]
                if enforce_numeric_metric:
                    if pd.isnull(metric_value):
                        if not replace_nan_with_zero:
                            raise ValueError(
                                f"""Computation of metric "{metric_name}" resulted in NaN ("not a number") value."""
                            )

                        batch_metric_values.append(0.0)
                    elif not (
                        (  # noqa: PLR1701
                            isinstance(metric_value, (str, np.str_))
                            and is_parseable_date(value=metric_value)
                        )
                        or isinstance(metric_value, datetime.datetime)
                        or isinstance(metric_value, numbers.Number)
                        or isinstance(metric_value, decimal.Decimal)
                        or np.issubdtype(metric_value.dtype, np.number)
                    ):
                        raise gx_exceptions.ProfilerExecutionError(
                            message=f"""Applicability of {parameter_builder.__class__.__name__} is restricted to \
numeric-valued and datetime-valued metrics (value {metric_value} of type "{str(type(metric_value))}" was computed).
"""
                        )
                    else:
                        batch_metric_values.append(metric_value)
                else:
                    batch_metric_values.append(metric_value)

            metric_values_by_batch_id[batch_id] = np.asarray(batch_metric_values)

        attributed_resolved_metrics.metric_values_by_batch_id = (
            metric_values_by_batch_id
        )

    @staticmethod
    def _get_best_candidate_above_threshold(
        candidate_ratio_dict: Dict[str, float],
        threshold: float,
    ) -> Tuple[Optional[str], float]:
        """
        Helper method to calculate which candidate strings or patterns are the best match (ie. highest ratio),
        provided they are also above the threshold.
        """
        best_candidate: Optional[str] = None
        best_ratio: float = 0.0

        candidate: str
        ratio: float
        for candidate, ratio in candidate_ratio_dict.items():
            if ratio > best_ratio and ratio >= threshold:
                best_candidate = candidate
                best_ratio = ratio

        return best_candidate, best_ratio

    @staticmethod
    def _get_sorted_candidates_and_ratios(
        candidate_ratio_dict: Dict[str, float],
    ) -> Dict[str, float]:
        """
        Helper method to sort all candidate strings or patterns by success ratio (how well they matched the domain).

        Returns sorted dict of candidate as key and ratio as value
        """
        # noinspection PyTypeChecker
        return dict(
            sorted(
                candidate_ratio_dict.items(),
                key=lambda element: element[1],
                reverse=True,
            )
        )


def init_rule_parameter_builders(
    parameter_builder_configs: Optional[List[dict]] = None,
    data_context: Optional[AbstractDataContext] = None,
) -> Optional[List[ParameterBuilder]]:
    if parameter_builder_configs is None:
        return None

    return [
        init_parameter_builder(
            parameter_builder_config=parameter_builder_config,
            data_context=data_context,
        )
        for parameter_builder_config in parameter_builder_configs
    ]


def init_parameter_builder(
    parameter_builder_config: Union[ParameterBuilderConfig, dict],
    data_context: Optional[AbstractDataContext] = None,
) -> ParameterBuilder:
    if not isinstance(parameter_builder_config, dict):
        parameter_builder_config = parameter_builder_config.to_dict()

    parameter_builder: ParameterBuilder = instantiate_class_from_config(
        config=parameter_builder_config,
        runtime_environment={"data_context": data_context},
        config_defaults={
            "module_name": "great_expectations.rule_based_profiler.parameter_builder"
        },
    )
    return parameter_builder
