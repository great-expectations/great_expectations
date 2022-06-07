import copy
import itertools
import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import numpy as np

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch, BatchRequestBase
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.helpers.util import (
    build_metric_domain_kwargs,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_batch_ids as get_batch_ids_from_batch_list_or_batch_request,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_validator as get_validator_using_batch_list_or_batch_request,
)
from great_expectations.rule_based_profiler.types import (
    PARAMETER_KEY,
    AttributedResolvedMetrics,
    Builder,
    Domain,
    MetricComputationResult,
    MetricValue,
    MetricValues,
    ParameterContainer,
    build_parameter_container,
    get_fully_qualified_parameter_names,
)
from great_expectations.types.attributes import Attributes
from great_expectations.validator.metric_configuration import MetricConfiguration

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

    exclude_field_names: Set[str] = Builder.exclude_field_names | {
        "evaluation_parameter_builders",
    }

    def __init__(
        self,
        name: str,
        evaluation_parameter_builder_configs: Optional[
            List[ParameterBuilderConfig]
        ] = None,
        json_serialize: Union[str, bool] = True,
        data_context: Optional["BaseDataContext"] = None,  # noqa: F821
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
            json_serialize: If True (default), convert computed value to JSON prior to saving results.
            data_context: BaseDataContext associated with ParameterBuilder
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

        self._json_serialize = json_serialize

    def build_parameters(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        parameter_computation_impl: Optional[Callable] = None,
        json_serialize: Optional[bool] = None,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        recompute_existing_parameter_values: bool = False,
    ) -> None:
        """
        Args:
            domain: Domain object that is context for execution of this ParameterBuilder object.
            variables: attribute name/value pairs
            parameters: Dictionary of ParameterContainer objects corresponding to all Domain objects in memory.
            parameter_computation_impl: Object containing desired ParameterBuilder implementation.
            json_serialize: If absent, use property value (in standard way, supporting variables look-up).
            batch_list: Explicit list of Batch objects to supply data at runtime.
            batch_request: Explicit batch_request used to supply data at runtime.
            recompute_existing_parameter_values: If "True", recompute value if "fully_qualified_parameter_name" exists.
        """
        fully_qualified_parameter_names: List[
            str
        ] = get_fully_qualified_parameter_names(
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        if (
            recompute_existing_parameter_values
            or self.fully_qualified_parameter_name
            not in fully_qualified_parameter_names
        ):
            self.set_batch_list_or_batch_request(
                batch_list=batch_list,
                batch_request=batch_request,
            )

            resolve_evaluation_dependencies(
                parameter_builder=self,
                domain=domain,
                variables=variables,
                parameters=parameters,
                fully_qualified_parameter_names=fully_qualified_parameter_names,
                recompute_existing_parameter_values=recompute_existing_parameter_values,
            )

            if parameter_computation_impl is None:
                parameter_computation_impl = self._build_parameters

            parameter_computation_result: Attributes = parameter_computation_impl(
                domain=domain,
                variables=variables,
                parameters=parameters,
                recompute_existing_parameter_values=recompute_existing_parameter_values,
            )

            if json_serialize is None:
                # Obtain json_serialize directive from "rule state" (i.e., variables and parameters); from instance variable otherwise.
                json_serialize = get_parameter_value_and_validate_return_type(
                    domain=domain,
                    parameter_reference=self.json_serialize,
                    expected_return_type=bool,
                    variables=variables,
                    parameters=parameters,
                )

            parameter_values: Dict[str, Any] = {
                self.fully_qualified_parameter_name: convert_to_json_serializable(
                    data=parameter_computation_result
                )
                if json_serialize
                else parameter_computation_result,
            }

            build_parameter_container(
                parameter_container=parameters[domain.id],
                parameter_values=parameter_values,
            )

    @property
    def name(self) -> str:
        return self._name

    @property
    def evaluation_parameter_builders(
        self,
    ) -> Optional[List["ParameterBuilder"]]:  # noqa: F821
        return self._evaluation_parameter_builders

    @property
    def evaluation_parameter_builder_configs(
        self,
    ) -> Optional[List[ParameterBuilderConfig]]:
        return self._evaluation_parameter_builder_configs

    @property
    def json_serialize(self) -> Union[str, bool]:
        return self._json_serialize

    @json_serialize.setter
    def json_serialize(self, value: Union[str, bool]) -> None:
        self._json_serialize = value

    @property
    def fully_qualified_parameter_name(self) -> str:
        return f"{PARAMETER_KEY}{self.name}"

    @abstractmethod
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
        pass

    def get_validator(
        self,
        domain: Optional[Domain] = None,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> Optional["Validator"]:  # noqa: F821
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
        domain: Optional[Domain] = None,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> Optional[List[str]]:
        return get_batch_ids_from_batch_list_or_batch_request(
            data_context=self.data_context,
            batch_list=self.batch_list,
            batch_request=self.batch_request,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

    def get_metrics(
        self,
        metric_name: str,
        metric_domain_kwargs: Optional[
            Union[Union[str, dict], List[Union[str, dict]]]
        ] = None,
        metric_value_kwargs: Optional[
            Union[Union[str, dict], List[Union[str, dict]]]
        ] = None,
        enforce_numeric_metric: Union[str, bool] = False,
        replace_nan_with_zero: Union[str, bool] = False,
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
        :param enforce_numeric_metric: Flag controlling whether or not metric output must be numerically-valued.
        :param replace_nan_with_zero: Directive controlling how NaN metric values, if encountered, should be handled.
        :param domain: Domain object scoping "$variable"/"$parameter"-style references in configuration and runtime.
        :param variables: Part of the "rule state" available for "$variable"-style references.
        :param parameters: Part of the "rule state" available for "$parameter"-style references.
        :return: MetricComputationResult object, containing both: data samples in the format "N x R^m", where "N" (most
        significant dimension) is the number of measurements (e.g., one per Batch of data), while "R^m" is the
        multi-dimensional metric, whose values are being estimated, and details (to be used for metadata purposes).
        """
        if not metric_name:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""Utilizing "{self.__class__.__name__}.get_metrics()" requires valid "metric_name" to be \
specified (empty "metric_name" value detected)."""
            )

        batch_ids: Optional[List[str]] = self.get_batch_ids(
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        if not batch_ids:
            raise ge_exceptions.ProfilerExecutionError(
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
                metric_dependencies=None,
            )
            for kwargs_pair_cursor in kwargs_combinations
        ]

        # Step-4: Sort "MetricConfiguration" directives by "metric_value_kwargs_id" and "batch_id" (in that order).
        # This precise sort order enables pairing every metric value with its respective "batch_id" (e.g., for display).

        metrics_to_resolve = sorted(
            metrics_to_resolve,
            key=lambda metric_configuration_element: (
                metric_configuration_element.metric_value_kwargs_id,
                metric_configuration_element.metric_domain_kwargs["batch_id"],
            ),
        )

        # Step-5: Resolve all metrics in one operation simultaneously.

        # The Validator object used for metric calculation purposes.
        validator: "Validator" = self.get_validator(  # noqa: F821
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        resolved_metrics: Dict[Tuple[str, str, str], Any] = validator.compute_metrics(
            metric_configurations=metrics_to_resolve
        )

        # Step-6: Sort resolved metrics according to same sort order as was applied to "MetricConfiguration" directives.

        resolved_metrics_sorted: Dict[Tuple[str, str, str], Any] = {}

        metric_configuration: MetricConfiguration

        resolved_metric_value: Any

        for metric_configuration in metrics_to_resolve:
            if metric_configuration.id not in resolved_metrics:
                logger.warning(
                    f"{metric_configuration.id[0]} was not found in the resolved Metrics for ParameterBuilder."
                )
                continue

            resolved_metrics_sorted[metric_configuration.id] = resolved_metrics[
                metric_configuration.id
            ]

        # Step-7: Map resolved metrics to their attributes for identification and recovery by receiver.

        attributed_resolved_metrics_map: Dict[str, AttributedResolvedMetrics] = {}

        attributed_resolved_metrics: AttributedResolvedMetrics

        for metric_configuration in metrics_to_resolve:
            attributed_resolved_metrics = attributed_resolved_metrics_map.get(
                metric_configuration.metric_value_kwargs_id
            )
            if attributed_resolved_metrics is None:
                attributed_resolved_metrics = AttributedResolvedMetrics(
                    batch_ids=batch_ids,
                    metric_attributes=metric_configuration.metric_value_kwargs,
                    metric_values_by_batch_id=None,
                )
                attributed_resolved_metrics_map[
                    metric_configuration.metric_value_kwargs_id
                ] = attributed_resolved_metrics

            if metric_configuration.id in resolved_metrics_sorted:
                resolved_metric_value = resolved_metrics_sorted[metric_configuration.id]
                attributed_resolved_metrics.add_resolved_metric(
                    batch_id=metric_configuration.metric_domain_kwargs["batch_id"],
                    value=resolved_metric_value,
                )
            else:
                continue

        # Step-8: Convert scalar metric values to vectors to enable uniformity of processing in subsequent operations.

        metric_attributes_id: str
        for (
            metric_attributes_id,
            attributed_resolved_metrics,
        ) in attributed_resolved_metrics_map.items():
            if (
                isinstance(
                    attributed_resolved_metrics.conditioned_metric_values, np.ndarray
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

        # Step-9: Apply numeric/hygiene flags (e.g., "enforce_numeric_metric", "replace_nan_with_zero") to results.

        for (
            metric_attributes_id,
            attributed_resolved_metrics,
        ) in attributed_resolved_metrics_map.items():
            self._sanitize_metric_computation(
                metric_name=metric_name,
                attributed_resolved_metrics=attributed_resolved_metrics,
                enforce_numeric_metric=enforce_numeric_metric,
                replace_nan_with_zero=replace_nan_with_zero,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )

        # Step-10: Build and return result to receiver (apply simplifications to cases of single "metric_value_kwargs").
        return MetricComputationResult(
            attributed_resolved_metrics=list(attributed_resolved_metrics_map.values()),
            details={
                "metric_configuration": {
                    "metric_name": metric_name,
                    "domain_kwargs": domain_kwargs,
                    "metric_value_kwargs": metric_value_kwargs[0]
                    if len(metric_value_kwargs) == 1
                    else metric_value_kwargs,
                    "metric_dependencies": None,
                },
                "num_batches": len(batch_ids),
            },
        )

    def _sanitize_metric_computation(
        self,
        metric_name: str,
        attributed_resolved_metrics: AttributedResolvedMetrics,
        enforce_numeric_metric: Union[str, bool] = False,
        replace_nan_with_zero: Union[str, bool] = False,
        domain: Optional[Domain] = None,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> AttributedResolvedMetrics:
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
            return attributed_resolved_metrics

        metric_values_by_batch_id: Dict[str, MetricValue] = {}

        batch_id: str
        metric_values: MetricValues
        for (
            batch_id,
            metric_values,
        ) in attributed_resolved_metrics.conditioned_attributed_metric_values.items():
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
                    if not np.issubdtype(metric_value.dtype, np.number):
                        raise ge_exceptions.ProfilerExecutionError(
                            message=f"""Applicability of {self.__class__.__name__} is restricted to numeric-valued metrics \
(value of type "{str(metric_value.dtype)}" was computed).
"""
                        )

                    if np.isnan(metric_value):
                        if not replace_nan_with_zero:
                            raise ValueError(
                                f"""Computation of metric "{metric_name}" resulted in NaN ("not a number") value.
"""
                            )

                        batch_metric_values.append(0.0)
                    else:
                        batch_metric_values.append(metric_value)

            metric_values_by_batch_id[batch_id] = batch_metric_values

        attributed_resolved_metrics.metric_values_by_batch_id = (
            metric_values_by_batch_id
        )

        return attributed_resolved_metrics

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
    data_context: Optional["BaseDataContext"] = None,  # noqa: F821
) -> Optional[List["ParameterBuilder"]]:  # noqa: F821
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
    parameter_builder_config: Union["ParameterBuilderConfig", dict],  # noqa: F821
    data_context: Optional["BaseDataContext"] = None,  # noqa: F821
) -> "ParameterBuilder":  # noqa: F821
    if not isinstance(parameter_builder_config, dict):
        parameter_builder_config = parameter_builder_config.to_dict()

    parameter_builder: "ParameterBuilder" = instantiate_class_from_config(  # noqa: F821
        config=parameter_builder_config,
        runtime_environment={"data_context": data_context},
        config_defaults={
            "module_name": "great_expectations.rule_based_profiler.parameter_builder"
        },
    )
    return parameter_builder


def resolve_evaluation_dependencies(
    parameter_builder: "ParameterBuilder",  # noqa: F821
    domain: Domain,
    variables: Optional[ParameterContainer] = None,
    parameters: Optional[Dict[str, ParameterContainer]] = None,
    fully_qualified_parameter_names: Optional[List[str]] = None,
    recompute_existing_parameter_values: bool = False,
) -> None:
    """
    This method computes ("resolves") pre-requisite ("evaluation") dependencies (i.e., results of executing other
    "ParameterBuilder" objects), whose output(s) are needed by specified "ParameterBuilder" object to fulfill its goals.
    """

    # Step-1: Check if any "evaluation_parameter_builders" are configured for specified "ParameterBuilder" object.
    evaluation_parameter_builders: List[
        "ParameterBuilder"  # noqa: F821
    ] = parameter_builder.evaluation_parameter_builders

    if not evaluation_parameter_builders:
        return

    # Step-2: Obtain all fully-qualified parameter names ("variables" and "parameter" keys) in namespace of "Domain"
    # (fully-qualified parameter names are stored in "ParameterNode" objects of "ParameterContainer" of "Domain"
    # whenever "ParameterBuilder.build_parameters()" is executed for "ParameterBuilder.fully_qualified_parameter_name").
    if fully_qualified_parameter_names is None:
        fully_qualified_parameter_names = get_fully_qualified_parameter_names(
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

    # Step-3: Check for presence of fully-qualified parameter names of "ParameterBuilder" objects, obtained by iterating
    # over evaluation dependencies.  "Execute ParameterBuilder.build_parameters()" if absent from "Domain" scoped list.
    evaluation_parameter_builder: "ParameterBuilder"  # noqa: F821
    for evaluation_parameter_builder in evaluation_parameter_builders:
        fully_qualified_evaluation_parameter_builder_name: str = (
            f"{PARAMETER_KEY}{evaluation_parameter_builder.name}"
        )

        if (
            fully_qualified_evaluation_parameter_builder_name
            not in fully_qualified_parameter_names
        ):
            evaluation_parameter_builder.set_batch_list_or_batch_request(
                batch_list=parameter_builder.batch_list,
                batch_request=parameter_builder.batch_request,
            )

            evaluation_parameter_builder.build_parameters(
                domain=domain,
                variables=variables,
                parameters=parameters,
                recompute_existing_parameter_values=recompute_existing_parameter_values,
            )

            # Step-4: Any "ParameterBuilder" object, including members of "evaluation_parameter_builders" list may be
            # configured with its own "evaluation_parameter_builders" list.  Recursive call handles such situations.
            resolve_evaluation_dependencies(
                parameter_builder=evaluation_parameter_builder,
                domain=domain,
                variables=variables,
                parameters=parameters,
                recompute_existing_parameter_values=recompute_existing_parameter_values,
            )
