import copy
import itertools
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, make_dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import numpy as np

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import (
    Batch,
    BatchRequest,
    BatchRequestBase,
    RuntimeBatchRequest,
)
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
    Attributes,
    Builder,
    Domain,
    ParameterContainer,
    build_parameter_container,
    get_fully_qualified_parameter_names,
)
from great_expectations.types import SerializableDictDot
from great_expectations.validator.metric_configuration import MetricConfiguration

# TODO: <Alex>These are placeholder types, until a formal metric computation state class is made available.</Alex>
MetricValue = Union[Any, List[Any], np.ndarray]
MetricValues = Union[MetricValue, np.ndarray]
MetricComputationDetails = Dict[str, Any]
MetricComputationResult = make_dataclass(
    "MetricComputationResult", ["metric_values", "details"]
)


@dataclass
class AttributedResolvedMetrics(SerializableDictDot):
    """
    This class facilitates computing multiple metrics as one operation.

    In order to gather results pertaining to diverse MetricConfiguration directives, computed metrics are augmented
    with uniquely identifiable attribution object so that receivers can filter them from overall resolved metrics.
    """

    metric_values: MetricValues
    metric_attributes: Attributes

    def add_resolved_metric(self, value: Any) -> None:
        if self.metric_values is None:
            self.metric_values = []

        self.metric_values.append(value)

    @property
    def id(self) -> str:
        return self.metric_attributes.to_id()

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=self.to_dict())


class ParameterBuilder(Builder, ABC):
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
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[
            Union[str, BatchRequest, RuntimeBatchRequest, dict]
        ] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
    ):
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
            batch_list: explicitly passed Batch objects for parameter computation (take precedence over batch_request).
            batch_request: specified in ParameterBuilder configuration to get Batch objects for parameter computation.
            data_context: DataContext
        """
        super().__init__(
            batch_list=batch_list,
            batch_request=batch_request,
            data_context=data_context,
        )

        self._name = name

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
        force_batch_data: bool = False,
    ) -> None:
        """
        Args:
            domain: Domain object that is context for execution of this ParameterBuilder object.
            variables: attribute name/value pairs
            parameters: Dictionary of ParameterContainer objects corresponding to all Domain context in memory.
            parameter_computation_impl: Object containing desired ParameterBuilder implementation.
            json_serialize: If True (default), convert computed value to JSON prior to saving results.
            batch_list: Explicit list of Batch objects to supply data at runtime.
            batch_request: Explicit batch_request used to supply data at runtime.
            force_batch_data: Whether or not to overwrite existing batch_request value in ParameterBuilder components.
        """
        self.set_batch_list_or_batch_request(
            batch_list=batch_list,
            batch_request=batch_request,
            force_batch_data=force_batch_data,
        )

        resolve_evaluation_dependencies(
            parameter_builder=self,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        if parameter_computation_impl is None:
            parameter_computation_impl = self._build_parameters

        computed_parameter_value: Any
        parameter_computation_details: dict
        (
            computed_parameter_value,
            parameter_computation_details,
        ) = parameter_computation_impl(
            domain=domain,
            variables=variables,
            parameters=parameters,
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
            self.fully_qualified_parameter_name: {
                "value": convert_to_json_serializable(data=computed_parameter_value)
                if json_serialize
                else computed_parameter_value,
                "details": parameter_computation_details,
            },
        }

        build_parameter_container(
            parameter_container=parameters[domain.id], parameter_values=parameter_values
        )

    @property
    @abstractmethod
    def fully_qualified_parameter_name(self) -> str:
        pass

    @property
    def name(self) -> str:
        return self._name

    @property
    def evaluation_parameter_builders(
        self,
    ) -> Optional[List["ParameterBuilder"]]:  # noqa: F821
        return self._evaluation_parameter_builders

    @property
    def json_serialize(self) -> Union[str, bool]:
        return self._json_serialize

    @abstractmethod
    def _build_parameters(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> Tuple[Any, dict]:
        """
        Builds ParameterContainer object that holds ParameterNode objects with attribute name-value pairs and optional
        details.

        return: Tuple containing computed_parameter_value and parameter_computation_details metadata.
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
        batch_ids: Optional[List[str]] = self.get_batch_ids(
            domain=domain,
            variables=variables,
            parameters=parameters,
        )
        if not batch_ids:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"Utilizing a {self.__class__.__name__} requires a non-empty list of batch identifiers."
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

        # Step-3: Generate "MetricConfiguration" directives for all "metric_domain_kwargs" / "metric_value_kwargs" pairs.

        domain_kwargs_cursor: dict
        kwargs_combinations: List[List[dict]] = [
            [domain_kwargs_cursor, value_kwargs_cursor]
            for value_kwargs_cursor in metric_value_kwargs
            for domain_kwargs_cursor in metric_domain_kwargs
        ]

        kwargs_pair_cursor: List[dict, dict]
        metrics_to_resolve: List[MetricConfiguration] = [
            MetricConfiguration(
                metric_name=metric_name,
                metric_domain_kwargs=kwargs_pair_cursor[0],
                metric_value_kwargs=kwargs_pair_cursor[1],
                metric_dependencies=None,
            )
            for kwargs_pair_cursor in kwargs_combinations
        ]

        # Step-4: Resolve all metrics in one operation simultaneously.

        # The Validator object used for metric calculation purposes.
        validator: "Validator" = self.get_validator(  # noqa: F821
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        resolved_metrics: Dict[Tuple[str, str, str], Any] = validator.compute_metrics(
            metric_configurations=metrics_to_resolve
        )

        # Step-5: Map resolved metrics to their attributes for identification and recovery by receiver.

        metric_configuration: MetricConfiguration
        attributed_resolved_metrics_map: Dict[str, AttributedResolvedMetrics] = {}
        for metric_configuration in metrics_to_resolve:
            attributed_resolved_metrics: AttributedResolvedMetrics = (
                attributed_resolved_metrics_map.get(
                    metric_configuration.metric_value_kwargs_id
                )
            )
            if attributed_resolved_metrics is None:
                attributed_resolved_metrics = AttributedResolvedMetrics(
                    metric_attributes=metric_configuration.metric_value_kwargs,
                    metric_values=[],
                )
                attributed_resolved_metrics_map[
                    metric_configuration.metric_value_kwargs_id
                ] = attributed_resolved_metrics

            resolved_metric_value: Union[
                Tuple[str, str, str], None
            ] = resolved_metrics.get(metric_configuration.id)
            if resolved_metric_value is None:
                raise ge_exceptions.ProfilerExecutionError(
                    f"{metric_configuration.id[0]} was not found in the resolved Metrics for ParameterBuilder."
                )

            attributed_resolved_metrics.add_resolved_metric(value=resolved_metric_value)

        metric_attributes_id: str
        metric_values: AttributedResolvedMetrics

        # Step-6: Leverage Numpy Array capabilities for subsequent operations on results of computed/resolved metrics.

        attributed_resolved_metrics_map = {
            metric_attributes_id: AttributedResolvedMetrics(
                metric_attributes=metric_values.metric_attributes,
                metric_values=np.array(metric_values.metric_values),
            )
            for metric_attributes_id, metric_values in attributed_resolved_metrics_map.items()
        }

        # Step-7: Convert scalar metric values to vectors to enable uniformity of processing in subsequent operations.

        idx: int
        for (
            metric_attributes_id,
            metric_values,
        ) in attributed_resolved_metrics_map.items():
            if metric_values.metric_values.ndim == 1:
                metric_values.metric_values = [
                    [metric_values.metric_values[idx]] for idx in range(len(batch_ids))
                ]
                metric_values.metric_values = np.array(metric_values.metric_values)
                attributed_resolved_metrics_map[metric_attributes_id] = metric_values

        # Step-8: Apply numeric/hygiene directives (e.g., "enforce_numeric_metric", "replace_nan_with_zero") to results.
        for (
            metric_attributes_id,
            metric_values,
        ) in attributed_resolved_metrics_map.items():
            self._sanitize_metric_computation(
                metric_name=metric_name,
                metric_values=metric_values.metric_values,
                enforce_numeric_metric=enforce_numeric_metric,
                replace_nan_with_zero=replace_nan_with_zero,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )

        # Step-9: Compose and return result to receiver (apply simplifications to cases of single "metric_value_kwargs").
        return MetricComputationResult(
            list(attributed_resolved_metrics_map.values()),
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
        metric_values: np.ndarray,
        enforce_numeric_metric: Union[str, bool] = False,
        replace_nan_with_zero: Union[str, bool] = False,
        domain: Optional[Domain] = None,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> np.ndarray:
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

        # Outer-most dimension is data samples (e.g., one per Batch); the rest are dimensions of the actual metric.
        metric_value_shape: tuple = metric_values.shape[1:]

        # Generate all permutations of indexes for accessing every element of the multi-dimensional metric.
        metric_value_shape_idx: int
        axes: List[np.ndarray] = [
            np.indices(dimensions=(metric_value_shape_idx,))[0]
            for metric_value_shape_idx in metric_value_shape
        ]
        metric_value_indices: List[tuple] = list(itertools.product(*tuple(axes)))

        # Generate all permutations of indexes for accessing estimates of every element of the multi-dimensional metric.
        # Prefixing multi-dimensional index with "(slice(None, None, None),)" is equivalent to "[:,]" access.
        metric_value_idx: tuple
        metric_value_vector_indices: List[tuple] = [
            (slice(None, None, None),) + metric_value_idx
            for metric_value_idx in metric_value_indices
        ]

        # Traverse indices of sample vectors corresponding to every element of multi-dimensional metric.
        metric_value_vector: np.ndarray
        for metric_value_idx in metric_value_vector_indices:
            # Obtain "N"-element-long vector of samples for each element of multi-dimensional metric.
            metric_value_vector = metric_values[metric_value_idx]
            if enforce_numeric_metric:
                if not np.issubdtype(metric_value_vector.dtype, np.number):
                    raise ge_exceptions.ProfilerExecutionError(
                        message=f"""Applicability of {self.__class__.__name__} is restricted to numeric-valued metrics \
(value of type "{str(metric_value_vector.dtype)}" was computed).
"""
                    )

                if np.any(np.isnan(metric_value_vector)):
                    if not replace_nan_with_zero:
                        raise ValueError(
                            f"""Computation of metric "{metric_name}" resulted in NaN ("not a number") value.
"""
                        )

                    np.nan_to_num(metric_value_vector, copy=False, nan=0.0)

        return metric_values

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
    data_context: Optional["DataContext"] = None,  # noqa: F821
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
    data_context: Optional["DataContext"] = None,  # noqa: F821
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
    fully_qualified_parameter_names: List[str] = get_fully_qualified_parameter_names(
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
                force_batch_data=False,
            )

            evaluation_parameter_builder.build_parameters(
                domain=domain,
                variables=variables,
                parameters=parameters,
            )

            # Step-4: Any "ParameterBuilder" object, including members of "evaluation_parameter_builders" list may be
            # configured with its own "evaluation_parameter_builders" list.  Recursive call handles such situations.
            resolve_evaluation_dependencies(
                parameter_builder=evaluation_parameter_builder,
                domain=domain,
                variables=variables,
                parameters=parameters,
            )
