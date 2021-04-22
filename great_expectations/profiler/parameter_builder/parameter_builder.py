from abc import ABC, abstractmethod
from typing import List, Optional

from great_expectations.data_context import DataContext
from great_expectations.profiler.parameter_builder.parameter_tree_container_node import ParameterTreeContainerNode
from great_expectations.profiler.profiler_rule.rule_state import RuleState
from great_expectations.validator.validator import Validator


class ParameterBuilder(ABC):
    """
    A ParameterBuilder implementation provides support for building Expectation Configuration Parameters suitable for
    use in other ParameterBuilders or in ConfigurationBuilders as part of profiling.

    A ParameterBuilder is configured as part of a ProfilerRule. Its primary interface is the `build_parameters` method.

    As part of a ProfilerRule, the following configuration will create a new parameter for each domain returned by the
    domain_builder, with an associated id.

        ```
        parameter_builders:
          - parameter_name: mean
            class_name: MetricParameterBuilder
            metric_name: column.mean
            metric_domain_kwargs: $domain.domain_kwargs
        ```
    """

    def __init__(
        self, *, parameter_name: str, data_context: Optional[DataContext] = None
    ):
        self._parameter_name = parameter_name
        self._data_context = data_context

    @property
    def parameter_name(self) -> str:
        return self._parameter_name

    def build_parameters(
        self,
        *,
        rule_state: Optional[RuleState] = None,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
        **kwargs
    ) -> ParameterTreeContainerNode:
        """Build the parameters for the specified domain_kwargs."""
        return self._build_parameters(
            rule_state=rule_state, validator=validator, batch_ids=batch_ids, **kwargs
        )

    @abstractmethod
    def _build_parameters(
        self,
        *,
        rule_state: Optional[RuleState] = None,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
        **kwargs
    ) -> ParameterTreeContainerNode:
        pass
