from abc import ABC, abstractmethod
from typing import List, Optional

from great_expectations.data_context import DataContext
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.parameter_builder.parameter_container import (
    ParameterContainer,
)
from great_expectations.profiler.rule.rule import Rule
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
        self,
        parameter_name: str,
        rule: Rule,
        domain: Domain,
        validator: Validator,
        data_context: Optional[DataContext] = None,
    ):
        self._parameter_name = parameter_name
        self._rule = rule
        self._domain = domain
        self._validator = validator
        self._data_context = data_context

    def build_parameters(
        self,
        *,
        batch_ids: Optional[List[str]] = None,
    ) -> ParameterContainer:
        """Build the parameters for the specified domain_kwargs."""
        return self._build_parameters(batch_ids=batch_ids)

    @abstractmethod
    def _build_parameters(
        self,
        *,
        batch_ids: Optional[List[str]] = None,
    ) -> ParameterContainer:
        pass

    @property
    def parameter_name(self) -> str:
        return self._parameter_name

    @property
    def rule(self) -> Rule:
        return self._rule

    @property
    def domain(self) -> Domain:
        return self._domain

    @property
    def validator(self) -> Validator:
        return self._validator

    @property
    def data_context(self) -> DataContext:
        return self._data_context
