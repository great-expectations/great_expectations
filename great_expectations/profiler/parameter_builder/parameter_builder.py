from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from great_expectations.data_context import DataContext
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.parameter_builder.parameter_container import (
    ParameterContainer,
)
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
          - name: my_parameter_builder
            class_name: MetricParameterBuilder
            metric_name: column.mean
            metric_domain_kwargs: $domain.domain_kwargs
        ```
    """

    def __init__(
        self,
        name: str,
        data_context: Optional[DataContext] = None,
    ):
        self._name = name
        self._data_context = data_context

    def build_parameters(
        self,
        domain: Domain,
        validator: Validator,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        batch_ids: Optional[List[str]] = None,
    ) -> ParameterContainer:
        return self._build_parameters(
            domain=domain,
            validator=validator,
            variables=variables,
            parameters=parameters,
            batch_ids=batch_ids,
        )

    @abstractmethod
    def _build_parameters(
        self,
        domain: Domain,
        validator: Validator,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        batch_ids: Optional[List[str]] = None,
    ) -> ParameterContainer:
        pass

    @property
    def name(self) -> str:
        return self._name

    @property
    def data_context(self) -> DataContext:
        return self._data_context
