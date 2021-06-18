import uuid
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union

from great_expectations.core.batch import Batch, BatchRequest
from great_expectations.data_context import DataContext
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
)
from great_expectations.rule_based_profiler.util import build_batch_request
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
          - parameter_name: my_parameter
            class_name: MetricParameterBuilder
            metric_name: column.mean
            metric_domain_kwargs: $domain.domain_kwargs
        ```
    """

    def __init__(
        self,
        parameter_name: str,
        data_context: Optional[DataContext] = None,
        batch_request: Optional[Union[dict, str]] = None,
    ):
        """
        The ParameterBuilder will build parameters for the active domain from the rule.

        Args:
            parameter_name: the name of this parameter -- this is user-specified parameter name (from configuration);
            it is not the fully-qualified parameter name; a fully-qualified parameter name must start with "$parameter."
            and may contain one or more subsequent parts (e.g., "$parameter.<my_param_from_config>.<metric_name>").
            data_context: DataContext
            batch_request: specified in ParameterBuilder configuration to get Batch objects for parameter computation.
        """

        self._parameter_name = parameter_name
        self._data_context = data_context
        self._batch_request = batch_request

    def build_parameters(
        self,
        parameter_container: ParameterContainer,
        domain: Domain,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ):
        self._build_parameters(
            parameter_container=parameter_container,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

    @abstractmethod
    def _build_parameters(
        self,
        parameter_container: ParameterContainer,
        domain: Domain,
        *,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ):
        pass

    def get_validator(
        self,
        domain: Optional[Domain] = None,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> Optional[Validator]:
        if self._batch_request is None:
            return None

        batch_request: Optional[BatchRequest] = build_batch_request(
            domain=domain,
            batch_request=self._batch_request,
            variables=variables,
            parameters=parameters,
        )

        expectation_suite_name: str = (
            f"tmp_parameter_builder_suite_domain_{domain.id}_{str(uuid.uuid4())[:8]}"
        )
        return self.data_context.get_validator(
            batch_request=batch_request,
            create_expectation_suite_with_name=expectation_suite_name,
        )

    def get_batch_ids(
        self,
        domain: Optional[Domain] = None,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> Optional[List[str]]:
        if self._batch_request is None:
            return None

        batch_request: Optional[BatchRequest] = build_batch_request(
            domain=domain,
            batch_request=self._batch_request,
            variables=variables,
            parameters=parameters,
        )

        batch_list: List[Batch] = self.data_context.get_batch_list(
            batch_request=batch_request
        )

        batch: Batch
        batch_ids: List[str] = [batch.id for batch in batch_list]

        return batch_ids

    @property
    def parameter_name(self) -> str:
        return self._parameter_name

    @property
    def data_context(self) -> DataContext:
        return self._data_context

    @property
    def name(self) -> str:
        return f"{self.parameter_name}_parameter_builder"
