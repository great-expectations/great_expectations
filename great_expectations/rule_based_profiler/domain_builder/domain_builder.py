import uuid
from abc import ABC, abstractmethod
from typing import List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core.batch import Batch, BatchRequest
from great_expectations.rule_based_profiler.domain_builder.domain import Domain
from great_expectations.rule_based_profiler.parameter_builder.parameter_container import (
    ParameterContainer,
)
from great_expectations.rule_based_profiler.util import build_batch_request
from great_expectations.validator.validator import Validator


class DomainBuilder(ABC):
    """
    A DomainBuilder provides methods to get domains based on one or more batches of data.
    """

    def __init__(
        self,
        data_context: DataContext,
        batch_request: Optional[Union[dict, str]] = None,
    ):
        """
        Args:
            data_context: DataContext
            batch_request: specified in DomainBuilder configuration to get Batch objects for domain computation.
        """

        if data_context is None:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"{self.__class__.__name__} requires a data_context, but none was provided."
            )

        self._data_context = data_context
        self._batch_request = batch_request

    def get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """
        Note: Please do not overwrite the public "get_domains()" method.  If a child class needs to check parameters,
        then please do so in its implementation of the (private) "_get_domains()" method, or in a utility method.
        """
        return self._get_domains(variables=variables)

    @abstractmethod
    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """
        _get_domains is the primary workhorse for the DomainBuilder
        """

        pass

    def get_batch_id(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> Optional[str]:
        if self._batch_request is None:
            return None

        batch_request: Optional[BatchRequest] = build_batch_request(
            domain=None,
            batch_request=self._batch_request,
            variables=variables,
            parameters=None,
        )

        batch_list: List[Batch] = self.data_context.get_batch_list(
            batch_request=batch_request
        )

        num_batches: int = len(batch_list)
        if num_batches != 1:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"{self.__class__.__name__} requires exactly one batch ({num_batches} were retrieved)."
            )

        return batch_list[0].id

    def get_validator(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> Optional[Validator]:
        if self._batch_request is None:
            return None

        batch_request: Optional[BatchRequest] = build_batch_request(
            domain=None,
            batch_request=self._batch_request,
            variables=variables,
            parameters=None,
        )

        expectation_suite_name: str = (
            f"tmp_domain_builder_suite_{str(uuid.uuid4())[:8]}"
        )
        return self.data_context.get_validator(
            batch_request=batch_request,
            create_expectation_suite_with_name=expectation_suite_name,
        )

    @property
    def data_context(self) -> DataContext:
        return self._data_context
