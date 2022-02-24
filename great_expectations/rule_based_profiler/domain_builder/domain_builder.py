from abc import ABC, abstractmethod
from typing import List, Optional, Set, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.types import (
    Builder,
    Domain,
    ParameterContainer,
)
from great_expectations.rule_based_profiler.util import (
    get_batch_ids as get_batch_ids_from_batch_list_or_batch_request,
)
from great_expectations.rule_based_profiler.util import (
    get_validator as get_validator_using_batch_list_or_batch_request,
)


class DomainBuilder(Builder, ABC):
    """
    A DomainBuilder provides methods to get domains based on one or more batches of data.
    """

    exclude_field_names: Set[str] = {
        "data_context",
        "batch",
    }

    def __init__(
        self,
        batch: Optional[Batch] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
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

        self._batch = batch

    def get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """
        Note: Please do not overwrite the public "get_domains()" method.  If a child class needs to check parameters,
        then please do so in its implementation of the (private) "_get_domains()" method, or in a utility method.
        """
        return self._get_domains(variables=variables)

    @property
    @abstractmethod
    def domain_type(self) -> Union[str, MetricDomainTypes]:
        pass

    @property
    def batch_request(self) -> Optional[Union[BatchRequest, RuntimeBatchRequest, dict]]:
        return self._batch_request

    """
    Full getter/setter accessors for "batch_request" and "batch" are for configuring DomainBuilder dynamically.
    """

    @batch_request.setter
    def batch_request(
        self, value: Union[BatchRequest, RuntimeBatchRequest, dict]
    ) -> None:
        self._batch_request = value

    @property
    def batch(self) -> Optional[Batch]:
        return self._batch

    @batch.setter
    def batch(self, value: Batch) -> None:
        self._batch = value

    @property
    def data_context(self) -> "DataContext":  # noqa: F821
        return self._data_context

    @abstractmethod
    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """
        _get_domains is the primary workhorse for the DomainBuilder
        """

        pass

    def get_validator(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> Optional["Validator"]:  # noqa: F821
        return get_validator_using_batch_list_or_batch_request(
            purpose="domain_builder",
            data_context=self.data_context,
            batch_list=[self.batch],
            batch_request=self.batch_request,
            domain=None,
            variables=variables,
            parameters=None,
        )

    def _get_batch_ids(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> Optional[List[str]]:
        return get_batch_ids_from_batch_list_or_batch_request(
            data_context=self.data_context,
            batch_list=[self.batch],
            batch_request=self.batch_request,
            domain=None,
            variables=variables,
            parameters=None,
        )

    def get_batch_id(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> str:
        batch_ids: Optional[List[str]] = self._get_batch_ids(
            variables=variables,
        )
        num_batch_ids: int = len(batch_ids)
        if num_batch_ids != 1:
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""{self.__class__.__name__}.get_batch_id() must return exactly one batch_id ({num_batch_ids} \
were retrieved).
"""
            )

        return batch_ids[0]


def build_domains_from_column_names(column_names: List[str]) -> List[Domain]:
    """Build column type domains from column names.

    Args:
        column_names: List of columns to convert.

    Returns:
        A list of column type Domain objects built from column names.
    """
    column_name: str
    domains: List[Domain] = [
        Domain(
            domain_type=MetricDomainTypes.COLUMN,
            domain_kwargs={
                "column": column_name,
            },
        )
        for column_name in column_names
    ]

    return domains
