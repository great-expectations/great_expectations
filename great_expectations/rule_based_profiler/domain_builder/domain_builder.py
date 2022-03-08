import itertools
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.helpers.util import (
    get_batch_ids as get_batch_ids_from_batch_list_or_batch_request,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_validator as get_validator_using_batch_list_or_batch_request,
)
from great_expectations.rule_based_profiler.types import (
    Builder,
    Domain,
    ParameterContainer,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


class DomainBuilder(Builder, ABC):
    """
    A DomainBuilder provides methods to get domains based on one or more batches of data.
    """

    def __init__(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
    ):
        """
        Args:
            batch_list: explicitly specified Batch objects for use in DomainBuilder
            batch_request: specified in DomainBuilder configuration to get Batch objects for domain computation.
            data_context: DataContext
        """
        super().__init__(
            batch_list=batch_list,
            batch_request=batch_request,
            data_context=data_context,
        )

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

    """
    Full getter/setter accessors for "batch_request" and "batch_list" are for configuring DomainBuilder dynamically.
    """

    @property
    def batch_request(self) -> Optional[Union[BatchRequest, RuntimeBatchRequest, dict]]:
        return self._batch_request

    @batch_request.setter
    def batch_request(
        self, value: Union[BatchRequest, RuntimeBatchRequest, dict]
    ) -> None:
        self._batch_request = value

    @property
    def batch_list(self) -> Optional[List[Batch]]:
        return self._batch_list

    @batch_list.setter
    def batch_list(self, value: List[Batch]) -> None:
        self._batch_list = value

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

    @staticmethod
    def get_resolved_metrics_by_key(
        validator: "Validator",  # noqa: F821
        metric_configurations_by_key: Dict[str, List[MetricConfiguration]],
    ) -> Dict[str, Dict[Tuple[str, str, str], Any]]:
        """
        Compute (resolve) metrics for every column name supplied on input.

        Args:
            validator: Validator used to compute column cardinality.
            metric_configurations_by_key: metric configurations used to compute figures of merit.
            Dictionary of the form {
                "my_key": List[MetricConfiguration],  # examples of "my_key" are: "my_column_name", "my_batch_id", etc.
            }

        Returns:
            Dictionary of the form {
                "my_key": Dict[Tuple[str, str, str], Any],
            }
        """
        key: str
        metric_configuration: MetricConfiguration
        metric_configurations_for_key: List[MetricConfiguration]

        # Step 1: Gather "MetricConfiguration" objects corresponding to all possible key values/combinations.
        # and compute all metric values (resolve "MetricConfiguration" objects ) using a single method call.
        resolved_metrics: Dict[Tuple[str, str, str], Any] = validator.compute_metrics(
            metric_configurations=[
                metric_configuration
                for key, metric_configurations_for_key in metric_configurations_by_key.items()
                for metric_configuration in metric_configurations_for_key
            ]
        )

        # Step 2: Gather "MetricConfiguration" ID values for each key (one element per batch_id in every list).
        metric_configuration_ids_by_key: Dict[str, List[Tuple[str, str, str]]] = {
            key: [
                metric_configuration.id
                for metric_configuration in metric_configurations_for_key
            ]
            for key, metric_configurations_for_key in metric_configurations_by_key.items()
        }

        metric_configuration_ids: List[Tuple[str, str, str]]
        # Step 3: Obtain flattened list of "MetricConfiguration" ID values across all key values/combinations.
        metric_configuration_ids_all_keys: List[Tuple[str, str, str]] = list(
            itertools.chain(
                *[
                    metric_configuration_ids
                    for metric_configuration_ids in metric_configuration_ids_by_key.values()
                ]
            )
        )

        # Step 4: Retain only those metric computation results that both, correspond to "MetricConfiguration" objects of
        # interest (reflecting specified key values/combinations).
        metric_configuration_id: Tuple[str, str, str]
        metric_value: Any
        resolved_metrics = {
            metric_configuration_id: metric_value
            for metric_configuration_id, metric_value in resolved_metrics.items()
            if metric_configuration_id in metric_configuration_ids_all_keys
        }

        # Step 5: Gather "MetricConfiguration" ID values for effective collection of resolved metrics.
        metric_configuration_ids_resolved_metrics: List[Tuple[str, str, str]] = list(
            resolved_metrics.keys()
        )

        # Step 6: Produce "key" list, corresponding to effective "MetricConfiguration" ID values.
        candidate_keys: List[str] = [
            key
            for key, metric_configuration_ids in metric_configuration_ids_by_key.items()
            if all(
                [
                    metric_configuration_id in metric_configuration_ids_resolved_metrics
                    for metric_configuration_id in metric_configuration_ids
                ]
            )
        ]

        resolved_metrics_by_key: Dict[str, Dict[Tuple[str, str, str], Any]] = {
            key: {
                metric_configuration.id: resolved_metrics[metric_configuration.id]
                for metric_configuration in metric_configurations_by_key[key]
            }
            for key in candidate_keys
        }

        return resolved_metrics_by_key

    def get_table_row_counts(
        self,
        validator: Optional["Validator"] = None,  # noqa: F821
        batch_ids: Optional[List[str]] = None,
        variables: Optional[ParameterContainer] = None,
    ) -> Dict[str, int]:
        if validator is None:
            validator = self.get_validator(variables=variables)

        if batch_ids is None:
            batch_ids = self.get_batch_ids(variables=variables)

        batch_id: str

        metric_configurations_by_batch_id: Dict[str, List[MetricConfiguration]] = {
            batch_id: [
                MetricConfiguration(
                    metric_name="table.row_count",
                    metric_domain_kwargs={
                        "batch_id": batch_id,
                    },
                    metric_value_kwargs={
                        "include_nested": True,
                    },
                    metric_dependencies=None,
                )
            ]
            for batch_id in batch_ids
        }

        resolved_metrics_by_batch_id: Dict[
            str, Dict[Tuple[str, str, str], Any]
        ] = self.get_resolved_metrics_by_key(
            validator=validator,
            metric_configurations_by_key=metric_configurations_by_batch_id,
        )

        batch_id: str
        resolved_metrics: Dict[Tuple[str, str, str], Any]
        metric_value: Any
        table_row_count_lists_by_batch_id: Dict[str, List[int]] = {
            batch_id: [metric_value for metric_value in resolved_metrics.values()]
            for batch_id, resolved_metrics in resolved_metrics_by_batch_id.items()
        }
        table_row_counts_by_batch_id: Dict[str, int] = {
            batch_id: metric_value[0]
            for batch_id, metric_value in table_row_count_lists_by_batch_id.items()
        }

        return table_row_counts_by_batch_id

    def get_validator(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> Optional["Validator"]:  # noqa: F821
        return get_validator_using_batch_list_or_batch_request(
            purpose="domain_builder",
            data_context=self.data_context,
            batch_list=self.batch_list,
            batch_request=self.batch_request,
            domain=None,
            variables=variables,
            parameters=None,
        )

    def get_batch_ids(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> Optional[List[str]]:
        return get_batch_ids_from_batch_list_or_batch_request(
            data_context=self.data_context,
            batch_list=self.batch_list,
            batch_request=self.batch_request,
            domain=None,
            variables=variables,
            parameters=None,
        )


def build_simple_domains_from_column_names(
    column_names: List[str],
    domain_type: MetricDomainTypes = MetricDomainTypes.COLUMN,
) -> List[Domain]:
    """
    This utility method builds "simple" Domain objects (i.e., required fields only, no "details" metadata accepted).

    :param column_names: list of column names to serve as values for "column" keys in "domain_kwargs" dictionary
    :param domain_type: type of Domain objects (same "domain_type" must be applicable to all Domain objects returned)
    :return: list of resulting Domain objects
    """
    column_name: str
    domains: List[Domain] = [
        Domain(
            domain_type=domain_type,
            domain_kwargs={
                "column": column_name,
            },
        )
        for column_name in column_names
    ]

    return domains
