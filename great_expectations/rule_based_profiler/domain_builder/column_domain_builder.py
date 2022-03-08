import itertools
from typing import Any, Dict, List, Optional, Tuple, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.domain_builder.domain_builder import (
    build_simple_domains_from_column_names,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.validator.metric_configuration import MetricConfiguration


class ColumnDomainBuilder(DomainBuilder):
    def __init__(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
        column_names: Optional[Union[str, Optional[List[str]]]] = None,
    ):
        """
        Args:
            batch_list: explicitly specified Batch objects for use in DomainBuilder
            batch_request: specified in DomainBuilder configuration to get Batch objects for domain computation.
            data_context: DataContext
            column_names: Explicitly specified column_names list desired (if None, it is computed based on active Batch)
        """
        super().__init__(
            batch_list=batch_list,
            batch_request=batch_request,
            data_context=data_context,
        )

        self._column_names = column_names

    @property
    def domain_type(self) -> Union[str, MetricDomainTypes]:
        return MetricDomainTypes.COLUMN

    """
    All DomainBuilder classes, whose "domain_type" property equals "MetricDomainTypes.COLUMN", must extend present class
    (ColumnDomainBuilder) in order to provide full getter/setter accessor for "column_names" property (as override).
    """

    @property
    def column_names(self) -> Optional[Union[str, Optional[List[str]]]]:
        return self._column_names

    @column_names.setter
    def column_names(self, value: Optional[Union[str, Optional[List[str]]]]) -> None:
        self._column_names = value

    def get_effective_column_names(
        self,
        include_columns: Optional[Union[str, Optional[List[str]]]] = None,
        exclude_columns: Optional[Union[str, Optional[List[str]]]] = None,
        variables: Optional[ParameterContainer] = None,
    ) -> List[str]:
        # Obtain include_columns from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        include_columns = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=include_columns,
            expected_return_type=None,
            variables=variables,
            parameters=None,
        )

        # Obtain exclude_columns from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        exclude_columns = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=exclude_columns,
            expected_return_type=None,
            variables=variables,
            parameters=None,
        )

        batch_ids: List[str] = self.get_batch_ids(variables=variables)

        table_columns: List[str] = self.get_validator(variables=variables).get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs={
                    "batch_id": batch_ids[-1],  # active_batch_id
                },
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )

        effective_column_names: List[str] = include_columns or table_columns

        if exclude_columns is None:
            exclude_columns = []

        column_name: str
        effective_column_names = [
            column_name
            for column_name in effective_column_names
            if column_name not in exclude_columns
        ]

        if set(effective_column_names) == set(table_columns):
            return effective_column_names

        column_name: str
        for column_name in effective_column_names:
            if column_name not in table_columns:
                raise ge_exceptions.ProfilerExecutionError(
                    message=f'Error: The column "{column_name}" in BatchData does not exist.'
                )

        return effective_column_names

    @staticmethod
    def get_resolved_metrics_by_column_name(
        validator: "Validator",  # noqa: F821
        metric_configurations_by_column_name: Dict[str, List[MetricConfiguration]],
    ) -> Dict[str, Dict[Tuple[str, str, str], Any]]:
        """
        Compute (resolve) metrics for every column name supplied on input.

        Args:
            validator: Validator used to compute column cardinality.
            metric_configurations_by_column_name: metric configurations used to compute figures of merit.
            Dictionary of the form {
                "my_column_name": List[MetricConfiguration],
            }

        Returns:
            Dictionary of the form {
                "my_column_name": Dict[Tuple[str, str, str], Any],
            }
        """
        column_name: str
        metric_configuration: MetricConfiguration
        metric_configurations_for_column_name: List[MetricConfiguration]

        # Step 1: Gather "MetricConfiguration" objects corresponding to all possible column_name/batch_id combinations.
        # and compute all metric values (resolve "MetricConfiguration" objects ) using a single method call.
        resolved_metrics: Dict[Tuple[str, str, str], Any] = validator.compute_metrics(
            metric_configurations=[
                metric_configuration
                for column_name, metric_configurations_for_column_name in metric_configurations_by_column_name.items()
                for metric_configuration in metric_configurations_for_column_name
            ]
        )

        # Step 2: Gather "MetricConfiguration" ID values for each column_name (one element per batch_id in every list).
        metric_configuration_ids_by_column_name: Dict[
            str, List[Tuple[str, str, str]]
        ] = {
            column_name: [
                metric_configuration.id
                for metric_configuration in metric_configurations_for_column_name
            ]
            for column_name, metric_configurations_for_column_name in metric_configurations_by_column_name.items()
        }

        metric_configuration_ids: List[Tuple[str, str, str]]
        # Step 3: Obtain flattened list of "MetricConfiguration" ID values across all column_name/batch_id combinations.
        metric_configuration_ids_all_column_names: List[Tuple[str, str, str]] = list(
            itertools.chain(
                *[
                    metric_configuration_ids
                    for metric_configuration_ids in metric_configuration_ids_by_column_name.values()
                ]
            )
        )

        # Step 4: Retain only those metric computation results that both, correspond to "MetricConfiguration" objects of
        # interest (reflecting specified column_name/batch_id combinations).
        metric_configuration_id: Tuple[str, str, str]
        metric_value: Any
        resolved_metrics = {
            metric_configuration_id: metric_value
            for metric_configuration_id, metric_value in resolved_metrics.items()
            if metric_configuration_id in metric_configuration_ids_all_column_names
        }

        # Step 5: Gather "MetricConfiguration" ID values for effective collection of resolved metrics.
        metric_configuration_ids_resolved_metrics: List[Tuple[str, str, str]] = list(
            resolved_metrics.keys()
        )

        # Step 6: Produce "column_name" list, corresponding to effective "MetricConfiguration" ID values.
        candidate_column_names: List[str] = [
            column_name
            for column_name, metric_configuration_ids in metric_configuration_ids_by_column_name.items()
            if all(
                [
                    metric_configuration_id in metric_configuration_ids_resolved_metrics
                    for metric_configuration_id in metric_configuration_ids
                ]
            )
        ]

        resolved_metrics_by_column_name: Dict[str, Dict[Tuple[str, str, str], Any]] = {
            column_name: {
                metric_configuration.id: resolved_metrics[metric_configuration.id]
                for metric_configuration in metric_configurations_by_column_name[
                    column_name
                ]
            }
            for column_name in candidate_column_names
        }

        return resolved_metrics_by_column_name

    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """
        Obtains and returns domains for all columns of a table (or for configured columns, if they exist in the table).
        """
        return build_simple_domains_from_column_names(
            column_names=self.get_effective_column_names(
                include_columns=self.column_names,
                exclude_columns=None,
                variables=variables,
            ),
            domain_type=self.domain_type,
        )
