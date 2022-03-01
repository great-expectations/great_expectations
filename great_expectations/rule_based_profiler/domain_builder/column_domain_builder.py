from typing import List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.validator.metric_configuration import MetricConfiguration


class ColumnDomainBuilder(DomainBuilder):
    def __init__(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
        column_names: Optional[List[str]] = None,
    ):
        """
        Args:
            batch_list: explicitly specified Batch objects foruse in DomainBuilder
            batch_request: specified in DomainBuilder configuration to get Batch objects for domain computation.
            data_context: DataContext
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
    def column_names(self) -> List[str]:
        return self._column_names

    @column_names.setter
    def column_names(self, value: List[str]) -> None:
        self._column_names = value

    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """
        Obtains and returns domains for all columns of a table (or for configured columns, if they exist in the table).
        """
        batch_id: str = self.get_batch_id(variables=variables)
        table_columns: List[str] = self.get_validator(variables=variables).get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs={
                    "batch_id": batch_id,
                },
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )
        if self.column_names is None:
            self.column_names = table_columns
        else:
            column_name: str
            for column_name in self.column_names:
                if column_name not in table_columns:
                    raise ge_exceptions.ProfilerExecutionError(
                        message=f'Error: The column "{column_name}" in BatchData does not exist.'
                    )

        column_name: str
        domains: List[Domain] = [
            Domain(
                domain_type=self.domain_type,
                domain_kwargs={
                    "column": column_name,
                },
            )
            for column_name in self.column_names
        ]

        return domains
