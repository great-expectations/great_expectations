from typing import Any, Dict, List, Optional, Tuple, Union

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder
from great_expectations.rule_based_profiler.helpers.util import (
    build_simple_domains_from_column_names,
    get_parameter_value_and_validate_return_type,
    get_resolved_metrics_by_key,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.validator.metric_configuration import MetricConfiguration


class TableColumnTypesColumnDomainBuilder(ColumnDomainBuilder):
    """
    This DomainBuilder uses execution engine column type metadata to identify domains.
    """

    def __init__(
        self,
        column_type: Any,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
        column_names: Optional[Union[str, Optional[List[str]]]] = None,
    ):
        """
        Create column domains using .

        Args:
            column_type: the column type you wish to return domains for
            batch_list: explicitly specified Batch objects for use in DomainBuilder
            batch_request: BatchRequest to be optionally used to define batches to consider for this domain builder
            data_context: DataContext associated with this profiler
            column_names: Explicitly specified column_names list desired (if None, it is computed based on active Batch)
        """
        super().__init__(
            batch_list=batch_list,
            batch_request=batch_request,
            data_context=data_context,
            column_names=column_names,
        )

        self._column_type = column_type

    @property
    def column_type(self) -> str:
        return self._column_type

    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """Return domains matching the column type.

        Args:
            variables: Optional variables to substitute when evaluating.

        Returns:
            List of domains that match the column type.
        """
        # Obtain column_type from "rule state" (i.e., variables and parameters); from instance variable otherwise.
        column_type: Any = get_parameter_value_and_validate_return_type(
            domain=None,
            parameter_reference=self.column_type,
            expected_return_type=None,
            variables=variables,
            parameters=None,
        )

        table_column_names: List[str] = self.get_effective_column_names(
            include_columns=self.column_names,
            exclude_columns=None,
            variables=variables,
        )

        validator: "Validator" = self.get_validator(variables=variables)  # noqa: F821

        batch_ids: List[str] = self.get_batch_ids(variables=variables)

        metric_configurations: Dict[
            str, List[MetricConfiguration]
        ] = self._generate_metric_configurations(
            batch_ids=batch_ids,
        )

        candidate_column_names: List[
            str
        ] = self._get_column_names_satisfying_column_type(
            validator=validator,
            metric_configurations=metric_configurations,
            column_type=column_type,
        )

        return build_simple_domains_from_column_names(
            column_names=candidate_column_names,
            domain_type=self.domain_type,
        )

    @staticmethod
    def _generate_metric_configurations(
        batch_ids: List[str],
    ) -> Dict[str, List[MetricConfiguration]]:
        """
        Generate metric configurations used to get table column types.

        Args:
            batch_ids: List of batch_ids used to create metric configurations.

        Returns:
            Dictionary of the form {
                table: List[MetricConfiguration],
            }
        """
        batch_id: str
        metric_configurations: Dict[str, List[MetricConfiguration]] = {
            "table": [
                MetricConfiguration(
                    metric_name=f"table.column_types",
                    metric_domain_kwargs={
                        "batch_id": batch_id,
                    },
                    metric_value_kwargs=None,
                    metric_dependencies=None,
                )
                for batch_id in batch_ids
            ]
        }

        return metric_configurations

    @staticmethod
    def _get_column_names_satisfying_column_type(
        validator: "Validator",  # noqa: F821
        metric_configurations: Dict[str, List[MetricConfiguration]],
        column_type: Any,
    ) -> List[str]:
        """
        Get table column types and return columns matching the provided type.

        Args:
            validator: Validator used to compute column cardinality.
            metric_configurations: metric configurations used to compute column types.

        Returns:
            List of column names satisfying column type.
        """
        column_name: str
        resolved_metrics: Dict[Tuple[str, str, str], Any]

        resolved_metrics: Dict[
            str, Dict[Tuple[str, str, str], Any]
        ] = get_resolved_metrics_by_key(
            validator=validator,
            metric_configurations_by_key=metric_configurations,
        )

        resolved_metric: Dict
        table: List
        column: Tuple
        table_column_types_by_column_name: Dict[str, List[Any]] = {}
        for resolved_metric in resolved_metrics.values():
            for table in resolved_metric.values():
                for column in table:
                    table_column_types_by_column_name[column["name"]] = column["type"]

        candidate_column_names: List[str] = [
            name
            for name, table_column_type in table_column_types_by_column_name.items()
            if table_column_type == column_type
        ]

        return candidate_column_names
