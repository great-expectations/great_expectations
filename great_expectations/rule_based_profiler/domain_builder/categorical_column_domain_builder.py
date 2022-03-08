from typing import Any, Dict, List, Optional, Set, Tuple, Union

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder
from great_expectations.rule_based_profiler.domain_builder.domain_builder import (
    build_simple_domains_from_column_names,
)
from great_expectations.rule_based_profiler.helpers.cardinality_checker import (
    AbsoluteCardinalityLimit,
    CardinalityChecker,
    CardinalityLimitMode,
    RelativeCardinalityLimit,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.validator.metric_configuration import MetricConfiguration


class CategoricalColumnDomainBuilder(ColumnDomainBuilder):
    """
    This DomainBuilder uses column cardinality to identify domains.
    """

    exclude_field_names: Set[str] = ColumnDomainBuilder.exclude_field_names | {
        "cardinality_checker",
    }

    def __init__(
        self,
        batch_list: Optional[List[Batch]] = None,
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        data_context: Optional["DataContext"] = None,  # noqa: F821
        limit_mode: Optional[Union[CardinalityLimitMode, str]] = None,
        max_unique_values: Optional[int] = None,
        max_proportion_unique: Optional[int] = None,
        exclude_columns: Optional[List[str]] = None,
    ):
        """Create column domains where cardinality is within the specified limit.

        Cardinality refers to the number of unique values in a given domain.
        Categorical generally refers to columns with relatively limited
        number of unique values.
        Limit mode can be absolute (number of unique values) or relative
        (proportion of unique values). You can choose one of: limit_mode,
        max_unique_values or max_proportion_unique to specify the cardinality
        limit.
        Note that the limit must be met for each batch separately that is
        supplied in the batch_request or the column domain will not be included.
        Note that the columns used will be from the first batch retrieved
        via the batch_request. If other batches contain additional columns,
        these will not be considered.

        Args:
            batch_list: explicitly specified Batch objects for use in DomainBuilder
            batch_request: BatchRequest to be optionally used to define batches to consider for this domain builder.
            data_context: DataContext associated with this profiler.
            limit_mode: CardinalityLimitMode or string name of the mode
                defining the maximum allowable cardinality to use when
                filtering columns.
            max_unique_values: number of max unique rows for a custom
                cardinality limit to use when filtering columns.
            max_proportion_unique: proportion of unique values for a
                custom cardinality limit to use when filtering columns.
            exclude_columns: If provided, these columns are pre-filtered and
                excluded from consideration, cardinality is not computed.
        """
        super().__init__(
            batch_list=batch_list,
            batch_request=batch_request,
            data_context=data_context,
            column_names=None,
        )

        self._cardinality_checker = CardinalityChecker(
            limit_mode=limit_mode,
            max_unique_values=max_unique_values,
            max_proportion_unique=max_proportion_unique,
        )

        self._exclude_columns = exclude_columns

    @property
    def domain_type(self) -> Union[str, MetricDomainTypes]:
        return MetricDomainTypes.COLUMN

    @property
    def cardinality_checker(self) -> CardinalityChecker:
        return self._cardinality_checker

    @property
    def exclude_columns(self) -> List[str]:
        return self._exclude_columns

    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """Return domains matching the selected limit_mode.

        Args:
            variables: Optional variables to substitute when evaluating.

        Returns:
            List of domains that match the desired cardinality.
        """
        table_column_names: List[str] = self.get_effective_column_names(
            include_columns=None,
            exclude_columns=self.exclude_columns,
            variables=variables,
        )

        batch_ids: List[str] = self.get_batch_ids(variables=variables)

        metrics_for_cardinality_check: Dict[
            str, List[MetricConfiguration]
        ] = self._generate_metric_configurations_to_check_cardinality(
            batch_ids=batch_ids, column_names=table_column_names
        )

        validator: "Validator" = self.get_validator(variables=variables)  # noqa: F821
        candidate_column_names: List[
            str
        ] = self._column_names_meeting_cardinality_limit(
            validator=validator,
            metrics_for_cardinality_check=metrics_for_cardinality_check,
        )

        return build_simple_domains_from_column_names(
            column_names=candidate_column_names,
            domain_type=self.domain_type,
        )

    def _generate_metric_configurations_to_check_cardinality(
        self,
        batch_ids: List[str],
        column_names: List[str],
    ) -> Dict[str, List[MetricConfiguration]]:
        """Generate metric configurations used to compute metrics for checking cardinality.

        Args:
            batch_ids: List of batch_ids used to create metric configurations.
            column_names: List of column_names used to create metric configurations.

        Returns:
            Dictionary of the form {
                "my_column_name": List[MetricConfiguration],
            }
        """

        limit_mode: Union[
            AbsoluteCardinalityLimit, RelativeCardinalityLimit
        ] = self.cardinality_checker.limit_mode

        column_name: str
        batch_id: str
        metric_configurations: Dict[str, List[MetricConfiguration]] = {
            column_name: [
                MetricConfiguration(
                    metric_name=limit_mode.metric_name_defining_limit,
                    metric_domain_kwargs={
                        "column": column_name,
                        "batch_id": batch_id,
                    },
                    metric_value_kwargs=None,
                    metric_dependencies=None,
                )
                for batch_id in batch_ids
            ]
            for column_name in column_names
        }

        return metric_configurations

    def _column_names_meeting_cardinality_limit(
        self,
        validator: "Validator",  # noqa: F821
        metrics_for_cardinality_check: Dict[str, List[MetricConfiguration]],
    ) -> List[str]:
        """Compute cardinality and return column names meeting cardinality limit.

        Args:
            validator: Validator used to compute column cardinality.
            metrics_for_cardinality_check: metric configurations used to compute cardinality.

        Returns:
            List of column names meeting cardinality.
        """
        column_name: str
        resolved_metrics: Dict[Tuple[str, str, str], Any]
        metric_value: Any

        resolved_metrics_by_column_name: Dict[
            str, Dict[Tuple[str, str, str], Any]
        ] = self.get_resolved_metrics_by_column_name(
            validator=validator,
            metric_configurations_by_column_name=metrics_for_cardinality_check,
        )

        candidate_column_names: List[str] = [
            column_name
            for column_name, resolved_metrics in resolved_metrics_by_column_name.items()
            if all(
                [
                    self.cardinality_checker.cardinality_within_limit(
                        metric_value=metric_value
                    )
                    for metric_value in list(resolved_metrics.values())
                ]
            )
        ]

        return candidate_column_names
