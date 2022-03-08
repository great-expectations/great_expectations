import itertools
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
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


class CategoricalColumnDomainBuilder(DomainBuilder):
    """
    This DomainBuilder uses column cardinality to identify domains.
    """

    exclude_field_names: Set[str] = DomainBuilder.exclude_field_names | {
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
        validator: "Validator" = self.get_validator(variables=variables)  # noqa: F821

        batch_ids: List[str] = self.get_batch_ids(variables=variables)

        table_column_names: List[str] = self._get_table_column_names_from_active_batch(
            validator=validator,
            batch_id=batch_ids[-1],  # active_batch_id
        )

        metrics_for_cardinality_check: Dict[
            str, List[MetricConfiguration]
        ] = self._generate_metric_configurations_to_check_cardinality(
            batch_ids=batch_ids, column_names=table_column_names
        )

        candidate_column_names: List[str] = self._columns_meeting_cardinality_limit(
            validator=validator,
            metrics_for_cardinality_check=metrics_for_cardinality_check,
        )

        return build_simple_domains_from_column_names(
            column_names=candidate_column_names,
            domain_type=self.domain_type,
        )

    def _get_table_column_names_from_active_batch(
        self,
        validator: "Validator",  # noqa: F821
        batch_id: str,
    ):
        """Retrieve table column names from the active batch.

        Args:
            validator: Validator to use in retrieving columns.

        Returns:
            List of column names from the active batch.
        """

        table_column_names: List[str] = validator.get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs={
                    "batch_id": batch_id,
                },
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )

        if self.exclude_columns is not None:
            table_column_names = [
                colname
                for colname in table_column_names
                if colname not in self.exclude_columns
            ]

        return table_column_names

    def _generate_metric_configurations_to_check_cardinality(
        self,
        batch_ids: List[str],
        column_names: List[str],
    ) -> Dict[str, List[MetricConfiguration]]:
        """Generate metric configurations used to compute metrics for checking cardinality.

        Args:
            batch_ids: List of batch_ids used to create metric configurations.
            column_names: List of column names used to create metric configurations.

        Returns:
            Dictionary of the form {
                column_name: List[MetricConfiguration],
            }
        """

        limit_mode: Union[
            AbsoluteCardinalityLimit, RelativeCardinalityLimit
        ] = self.cardinality_checker.limit_mode

        column_name: str
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

    def _columns_meeting_cardinality_limit(
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
        metric_configuration: MetricConfiguration
        metric_configurations_for_column_name: List[MetricConfiguration]

        # Step 1: Gather "MetricConfiguration" objects corresponding to all possible column_name/batch_id combinations.
        # and compute all metric values (resolve "MetricConfiguration" objects ) using a single method call.
        resolved_metrics: Dict[Tuple[str, str, str], Any] = validator.compute_metrics(
            metric_configurations=[
                metric_configuration
                for column_name, metric_configurations_for_column_name in metrics_for_cardinality_check.items()
                for metric_configuration in metric_configurations_for_column_name
            ]
        )

        # Step 2: Gather "MetricConfiguration" ID values for each column_name (one element per batch_id in every list).
        metric_configuration_ids_by_column_name: Dict[
            str, List[Tuple[str, str, str]]
        ] = {
            column_name: [metric_configuration.id]
            for column_name, metric_configurations_for_column_name in metrics_for_cardinality_check.items()
            for metric_configuration in metric_configurations_for_column_name
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
        # interest (reflecting specified column_name/batch_id combinations) and pass "CardinalityChecker" limit test.
        metric_configuration_id: Tuple[str, str, str]
        metric_value: Any
        resolved_metrics = {
            metric_configuration_id: metric_value
            for metric_configuration_id, metric_value in resolved_metrics.items()
            if metric_configuration_id in metric_configuration_ids_all_column_names
            and self.cardinality_checker.cardinality_within_limit(
                metric_value=metric_value
            )
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

        return candidate_column_names
