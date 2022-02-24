from typing import Dict, List, Optional, Union

from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.exceptions import ProfilerConfigurationError
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.domain_builder.cardinality_checker import (
    CardinalityChecker,
    CardinalityLimitMode,
)
from great_expectations.rule_based_profiler.domain_builder.domain_builder import (
    build_domains_from_column_names,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import Validator


class CategoricalColumnDomainBuilder(DomainBuilder):
    """
    This DomainBuilder uses column cardinality to identify domains.
    """

    def __init__(
        self,
        data_context: "DataContext",  # noqa: F821
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
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
            data_context: DataContext associated with this profiler.
            batch_request: BatchRequest to be optionally used to define batches
                to consider for this domain builder.
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
            data_context=data_context,
            batch_request=batch_request,
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

        batch_ids: List[str] = self._get_batch_ids(variables=variables)
        validator: Validator = self.get_validator(variables=variables)

        # Here we use a single get_metric call to get column names to build the
        # rest of the metrics.
        table_column_names: List[str] = self._get_table_column_names_from_active_batch(
            validator=validator,
        )

        metrics_for_cardinality_check: List[
            Dict[str, List[MetricConfiguration]]
        ] = self._generate_metric_configurations_to_check_cardinality(
            batch_ids=batch_ids, column_names=table_column_names
        )

        candidate_column_names: List[str] = self._columns_meeting_cardinality_limit(
            validator=validator,
            table_column_names=table_column_names,
            metrics_for_cardinality_check=metrics_for_cardinality_check,
        )

        column_domains: List[Domain] = build_domains_from_column_names(
            candidate_column_names
        )

        return column_domains

    def _get_table_column_names_from_active_batch(
        self,
        validator: Validator,
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
                    "batch_id": validator.active_batch_id,
                },
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )

        if self._exclude_columns is not None:
            table_column_names = [
                colname
                for colname in table_column_names
                if colname not in self._exclude_columns
            ]

        return table_column_names

    def _generate_metric_configurations_to_check_cardinality(
        self,
        batch_ids: List[str],
        column_names: List[str],
    ) -> List[Dict[str, List[MetricConfiguration]]]:
        """Generate metric configurations used to compute metrics for checking cardinality.

        Args:
            batch_ids: List of batch_ids used to create metric configurations.
            column_names: List of column names used to create metric configurations.

        Returns:
            List of dicts of the form [{column_name: List[MetricConfiguration]},...]
        """

        limit_mode: CardinalityLimitMode = self._cardinality_checker.limit_mode

        column_name: str
        metric_configurations: List[Dict[str, List[MetricConfiguration]]] = [
            {
                column_name: [
                    MetricConfiguration(
                        metric_name=limit_mode.metric_name_defining_limit,
                        metric_domain_kwargs={
                            "batch_id": batch_id,
                            "column": column_name,
                        },
                        metric_value_kwargs=None,
                        metric_dependencies=None,
                    )
                    for batch_id in batch_ids
                ]
            }
            for column_name in column_names
        ]

        return metric_configurations

    def _columns_meeting_cardinality_limit(
        self,
        validator: Validator,
        table_column_names: List[str],
        metrics_for_cardinality_check: List[Dict[str, List[MetricConfiguration]]],
    ) -> List[str]:
        """Compute cardinality and return column names meeting cardinality limit.

        Args:
            validator: Validator used to compute column cardinality.
            table_column_names: column names to verify cardinality.
            metrics_for_cardinality_check: metric configurations used to
                compute cardinality.

        Returns:
            List of column names meeting cardinality.
        """
        candidate_column_names: List[str] = table_column_names.copy()

        # TODO AJB 20220218: Remove loops and refactor into single call to validator.get_metrics()
        #  by first building up MetricConfigurations to compute. See GREAT-584
        for metric_for_cardinality_check in metrics_for_cardinality_check:
            for (
                column_name,
                metric_config_batch_list,
            ) in metric_for_cardinality_check.items():
                if column_name not in candidate_column_names:
                    continue
                else:
                    for metric_config in metric_config_batch_list:

                        metric_value = validator.get_metric(metric=metric_config)

                        batch_cardinality_within_limit: bool = (
                            self._cardinality_checker.cardinality_within_limit(
                                metric_value=metric_value
                            )
                        )

                        if batch_cardinality_within_limit is False:
                            candidate_column_names.remove(column_name)
                            break

        return candidate_column_names
