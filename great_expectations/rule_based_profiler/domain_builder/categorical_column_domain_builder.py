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
    """ """

    def __init__(
        self,
        data_context: "DataContext",  # noqa: F821
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        limit_mode: Optional[Union[CardinalityLimitMode, str]] = None,
        max_unique_values: Optional[int] = None,
        max_proportion_unique: Optional[int] = None,
        exclude_columns: Optional[List[str]] = None,
    ):
        """Filter columns with unique values no greater than limit_mode.

        Args:
            data_context: DataContext associated with this profiler.
            batch_request: BatchRequest to be optionally used to define batches
                to consider for this domain builder.
            limit_mode: CardinalityLimitMode to use when filtering columns.
            exclude_columns: If provided, exclude columns from consideration.
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

        Cardinality categories define a maximum number of unique items that
        can be contained in a given domain. If this number is exceeded, the
        domain is not included for the currently executing rule.
        This filter considers unique values across all supplied batches.

        Args:
            variables: Optional variables to substitute when evaluating.

        Returns:
            List of domains that match the desired cardinality.
        """

        batch_ids: List[str] = self._get_batch_ids(variables=variables)
        validator: Validator = self.get_validator(variables=variables)

        # Here we use a single get_metric call to get column names to build the
        # rest of the metrics.
        table_column_names: List[str] = self._get_table_column_names_from_first_batch(
            validator=validator,
            batch_ids=batch_ids,
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

    def _get_table_column_names_from_first_batch(
        self,
        validator: Validator,
        batch_ids: List[str],
    ):
        """Retrieve table column names from the first loaded batch.

        Args:
            validator: Validator to use in retrieving columns.
            batch_ids: Batches used in this profiler rule.

        Returns:
            List of column names from the first loaded batch.
        """

        if len(batch_ids) == 0:
            raise ProfilerConfigurationError(
                "No batch_ids were available, please check your configuration."
            )

        first_batch_id: str = batch_ids[0]

        table_column_names: List[str] = validator.get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs={
                    "batch_id": first_batch_id,
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

        metric_name: str = limit_mode.related_metric_name

        metric_configurations: List[Dict[str, List[MetricConfiguration]]] = []

        for column_name in column_names:
            metric_configurations.append(
                {
                    column_name: [
                        MetricConfiguration(
                            metric_name=metric_name,
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
            )

        return metric_configurations

    def _columns_meeting_cardinality_limit(
        self,
        validator: Validator,
        table_column_names: List[str],
        metrics_for_cardinality_check: List[Dict[str, List[MetricConfiguration]]],
    ) -> List[str]:
        """

        Args:
            validator: Validator used to compute column cardinality.
            table_column_names:
            metrics_for_cardinality_check:

        Returns:

        """
        candidate_column_names: List[str] = table_column_names.copy()

        # TODO AJB 20220218: Remove loops and refactor into single call to validator.get_metrics()
        #  by first building up MetricConfigurations to compute see GREAT-584
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
