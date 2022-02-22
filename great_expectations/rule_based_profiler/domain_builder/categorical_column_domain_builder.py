import enum
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.exceptions import ProfilerConfigurationError
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.domain_builder.domain_builder import (
    build_domains_from_column_names,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import Validator


@dataclass
class ProportionalCardinalityLimit:
    name: str
    max_proportion_unique: float


@dataclass
class AbsoluteCardinalityLimit:
    name: str
    max_unique_values: int


class CardinalityMode(enum.Enum):
    """Used to determine appropriate Expectation configurations based on data.

    Defines relative and absolute number of records (table rows) that
    correspond to each cardinality category.

    """

    ONE = AbsoluteCardinalityLimit("one", 1)
    VERY_FEW = AbsoluteCardinalityLimit("very_few", 60)
    # TODO AJB 20220216: add further implementation
    # raise NotImplementedError


class zz__CardinalityModeProcessor:
    """Check cardinality"""

    # TODO AJB 20220218: add further implementation, factor this out and test separately, change name
    def __init__(self, mode, limit):
        raise NotImplementedError

    def cardinality_within_limit(self):
        raise NotImplementedError


class CategoricalColumnDomainBuilder(DomainBuilder):
    """ """

    def __init__(
        self,
        data_context: "DataContext",  # noqa: F821
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        cardinality_limit: Optional[Union[CardinalityMode, str]] = None,
        exclude_columns: Optional[List[str]] = None,
    ):
        """Filter columns with unique values no greater than cardinality_limit.

        Args:
            data_context: DataContext associated with this profiler.
            batch_request: BatchRequest to be optionally used to define batches
                to consider for this domain builder.
            cardinality_limit: CardinalityMode to use when filtering columns.
            exclude_columns: If provided, exclude columns from consideration.
        """

        super().__init__(
            data_context=data_context,
            batch_request=batch_request,
        )

        if cardinality_limit is None:
            cardinality_limit = CardinalityMode.VERY_FEW.value

        self._cardinality_limit = cardinality_limit

        self._exclude_columns = exclude_columns

    @property
    def domain_type(self) -> Union[str, MetricDomainTypes]:
        return MetricDomainTypes.COLUMN

    @property
    def cardinality_limit(self) -> CardinalityMode:
        """Process cardinality_limit which is passed as a string from config.

        Returns:
            CardinalityMode default if no cardinality_limit is set,
            the corresponding CardinalityMode if passed as a string or
            a passthrough if supplied as CardinalityMode.

        """
        # TODO AJB 20220222: Add error handling, test
        if self._cardinality_limit is None:
            return CardinalityMode.VERY_FEW.value
        elif isinstance(self._cardinality_limit, str):
            return CardinalityMode[self._cardinality_limit.upper()].value
        else:
            return self._cardinality_limit.value

    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """Return domains matching the selected cardinality_limit.

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
            variables=variables
        )

        metrics_for_cardinality_check: List[
            Dict[str, List[MetricConfiguration]]
        ] = self._generate_metric_configurations_to_check_cardinality(
            batch_ids=batch_ids, column_names=table_column_names
        )

        # TODO AJB 20220218: Remove loops and refactor into single call to validator.get_metrics()
        #  by first building up MetricConfigurations to compute see GREAT-584
        computed_metrics_for_cardinality_check: List[
            Dict[str, List[Tuple[MetricConfiguration, Any]]]
        ] = []

        candidate_column_names: List[str] = table_column_names.copy()

        cardinality_limit: CardinalityMode = self.cardinality_limit

        print("cardinality_limit", cardinality_limit)

        for metric_for_cardinality_check in metrics_for_cardinality_check:
            for (
                column_name,
                metric_config_batch_list,
            ) in metric_for_cardinality_check.items():
                if column_name not in candidate_column_names:
                    continue
                else:
                    column_inclusion: bool = True
                    for metric_config in metric_config_batch_list:
                        metric_value = validator.get_metric(metric=metric_config)
                        if metric_config.metric_name == "column.distinct_values.count":
                            if metric_value > cardinality_limit.max_unique_values:
                                column_inclusion = False
                        elif metric_config.metric_name == "column.unique_proportion":
                            if metric_value > cardinality_limit.max_proportion_unique:
                                column_inclusion = False
                    if column_inclusion == False:
                        candidate_column_names.remove(column_name)

        print(candidate_column_names)

        print("breakpoint")
        # column_names_meeting_cardinality_limit: List[
        #     str
        # ] = self._columns_meeting_cardinality_limit(
        #     computed_metrics_for_cardinality_check
        # )

        # column_names_meeting_cardinality_limit: List[str] = [
        #     column_name
        #     for column_name in table_column_names
        #     if self._column_cardinality_within_limit(
        #         column=column_name, variables=variables
        #     )
        # ]

        # column_domains: List[Domain] = build_domains_from_column_names(
        #     column_names_meeting_cardinality_limit
        # )

        column_domains: List[Domain] = build_domains_from_column_names(
            candidate_column_names
        )

        return column_domains

    def _get_table_column_names_from_first_batch(
        self,
        variables: Optional[ParameterContainer] = None,
    ):
        """Retrieve table column names from the first loaded batch.

        Args:
            variables: Optional passthrough variables

        Returns:
            List of column names from the first loaded batch.
        """

        batch_ids: List[str] = self._get_batch_ids(variables=variables)

        if len(batch_ids) == 0:
            raise ProfilerConfigurationError(
                "No batch_ids were available, please check your configuration."
            )

        first_batch_id: str = batch_ids[0]

        validator: Validator = self.get_validator(variables=variables)
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

        cardinality_limit: CardinalityMode = self.cardinality_limit

        if isinstance(cardinality_limit, AbsoluteCardinalityLimit):
            metric_name: str = "column.distinct_values.count"
        elif isinstance(cardinality_limit, ProportionalCardinalityLimit):
            metric_name: str = "column.unique_proportion"
        else:
            raise ProfilerConfigurationError(
                f"Unrecognized cardinality_limit: {cardinality_limit}"
            )

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
        self, computed_metrics: List[Tuple[MetricConfiguration, Any]]
    ) -> List[str]:

        for computed_metric in computed_metrics:
            pass

        # TODO AJB 20220218: Implement, incl params
        raise NotImplementedError

    def _column_cardinality_within_limit(
        self,
        column: str,
        variables: Optional[ParameterContainer] = None,
    ) -> bool:

        validator: Validator = self.get_validator(variables=variables)

        batch_id: str = self.get_batch_id(variables=variables)

        cardinality_limit: CardinalityMode = self.cardinality_limit

        if isinstance(cardinality_limit, AbsoluteCardinalityLimit):

            column_distinct_values_count: int = validator.get_metric(
                metric=MetricConfiguration(
                    metric_name="column.distinct_values.count",
                    metric_domain_kwargs={
                        "batch_id": batch_id,
                        "column": column,
                    },
                    metric_value_kwargs=None,
                    metric_dependencies=None,
                )
            )
            return column_distinct_values_count <= cardinality_limit.max_unique_values

        elif isinstance(cardinality_limit, ProportionalCardinalityLimit):

            column_unique_proportion = validator.get_metric(
                metric=MetricConfiguration(
                    metric_name="column.unique_proportion",
                    metric_domain_kwargs={
                        "batch_id": batch_id,
                        "column": column,
                    },
                    metric_value_kwargs=None,
                    metric_dependencies=None,
                )
            )
            return column_unique_proportion <= cardinality_limit.max_proportion_unique
