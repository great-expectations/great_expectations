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
class RelativeCardinalityLimit:
    name: str
    max_proportion_unique: float


@dataclass
class AbsoluteCardinalityLimit:
    name: str
    max_unique_values: int


class CardinalityLimitMode(enum.Enum):
    """Used to determine appropriate Expectation configurations based on data.

    Defines relative (ratio) and absolute number of records (table rows) that
    correspond to each cardinality category.
    """

    ONE = AbsoluteCardinalityLimit("one", 1)
    TWO = AbsoluteCardinalityLimit("two", 2)
    VERY_FEW = AbsoluteCardinalityLimit("very_few", 10)
    FEW = AbsoluteCardinalityLimit("few", 100)
    SOME = AbsoluteCardinalityLimit("some", 1000)
    MANY = AbsoluteCardinalityLimit("many", 10000)
    VERY_MANY = AbsoluteCardinalityLimit("very_many", 100000)
    UNIQUE = RelativeCardinalityLimit("unique", 1.0)
    ABS_10 = AbsoluteCardinalityLimit("abs_10", 10)
    ABS_100 = AbsoluteCardinalityLimit("abs_100", 100)
    ABS_1000 = AbsoluteCardinalityLimit("abs_1000", 1000)
    ABS_10_000 = AbsoluteCardinalityLimit("abs_10_000", int(1e4))
    ABS_100_000 = AbsoluteCardinalityLimit("abs_100_000", int(1e5))
    ABS_1_000_000 = AbsoluteCardinalityLimit("abs_1_000_000", int(1e6))
    ABS_10_000_000 = AbsoluteCardinalityLimit("abs_10_000_000", int(1e7))
    ABS_100_000_000 = AbsoluteCardinalityLimit("abs_100_000_000", int(1e8))
    ABS_1_000_000_000 = AbsoluteCardinalityLimit("abs_1_000_000_000", int(1e9))
    REL_001 = RelativeCardinalityLimit("rel_001", 1e-5)
    REL_01 = RelativeCardinalityLimit("rel_01", 1e-4)
    REL_0_1 = RelativeCardinalityLimit("rel_0_1", 1e-3)
    REL_1 = RelativeCardinalityLimit("rel_1", 1e-2)
    REL_10 = RelativeCardinalityLimit("rel_10", 0.10)
    REL_25 = RelativeCardinalityLimit("rel_25", 0.25)
    REL_50 = RelativeCardinalityLimit("rel_50", 0.50)
    REL_75 = RelativeCardinalityLimit("rel_75", 0.75)
    ONE_PCT = RelativeCardinalityLimit("one_pct", 0.01)
    TEN_PCT = RelativeCardinalityLimit("ten_pct", 0.10)


class CardinalityChecker:
    """Handles cardinality checking given cardinality limit mode and measured value.

    Attributes:
        cardinality_limit_mode: CardinalityLimitMode defining the maximum
            allowable cardinality.
    """

    def __init__(self, cardinality_limit_mode: CardinalityLimitMode):
        self.supported_cardinality_limit_modes = (
            AbsoluteCardinalityLimit,
            RelativeCardinalityLimit,
        )
        self.supported_cardinality_limit_mode_names = (
            mode.__name__ for mode in self.supported_cardinality_limit_modes
        )
        self._cardinality_limit_mode = cardinality_limit_mode
        if not isinstance(
            self._cardinality_limit_mode, self.supported_cardinality_limit_modes
        ):
            raise ProfilerConfigurationError(
                f"Please specify a supported cardinality limit type, supported types are {','.join(self.supported_cardinality_limit_mode_names)}"
            )

    def cardinality_within_limit(self, metric_value: Union[int, float]):
        self._validate_metric_value(metric_value=metric_value)
        if isinstance(self._cardinality_limit_mode, AbsoluteCardinalityLimit):
            return metric_value <= self._cardinality_limit_mode.max_unique_values
        elif isinstance(self._cardinality_limit_mode, RelativeCardinalityLimit):
            return metric_value <= self._cardinality_limit_mode.max_proportion_unique

    @staticmethod
    def _validate_metric_value(metric_value: Union[int, float]):
        if not isinstance(metric_value, (int, float)):
            raise ProfilerConfigurationError(
                f"Value of measured cardinality must be of type int or float, you provided {type(metric_value)}"
            )
        if metric_value < 0.00:
            raise ProfilerConfigurationError(
                f"Value of cardinality (number of rows or percent unique) should be greater than 0.00, your value is {metric_value}"
            )


class CategoricalColumnDomainBuilder(DomainBuilder):
    """ """

    def __init__(
        self,
        data_context: "DataContext",  # noqa: F821
        batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None,
        cardinality_limit_mode: Optional[Union[CardinalityLimitMode, str]] = None,
        exclude_columns: Optional[List[str]] = None,
    ):
        """Filter columns with unique values no greater than cardinality_limit_mode.

        Args:
            data_context: DataContext associated with this profiler.
            batch_request: BatchRequest to be optionally used to define batches
                to consider for this domain builder.
            cardinality_limit_mode: CardinalityLimitMode to use when filtering columns.
            exclude_columns: If provided, exclude columns from consideration.
        """

        super().__init__(
            data_context=data_context,
            batch_request=batch_request,
        )

        if cardinality_limit_mode is None:
            cardinality_limit_mode = CardinalityLimitMode.VERY_FEW.value

        self._cardinality_limit_mode = cardinality_limit_mode
        self._cardinality_checker = CardinalityChecker(
            cardinality_limit_mode=self.cardinality_limit_mode
        )

        self._exclude_columns = exclude_columns

    @property
    def domain_type(self) -> Union[str, MetricDomainTypes]:
        return MetricDomainTypes.COLUMN

    @property
    def cardinality_limit_mode(self) -> CardinalityLimitMode:
        """Process cardinality_limit_mode which is passed as a string from config.

        Returns:
            CardinalityLimitMode default if no cardinality_limit_mode is set,
            the corresponding CardinalityLimitMode if passed as a string or
            a passthrough if supplied as CardinalityLimitMode.

        """
        # TODO AJB 20220222: Add error handling, test
        if self._cardinality_limit_mode is None:
            return CardinalityLimitMode.VERY_FEW.value
        elif isinstance(self._cardinality_limit_mode, str):
            return CardinalityLimitMode[self._cardinality_limit_mode.upper()].value
        else:
            return self._cardinality_limit_mode.value

    def _get_domains(
        self,
        variables: Optional[ParameterContainer] = None,
    ) -> List[Domain]:
        """Return domains matching the selected cardinality_limit_mode.

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

        cardinality_limit_mode: CardinalityLimitMode = self.cardinality_limit_mode

        if isinstance(cardinality_limit_mode, AbsoluteCardinalityLimit):
            metric_name: str = "column.distinct_values.count"
        elif isinstance(cardinality_limit_mode, RelativeCardinalityLimit):
            metric_name: str = "column.unique_proportion"
        else:
            raise ProfilerConfigurationError(
                f"Unrecognized cardinality_limit_mode: {cardinality_limit_mode}"
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
