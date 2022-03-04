import abc
import enum
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple, Union

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.exceptions import ProfilerConfigurationError
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.validator.metric_configuration import MetricConfiguration


@dataclass
class CardinalityLimit(abc.ABC):
    name: str


@dataclass
class RelativeCardinalityLimit(CardinalityLimit):
    max_proportion_unique: float
    metric_name_defining_limit: str = "column.unique_proportion"


@dataclass
class AbsoluteCardinalityLimit(CardinalityLimit):
    max_unique_values: int
    metric_name_defining_limit: str = "column.distinct_values.count"


class CardinalityLimitMode(enum.Enum):
    """Preset limits based on unique values (cardinality)

    Defines relative (ratio) and absolute number of records (table rows) that
    correspond to each cardinality category.

    Used to determine appropriate Expectation configurations based on data.
    """

    ZERO = AbsoluteCardinalityLimit("ZERO", 0)
    ONE = AbsoluteCardinalityLimit("ONE", 1)
    TWO = AbsoluteCardinalityLimit("TWO", 2)
    VERY_FEW = AbsoluteCardinalityLimit("VERY_FEW", 10)
    FEW = AbsoluteCardinalityLimit("FEW", 100)
    SOME = AbsoluteCardinalityLimit("SOME", 1000)
    MANY = AbsoluteCardinalityLimit("MANY", 10000)
    VERY_MANY = AbsoluteCardinalityLimit("VERY_MANY", 100000)
    UNIQUE = RelativeCardinalityLimit("UNIQUE", 1.0)
    ABS_10 = AbsoluteCardinalityLimit("ABS_10", 10)
    ABS_100 = AbsoluteCardinalityLimit("ABS_100", 100)
    ABS_1000 = AbsoluteCardinalityLimit("ABS_1000", 1000)
    ABS_10_000 = AbsoluteCardinalityLimit("ABS_10_000", int(1e4))
    ABS_100_000 = AbsoluteCardinalityLimit("ABS_100_000", int(1e5))
    ABS_1_000_000 = AbsoluteCardinalityLimit("ABS_1_000_000", int(1e6))
    ABS_10_000_000 = AbsoluteCardinalityLimit("ABS_10_000_000", int(1e7))
    ABS_100_000_000 = AbsoluteCardinalityLimit("ABS_100_000_000", int(1e8))
    ABS_1_000_000_000 = AbsoluteCardinalityLimit("ABS_1_000_000_000", int(1e9))
    REL_0 = RelativeCardinalityLimit("REL_0", 0.0)
    REL_001 = RelativeCardinalityLimit("REL_001", 1e-5)
    REL_01 = RelativeCardinalityLimit("REL_01", 1e-4)
    REL_0_1 = RelativeCardinalityLimit("REL_0_1", 1e-3)
    REL_1 = RelativeCardinalityLimit("REL_1", 1e-2)
    REL_10 = RelativeCardinalityLimit("REL_10", 0.10)
    REL_25 = RelativeCardinalityLimit("REL_25", 0.25)
    REL_50 = RelativeCardinalityLimit("REL_50", 0.50)
    REL_75 = RelativeCardinalityLimit("REL_75", 0.75)
    ONE_PCT = RelativeCardinalityLimit("ONE_PCT", 0.01)
    TEN_PCT = RelativeCardinalityLimit("TEN_PCT", 0.10)


class CardinalityChecker:
    """Handles cardinality checking given cardinality limit mode and measured value.

    This class also validates cardinality limit settings and converts from
    various types of settings. You can choose one of the attributes listed
    below to create an instance.

    Attributes:
        limit_mode: CardinalityLimitMode or string name of the mode
            defining the maximum allowable cardinality.
        max_unique_values: number of max unique rows for a custom
            cardinality limit.
        max_proportion_unique: proportion of unique values for a
            custom cardinality limit.
    """

    SUPPORTED_CARDINALITY_LIMIT_MODE_CLASSES: Tuple[
        Union[AbsoluteCardinalityLimit, RelativeCardinalityLimit]
    ] = (
        AbsoluteCardinalityLimit,
        RelativeCardinalityLimit,
    )
    SUPPORTED_CARDINALITY_LIMIT_MODE_STRINGS: Tuple[str] = (
        mode.name for mode in CardinalityLimitMode
    )

    def __init__(
        self,
        limit_mode: Optional[Union[CardinalityLimitMode, str]] = None,
        max_unique_values: Optional[int] = None,
        max_proportion_unique: Optional[float] = None,
    ):
        self.supported_limit_mode_class_names = (
            mode.__name__ for mode in self.SUPPORTED_CARDINALITY_LIMIT_MODE_CLASSES
        )

        self._limit_mode = self._convert_to_cardinality_mode(
            limit_mode=limit_mode,
            max_unique_values=max_unique_values,
            max_proportion_unique=max_proportion_unique,
        )

    @property
    def limit_mode(self) -> Union[AbsoluteCardinalityLimit, RelativeCardinalityLimit]:
        return self._limit_mode

    def cardinality_within_limit(self, metric_value: float) -> bool:
        """Determine if the cardinality is within configured limit.

        The metric_value supplied should be either a proportion of unique values
        or number of unique values based on the configured cardinality limit.

        Args:
            metric_value: int if number of unique values, float if proportion
                of unique values.

        Returns:
            boolean of whether the cardinality is within the configured limit
        """
        self._validate_metric_value(metric_value=metric_value)
        if isinstance(self._limit_mode, AbsoluteCardinalityLimit):
            return metric_value <= self._limit_mode.max_unique_values
        elif isinstance(self._limit_mode, RelativeCardinalityLimit):
            return metric_value <= self._limit_mode.max_proportion_unique

    @staticmethod
    def _validate_metric_value(metric_value: float) -> None:
        if not isinstance(metric_value, (int, float)):
            raise ProfilerConfigurationError(
                f"Value of measured cardinality must be of type int or float, you provided {type(metric_value)}"
            )
        if metric_value < 0.00:
            raise ProfilerConfigurationError(
                f"Value of cardinality (number of rows or percent unique) should be greater than 0.00, your value is {metric_value}"
            )

    def _convert_to_cardinality_mode(
        self,
        limit_mode: Optional[Union[CardinalityLimitMode, str]] = None,
        max_unique_values: Optional[int] = None,
        max_proportion_unique: Optional[float] = None,
    ) -> Union[AbsoluteCardinalityLimit, RelativeCardinalityLimit]:
        self._validate_input_parameters(
            limit_mode=limit_mode,
            max_unique_values=max_unique_values,
            max_proportion_unique=max_proportion_unique,
        )

        if limit_mode is not None:
            if isinstance(limit_mode, str):
                try:
                    return CardinalityLimitMode[limit_mode.upper()].value
                except KeyError:
                    raise ProfilerConfigurationError(
                        f"Please specify a supported cardinality mode. Supported cardinality modes are {[member.name for member in CardinalityLimitMode]}"
                    )
            else:
                return limit_mode.value
        if max_unique_values is not None:
            return AbsoluteCardinalityLimit(
                name=f"CUSTOM_ABS_{max_unique_values}",
                max_unique_values=max_unique_values,
            )
        if max_proportion_unique is not None:
            return RelativeCardinalityLimit(
                name=f"CUSTOM_REL_{max_proportion_unique}",
                max_proportion_unique=max_proportion_unique,
            )

    def _validate_input_parameters(
        self,
        limit_mode: Optional[Union[CardinalityLimitMode, str]] = None,
        max_unique_values: Optional[int] = None,
        max_proportion_unique: Optional[int] = None,
    ) -> None:
        num_supplied_params: int = sum(
            [
                0 if param is None else 1
                for param in (
                    limit_mode,
                    max_unique_values,
                    max_proportion_unique,
                )
            ]
        )
        if num_supplied_params != 1:
            raise ProfilerConfigurationError(
                f"Please pass ONE of the following parameters: limit_mode, max_unique_values, max_proportion_unique, you passed {num_supplied_params} parameters."
            )

        if limit_mode is not None:
            if not (
                isinstance(limit_mode, CardinalityLimitMode)
                or isinstance(limit_mode, str)
            ):
                raise ProfilerConfigurationError(
                    f"Please specify a supported cardinality limit type, supported classes are {','.join(self.supported_limit_mode_class_names)} and supported strings are {','.join(self.SUPPORTED_CARDINALITY_LIMIT_MODE_STRINGS)}"
                )
        if max_unique_values is not None:
            if not isinstance(max_unique_values, int):
                raise ProfilerConfigurationError(
                    f"Please specify an int, you specified a {type(max_unique_values)}"
                )
        if max_proportion_unique is not None:
            if not isinstance(max_proportion_unique, (float, int)):
                raise ProfilerConfigurationError(
                    f"Please specify a float or int, you specified a {type(max_proportion_unique)}"
                )


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

        # Here we use a single get_metric call to get column names to build the
        # rest of the metrics.
        batch_ids: List[str] = self.get_batch_ids(variables=variables)

        table_column_names: List[str] = self._get_table_column_names_from_active_batch(
            validator=validator,
            batch_id=batch_ids[-1],  # active_batch_id
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

        column_name: str
        domains: List[Domain] = [
            Domain(
                domain_type=self.domain_type,
                domain_kwargs={
                    "column": column_name,
                },
            )
            for column_name in candidate_column_names
        ]

        return domains

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
    ) -> List[Dict[str, List[MetricConfiguration]]]:
        """Generate metric configurations used to compute metrics for checking cardinality.

        Args:
            batch_ids: List of batch_ids used to create metric configurations.
            column_names: List of column names used to create metric configurations.

        Returns:
            List of dicts of the form [{column_name: List[MetricConfiguration]},...]
        """

        limit_mode: CardinalityLimitMode = self.cardinality_checker.limit_mode

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
        validator: "Validator",  # noqa: F821
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
                            self.cardinality_checker.cardinality_within_limit(
                                metric_value=metric_value
                            )
                        )

                        if batch_cardinality_within_limit is False:
                            candidate_column_names.remove(column_name)
                            break

        return candidate_column_names
