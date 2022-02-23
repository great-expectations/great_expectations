import abc
import enum
from dataclasses import dataclass
from typing import Optional, Tuple, Union

from great_expectations.exceptions import ProfilerConfigurationError


@dataclass
class CardinalityLimit(abc.ABC):
    name: str


@dataclass
class RelativeCardinalityLimit(CardinalityLimit):
    max_proportion_unique: float
    related_metric_name: str = "column.unique_proportion"


@dataclass
class AbsoluteCardinalityLimit(CardinalityLimit):
    max_unique_values: int
    related_metric_name: str = "column.distinct_values.count"


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
        cardinality_limit_mode: Optional[CardinalityLimitMode] = None,
        cardinality_max_rows: Optional[int] = None,
        cardinality_max_proportion_unique: Optional[int] = None,
    ):
        self.supported_cardinality_limit_mode_class_names = (
            mode.__name__ for mode in self.SUPPORTED_CARDINALITY_LIMIT_MODE_CLASSES
        )

        self._cardinality_limit_mode = self._convert_to_cardinality_mode(
            cardinality_limit_mode=cardinality_limit_mode,
            cardinality_max_rows=cardinality_max_rows,
            cardinality_max_proportion_unique=cardinality_max_proportion_unique,
        )

    @property
    def cardinality_limit_mode(self) -> CardinalityLimitMode:
        return self._cardinality_limit_mode

    def cardinality_within_limit(self, metric_value: Union[int, float]) -> bool:
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

    def _convert_to_cardinality_mode(
        self,
        cardinality_limit_mode: Optional[CardinalityLimitMode] = None,
        cardinality_max_rows: Optional[int] = None,
        cardinality_max_proportion_unique: Optional[int] = None,
    ):

        self._validate_input_parameters(
            cardinality_limit_mode=cardinality_limit_mode,
            cardinality_max_rows=cardinality_max_rows,
            cardinality_max_proportion_unique=cardinality_max_proportion_unique,
        )

        if cardinality_limit_mode is not None:
            if isinstance(cardinality_limit_mode, str):
                return CardinalityLimitMode[cardinality_limit_mode.upper()].value
            else:
                return cardinality_limit_mode
        if cardinality_max_rows is not None:
            return AbsoluteCardinalityLimit(
                name=f"custom_abs_{cardinality_max_rows}",
                max_unique_values=cardinality_max_rows,
            )
        if cardinality_max_proportion_unique is not None:
            return RelativeCardinalityLimit(
                name=f"custom_rel_{cardinality_max_proportion_unique}",
                max_proportion_unique=cardinality_max_proportion_unique,
            )

    def _validate_input_parameters(
        self,
        cardinality_limit_mode: Optional[Union[CardinalityLimitMode, str]] = None,
        cardinality_max_rows: Optional[int] = None,
        cardinality_max_proportion_unique: Optional[int] = None,
    ):
        params = (
            cardinality_limit_mode,
            cardinality_max_rows,
            cardinality_max_proportion_unique,
        )
        num_params: int = sum([0 if param is None else 1 for param in params])
        if num_params != 1:
            raise ProfilerConfigurationError(
                f"Please pass ONE of the following parameters: cardinality_limit_mode, cardinality_max_rows, cardinality_max_proportion_unique, you passed {num_params} parameters."
            )

        if cardinality_limit_mode is not None:
            if isinstance(cardinality_limit_mode, CardinalityLimit) or isinstance(
                cardinality_limit_mode, str
            ):
                pass
            else:
                raise ProfilerConfigurationError(
                    f"Please specify a supported cardinality limit type, supported classes are {','.join(self.supported_cardinality_limit_mode_class_names)} and supported strings are {','.join(self.SUPPORTED_CARDINALITY_LIMIT_MODE_STRINGS)}"
                )
