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
    def limit_mode(self) -> CardinalityLimitMode:
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
            if isinstance(limit_mode, CardinalityLimitMode) or isinstance(
                limit_mode, str
            ):
                pass
            else:
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
