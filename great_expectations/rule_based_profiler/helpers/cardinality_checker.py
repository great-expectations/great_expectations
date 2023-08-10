from __future__ import annotations

import abc
import enum
from dataclasses import dataclass
from typing import Union, cast

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.exceptions import ProfilerConfigurationError
from great_expectations.types import SerializableDictDot


@dataclass(frozen=True)
class CardinalityLimit(abc.ABC, SerializableDictDot):
    name: str


@dataclass(frozen=True)
class RelativeCardinalityLimit(CardinalityLimit):
    max_proportion_unique: float
    metric_name_defining_limit: str = "column.unique_proportion"

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(
            {
                "name": self.name,
                "max_proportion_unique": self.max_proportion_unique,
                "metric_name_defining_limit": self.metric_name_defining_limit,
            }
        )


@dataclass(frozen=True)
class AbsoluteCardinalityLimit(CardinalityLimit):
    max_unique_values: int
    metric_name_defining_limit: str = "column.distinct_values.count"

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(
            {
                "name": self.name,
                "max_proportion_unique": self.max_unique_values,
                "metric_name_defining_limit": self.metric_name_defining_limit,
            }
        )


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
    REL_100 = RelativeCardinalityLimit("REL_100", 1.0)
    ONE_PCT = RelativeCardinalityLimit("ONE_PCT", 0.01)
    TEN_PCT = RelativeCardinalityLimit("TEN_PCT", 0.10)


class CardinalityChecker:
    """Handles cardinality checking given cardinality limit mode and measured value.

    This class also validates cardinality limit settings and converts from
    various types of settings. You can choose one of the attributes listed
    below to create an instance.

    Attributes:
        cardinality_limit_mode: CardinalityLimitMode or string name of the mode
            defining the maximum allowable cardinality.
        max_unique_values: number of max unique rows for a custom
            cardinality limit.
        max_proportion_unique: proportion of unique values for a
            custom cardinality limit.
    """

    SUPPORTED_CARDINALITY_LIMIT_MODE_CLASSES = (
        AbsoluteCardinalityLimit,
        RelativeCardinalityLimit,
    )
    SUPPORTED_LIMIT_MODE_CLASS_NAMES = (
        mode.__name__ for mode in SUPPORTED_CARDINALITY_LIMIT_MODE_CLASSES
    )
    SUPPORTED_CARDINALITY_LIMIT_MODE_STRINGS = (
        mode.name for mode in CardinalityLimitMode
    )

    def __init__(
        self,
        cardinality_limit_mode: str | CardinalityLimitMode | dict | None = None,
        max_unique_values: int | None = None,
        max_proportion_unique: float | None = None,
    ) -> None:
        self._cardinality_limit_mode = self._convert_to_cardinality_limit_mode(
            cardinality_limit_mode=cardinality_limit_mode,
            max_unique_values=max_unique_values,
            max_proportion_unique=max_proportion_unique,
        )

    @property
    def cardinality_limit_mode(
        self,
    ) -> AbsoluteCardinalityLimit | RelativeCardinalityLimit:
        return self._cardinality_limit_mode

    def cardinality_within_limit(self, metric_value: Union[int, float]) -> bool:
        """Determine if the cardinality is within configured limit.

        The metric_value supplied should be either a proportion of unique values
        or number of unique values based on the configured cardinality limit.

        Args:
            metric_value: int if number of unique values, float if proportion
                of unique values.

        Returns:
            Boolean of whether the cardinality is within the configured limit
        """
        self._validate_metric_value(metric_value=metric_value)
        if isinstance(self._cardinality_limit_mode, AbsoluteCardinalityLimit):
            return metric_value <= self._cardinality_limit_mode.max_unique_values

        if isinstance(self._cardinality_limit_mode, RelativeCardinalityLimit):
            return (
                float(metric_value)
                <= self._cardinality_limit_mode.max_proportion_unique
            )

        raise ValueError(
            f'Unknown "cardinality_limit_mode" mode "{self._cardinality_limit_mode}" encountered.'
        )

    @staticmethod
    def _validate_metric_value(metric_value: Union[int, float]) -> None:
        if not isinstance(metric_value, (int, float)):
            raise ProfilerConfigurationError(
                f"Value of measured cardinality must be of type int or float, you provided {type(metric_value)}"
            )

        if metric_value < 0.00:  # noqa: PLR2004
            raise ProfilerConfigurationError(
                f"Value of cardinality (number of rows or percent unique) should be greater than 0.00, your value is {metric_value}"
            )

    @staticmethod
    def _to_cardinality_limit_mode(
        cardinality_limit_mode: str | CardinalityLimitMode | dict | None = None,
    ):
        if isinstance(cardinality_limit_mode, str):
            try:
                return CardinalityLimitMode[cardinality_limit_mode.upper()].value
            except KeyError:
                raise ProfilerConfigurationError(
                    f"Please specify a supported cardinality mode. Supported cardinality modes are {[member.name for member in CardinalityLimitMode]}"
                )
        elif isinstance(cardinality_limit_mode, dict):
            validate_input_parameters(
                cardinality_limit_mode=cardinality_limit_mode.get("name"),
                max_unique_values=cardinality_limit_mode.get("max_unique_values"),
                max_proportion_unique=cardinality_limit_mode.get(
                    "max_proportion_unique"
                ),
                required_num_supplied_params=2,
            )
            try:
                return AbsoluteCardinalityLimit(
                    name=cardinality_limit_mode["name"],
                    max_unique_values=cardinality_limit_mode["max_unique_values"],
                    metric_name_defining_limit=cardinality_limit_mode[
                        "metric_name_defining_limit"
                    ],
                )
            except (KeyError, ValueError):
                try:
                    return RelativeCardinalityLimit(
                        name=cardinality_limit_mode["name"],
                        max_proportion_unique=cardinality_limit_mode[
                            "max_proportion_unique"
                        ],
                        metric_name_defining_limit=cardinality_limit_mode[
                            "metric_name_defining_limit"
                        ],
                    )
                except (KeyError, ValueError):
                    raise ProfilerConfigurationError(
                        f"Please specify a supported cardinality mode.  Supported cardinality modes are {[member.name for member in CardinalityLimitMode]}"
                    )
        else:
            return cast(CardinalityLimitMode, cardinality_limit_mode).value

    @staticmethod
    def _convert_to_cardinality_limit_mode(
        cardinality_limit_mode: str | CardinalityLimitMode | dict | None = None,
        max_unique_values: int | None = None,
        max_proportion_unique: float | None = None,
    ) -> AbsoluteCardinalityLimit | RelativeCardinalityLimit:
        validate_input_parameters(
            cardinality_limit_mode=cardinality_limit_mode,
            max_unique_values=max_unique_values,
            max_proportion_unique=max_proportion_unique,
        )

        if cardinality_limit_mode is not None:
            return CardinalityChecker._to_cardinality_limit_mode(cardinality_limit_mode)
        elif max_unique_values is not None:
            return AbsoluteCardinalityLimit(
                name=f"CUSTOM_ABS_{max_unique_values}",
                max_unique_values=max_unique_values,
            )
        else:
            assert (
                max_proportion_unique is not None
            ), "Guaranteed to have `max_proportion_unique` due to prior call to `validate_input_parameters`"
            return RelativeCardinalityLimit(
                name=f"CUSTOM_REL_{max_proportion_unique}",
                max_proportion_unique=max_proportion_unique,
            )


def validate_input_parameters(
    cardinality_limit_mode: str | CardinalityLimitMode | dict | None = None,
    max_unique_values: int | None = None,
    max_proportion_unique: float | None = None,
    required_num_supplied_params: int = 1,
) -> None:
    num_supplied_params: int = sum(
        0 if param is None else 1
        for param in (
            cardinality_limit_mode,
            max_unique_values,
            max_proportion_unique,
        )
    )
    if num_supplied_params != required_num_supplied_params:
        raise ProfilerConfigurationError(
            f"Please pass ONE of the following parameters: cardinality_limit_mode, max_unique_values, max_proportion_unique, you passed {num_supplied_params} parameters."
        )

    if cardinality_limit_mode is not None:
        if not isinstance(cardinality_limit_mode, (str, CardinalityLimitMode, dict)):
            raise ProfilerConfigurationError(
                f"Please specify a supported cardinality limit type, supported classes are {','.join(CardinalityChecker.SUPPORTED_LIMIT_MODE_CLASS_NAMES)} and supported strings are {','.join(CardinalityChecker.SUPPORTED_CARDINALITY_LIMIT_MODE_STRINGS)}"
            )

        if required_num_supplied_params == 2:  # noqa: PLR2004
            try:
                assert isinstance(cardinality_limit_mode, str)
                return CardinalityLimitMode[cardinality_limit_mode.upper()].value
            except KeyError:
                raise ProfilerConfigurationError(
                    f"Please specify a supported cardinality mode. Supported cardinality modes are {[member.name for member in CardinalityLimitMode]}"
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
