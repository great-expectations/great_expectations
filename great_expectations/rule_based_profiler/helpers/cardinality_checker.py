import abc
import enum
from dataclasses import dataclass
from typing import Optional, Tuple, Union

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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return convert_to_json_serializable(
            {
                "name": self.name,
                "max_proportion_unique": self.max_unique_values,
                "metric_name_defining_limit": self.metric_name_defining_limit,
            }
        )


class CardinalityLimitMode(enum.Enum):
    "Preset limits based on unique values (cardinality)\n\n    Defines relative (ratio) and absolute number of records (table rows) that\n    correspond to each cardinality category.\n\n    Used to determine appropriate Expectation configurations based on data.\n"
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
    ABS_10_000 = AbsoluteCardinalityLimit("ABS_10_000", int(10000.0))
    ABS_100_000 = AbsoluteCardinalityLimit("ABS_100_000", int(100000.0))
    ABS_1_000_000 = AbsoluteCardinalityLimit("ABS_1_000_000", int(1000000.0))
    ABS_10_000_000 = AbsoluteCardinalityLimit("ABS_10_000_000", int(10000000.0))
    ABS_100_000_000 = AbsoluteCardinalityLimit("ABS_100_000_000", int(100000000.0))
    ABS_1_000_000_000 = AbsoluteCardinalityLimit("ABS_1_000_000_000", int(1000000000.0))
    REL_0 = RelativeCardinalityLimit("REL_0", 0.0)
    REL_001 = RelativeCardinalityLimit("REL_001", 1e-05)
    REL_01 = RelativeCardinalityLimit("REL_01", 0.0001)
    REL_0_1 = RelativeCardinalityLimit("REL_0_1", 0.001)
    REL_1 = RelativeCardinalityLimit("REL_1", 0.01)
    REL_10 = RelativeCardinalityLimit("REL_10", 0.1)
    REL_25 = RelativeCardinalityLimit("REL_25", 0.25)
    REL_50 = RelativeCardinalityLimit("REL_50", 0.5)
    REL_75 = RelativeCardinalityLimit("REL_75", 0.75)
    ONE_PCT = RelativeCardinalityLimit("ONE_PCT", 0.01)
    TEN_PCT = RelativeCardinalityLimit("TEN_PCT", 0.1)


class CardinalityChecker:
    "Handles cardinality checking given cardinality limit mode and measured value.\n\n    This class also validates cardinality limit settings and converts from\n    various types of settings. You can choose one of the attributes listed\n    below to create an instance.\n\n    Attributes:\n        limit_mode: CardinalityLimitMode or string name of the mode\n            defining the maximum allowable cardinality.\n        max_unique_values: number of max unique rows for a custom\n            cardinality limit.\n        max_proportion_unique: proportion of unique values for a\n            custom cardinality limit.\n"
    SUPPORTED_CARDINALITY_LIMIT_MODE_CLASSES: Tuple[
        Union[(AbsoluteCardinalityLimit, RelativeCardinalityLimit)]
    ] = (AbsoluteCardinalityLimit, RelativeCardinalityLimit)
    SUPPORTED_LIMIT_MODE_CLASS_NAMES: Tuple[str] = (
        mode.__name__ for mode in SUPPORTED_CARDINALITY_LIMIT_MODE_CLASSES
    )
    SUPPORTED_CARDINALITY_LIMIT_MODE_STRINGS: Tuple[str] = (
        mode.name for mode in CardinalityLimitMode
    )

    def __init__(
        self,
        limit_mode: Optional[Union[(CardinalityLimitMode, str)]] = None,
        max_unique_values: Optional[int] = None,
        max_proportion_unique: Optional[float] = None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._limit_mode = self._convert_to_cardinality_mode(
            limit_mode=limit_mode,
            max_unique_values=max_unique_values,
            max_proportion_unique=max_proportion_unique,
        )

    @property
    def limit_mode(self) -> Union[(AbsoluteCardinalityLimit, RelativeCardinalityLimit)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._limit_mode

    def cardinality_within_limit(self, metric_value: float) -> bool:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Determine if the cardinality is within configured limit.\n\n        The metric_value supplied should be either a proportion of unique values\n        or number of unique values based on the configured cardinality limit.\n\n        Args:\n            metric_value: int if number of unique values, float if proportion\n                of unique values.\n\n        Returns:\n            Boolean of whether the cardinality is within the configured limit\n        "
        self._validate_metric_value(metric_value=metric_value)
        if isinstance(self._limit_mode, AbsoluteCardinalityLimit):
            return metric_value <= self._limit_mode.max_unique_values
        elif isinstance(self._limit_mode, RelativeCardinalityLimit):
            return metric_value <= self._limit_mode.max_proportion_unique

    @staticmethod
    def _validate_metric_value(metric_value: float) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if not isinstance(metric_value, (int, float)):
            raise ProfilerConfigurationError(
                f"Value of measured cardinality must be of type int or float, you provided {type(metric_value)}"
            )
        if metric_value < 0.0:
            raise ProfilerConfigurationError(
                f"Value of cardinality (number of rows or percent unique) should be greater than 0.00, your value is {metric_value}"
            )

    @staticmethod
    def _convert_to_cardinality_mode(
        limit_mode: Optional[Union[(CardinalityLimitMode, str)]] = None,
        max_unique_values: Optional[int] = None,
        max_proportion_unique: Optional[float] = None,
    ) -> Union[(AbsoluteCardinalityLimit, RelativeCardinalityLimit)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        validate_input_parameters(
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


def validate_input_parameters(
    limit_mode: Optional[Union[(CardinalityLimitMode, str)]] = None,
    max_unique_values: Optional[int] = None,
    max_proportion_unique: Optional[int] = None,
) -> None:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    num_supplied_params: int = sum(
        [
            (0 if (param is None) else 1)
            for param in (limit_mode, max_unique_values, max_proportion_unique)
        ]
    )
    if num_supplied_params != 1:
        raise ProfilerConfigurationError(
            f"Please pass ONE of the following parameters: limit_mode, max_unique_values, max_proportion_unique, you passed {num_supplied_params} parameters."
        )
    if limit_mode is not None:
        if not (
            isinstance(limit_mode, CardinalityLimitMode) or isinstance(limit_mode, str)
        ):
            raise ProfilerConfigurationError(
                f"Please specify a supported cardinality limit type, supported classes are {','.join(CardinalityChecker.SUPPORTED_LIMIT_MODE_CLASS_NAMES)} and supported strings are {','.join(CardinalityChecker.SUPPORTED_CARDINALITY_LIMIT_MODE_STRINGS)}"
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
