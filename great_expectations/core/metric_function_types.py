import enum
import logging

logger = logging.getLogger(__name__)


class MetricFunctionTypes(enum.Enum):
    VALUE = "value"
    MAP_VALUES = "value"  # "map_values"
    WINDOW_VALUES = "value"  # "window_values"
    AGGREGATE_VALUE = "value"  # "aggregate_value"


class MetricPartialFunctionTypeSuffixes(enum.Enum):
    MAP = "map"
    CONDITION = "condition"
    AGGREGATE_FUNCTION = "aggregate_fn"


class MetricPartialFunctionTypes(enum.Enum):
    MAP_FN = "map_fn"
    MAP_SERIES = "map_series"
    WINDOW_FN = "window_fn"
    MAP_CONDITION_FN = "map_condition_fn"
    MAP_CONDITION_SERIES = "map_condition_series"
    WINDOW_CONDITION_FN = "window_condition_fn"
    AGGREGATE_FN = "aggregate_fn"

    @property
    def metric_suffix(self) -> str:
        if self.name in ["MAP_FN", "MAP_SERIES", "WINDOW_FN"]:
            return MetricPartialFunctionTypeSuffixes.MAP.value

        if self.name in [
            "MAP_CONDITION_FN",
            "MAP_CONDITION_SERIES",
            "WINDOW_CONDITION_FN",
        ]:
            return MetricPartialFunctionTypeSuffixes.CONDITION.value

        if self.name in ["AGGREGATE_FN"]:
            return MetricPartialFunctionTypeSuffixes.AGGREGATE_FUNCTION.value

        return ""
