import enum
import logging

from great_expectations.core._docs_decorators import public_api

logger = logging.getLogger(__name__)


@public_api
class MetricFunctionTypes(enum.Enum):
    """Enum type, whose members depict the nature of return value of a metric implementation function (defined for a specified "ExecutionEngine" subclass) that is the final result (rather than a Callable for deferred execution).

    The available types are:

    - `VALUE` -- metric implementation function returns a value computed over a dataset represented by "Domain" \
        (e.g., a statistic on column row values). \
        This is the only value in use (others below have never been used and are thus deprecated).
    - `MAP_VALUES` (never used and deprecated) -- metric implementation function returns a mapping between every
      "Domain" value and the result of a transformation of the corresponding "Domain" value.
    - `WINDOW_VALUES` (never used and deprecated) -- metric implementation function returns the result of applying a
      specified windowing operation over "Domain" values.
    - `AGGREGATE_VALUE` (never used and deprecated) -- metric implementation function returns the result of applying a
      specified aggregation operation to every "Domain" value.
    """

    VALUE = "value"


@public_api
class MetricPartialFunctionTypes(enum.Enum):
    """Enum type, whose members depict the nature of return value of a metric implementation function (defined for a specified "ExecutionEngine" subclass) that is a (partial) Callable to be executed once execution plan is complete.

    The available types are:

    - `MAP_FN` -- metric implementation function returns a mapping transformation for "Domain" values that evaluates to
      a quantity (rather than a condition statement, or a series, etc.).
    - `MAP_SERIES` -- metric implementation function returns a mapping transformation for "Domain" values that evaluates
      to a series-valued (e.g., Pandas.Series) result (rather than a Callable for deferred execution).
    - `WINDOW_FN` -- metric implementation function returns specified windowing operation over "Domain" values
      (currently applicable only to "SparkDFExecutionEngine").
    - `MAP_CONDITION_FN` -- metric implementation function returns a mapping transformation for "Domain" values that
      evaluates to a Callable (partial) computational component (as part of deferred execution plan) that expresses the
      specified condition (i.e., a logical operation).
    - `MAP_CONDITION_SERIES` -- metric implementation function returns a mapping transformation for "Domain" values that
      evaluates to a Callable (partial) computational component (as part of deferred execution plan) that expresses the
      specified condition (i.e., a logical operation) as a series-valued (e.g., Pandas.Series) result.
    - `WINDOW_CONDITION_FN` -- metric implementation function returns a windowing operation over "Domain" values that
      evaluates to a Callable (partial) computational component (as part of deferred execution plan) that expresses the
      specified condition (i.e., a logical operation).
    - `AGGREGATE_FN` -- metric implementation function returns an aggregation transformation over "Domain" values that
      evaluates to a Callable (partial) computational component (as part of deferred execution plan) that expresses the
      specified aggregated quantity.


    """

    MAP_FN = "map_fn"  # pertains to "PandasExecutionEngine"
    MAP_SERIES = "map_series"  # pertains to "PandasExecutionEngine"
    WINDOW_FN = "window_fn"  # currently pertains only to "SparkDFExecutionEngine"
    MAP_CONDITION_FN = "map_condition_fn"  # pertains to "SqlAlchemyExecutionEngine" and "SparkDFExecutionEngine"
    MAP_CONDITION_SERIES = "map_condition_series"  # pertains to "PandasExecutionEngine"
    WINDOW_CONDITION_FN = "window_condition_fn"  # pertains to "SqlAlchemyExecutionEngine" and "SparkDFExecutionEngine"
    AGGREGATE_FN = "aggregate_fn"  # pertains to "SqlAlchemyExecutionEngine" and "SparkDFExecutionEngine"

    @property
    @public_api
    def metric_suffix(self) -> str:
        """Examines the "name" property of this "Enum" and returns corresponding suffix for metric registration/usage.

        Returns:
            (str) designated metric name suffix
        """
        if self.name in [
            "MAP_FN",
            "MAP_SERIES",
            "WINDOW_FN",
        ]:
            return MetricPartialFunctionTypeSuffixes.MAP.value

        if self.name in [
            "MAP_CONDITION_FN",
            "MAP_CONDITION_SERIES",
            "WINDOW_CONDITION_FN",
        ]:
            return MetricPartialFunctionTypeSuffixes.CONDITION.value

        if self.name == "AGGREGATE_FN":
            return MetricPartialFunctionTypeSuffixes.AGGREGATE_FUNCTION.value

        return ""


@public_api
class MetricPartialFunctionTypeSuffixes(enum.Enum):
    """Enum type, whose members specify available suffixes for metrics representing partial functions."""

    MAP = "map"
    CONDITION = "condition"
    AGGREGATE_FUNCTION = "aggregate_fn"


@public_api
class SummarizationMetricNameSuffixes(enum.Enum):
    """Enum type, whose members specify suffixes for metrics used for summarizing Expectation validation results."""

    FILTERED_ROW_COUNT = "filtered_row_count"
    UNEXPECTED_COUNT = "unexpected_count"
    UNEXPECTED_INDEX_LIST = "unexpected_index_list"
    UNEXPECTED_INDEX_QUERY = "unexpected_index_query"
    UNEXPECTED_ROWS = "unexpected_rows"
    UNEXPECTED_VALUE_COUNTS = "unexpected_value_counts"
    UNEXPECTED_VALUES = "unexpected_values"
