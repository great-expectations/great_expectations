from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.metrics.map_metric import (
    ColumnMapMetricProvider,
    column_condition_partial,
)

from polyglot.detect import Detector

def detect_language(val):
    return Detector(val, quiet=True).language.code


class ColumnValuesDetectLanguage(ColumnMapMetricProvider):
    condition_metric_name = "column_values.value_language.equals"
    function_metric_name = "column_values.value_language"
    
    condition_value_keys = ("language",)

    @column_function_partial(engine=PandasExecutionEngine)
    def _pandas_function(cls, column, **kwargs):
        return column.astype(str).map(detect_language)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, language, _metrics, **kwargs):
        column_languages, _, _ = _metrics.get("column_values.value_language.map")
        return column_languages == language

    
    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, language, **kwargs):
        def is_language(val):
            return val == language

        language_udf = F.udf(is_language, sparktypes.BooleanType())

        return language_udf(column)