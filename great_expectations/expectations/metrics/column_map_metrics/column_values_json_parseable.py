
import json
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.metrics.import_manager import F, sparktypes
from great_expectations.expectations.metrics.map_metric_provider import ColumnMapMetricProvider, column_condition_partial

class ColumnValuesJsonParseable(ColumnMapMetricProvider):
    condition_metric_name = 'column_values.json_parseable'

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')

        def is_json(val):
            import inspect
            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                    continue
                print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
            try:
                json.loads(val)
                return True
            except:
                return False
        return column.map(is_json)

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, json_schema, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')

        def is_json(val):
            import inspect
            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                    continue
                print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
            try:
                json.loads(val)
                return True
            except:
                return False
        is_json_udf = F.udf(is_json, sparktypes.BooleanType())
        return is_json_udf(column)
