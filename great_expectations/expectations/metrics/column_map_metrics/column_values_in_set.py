
import warnings
import numpy as np
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine, SqlAlchemyExecutionEngine
from great_expectations.expectations.metrics.import_manager import F
from great_expectations.expectations.metrics.map_metric_provider import ColumnMapMetricProvider, column_condition_partial

class ColumnValuesInSet(ColumnMapMetricProvider):
    condition_metric_name = 'column_values.in_set'
    condition_value_keys = ('value_set', 'parse_strings_as_datetimes')

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, value_set, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        parse_strings_as_datetimes: bool = (kwargs.get('parse_strings_as_datetimes') or False)
        if parse_strings_as_datetimes:
            warnings.warn('The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in v0.16. As part of the V3 API transition, we\'ve moved away from input transformation. For more information, please see: https://greatexpectations.io/blog/why_we_dont_do_transformations_for_expectations/\n', DeprecationWarning)
        if (value_set is None):
            return np.ones(len(column), dtype=np.bool_)
        return column.isin(value_set)

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, value_set, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        parse_strings_as_datetimes: bool = (kwargs.get('parse_strings_as_datetimes') or False)
        if parse_strings_as_datetimes:
            warnings.warn('The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in v0.16. As part of the V3 API transition, we\'ve moved away from input transformation. For more information, please see: https://greatexpectations.io/blog/why_we_dont_do_transformations_for_expectations/\n', DeprecationWarning)
        if (value_set is None):
            return True
        if (len(value_set) == 0):
            return False
        return column.in_(value_set)

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, value_set, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        parse_strings_as_datetimes: bool = (kwargs.get('parse_strings_as_datetimes') or False)
        if parse_strings_as_datetimes:
            warnings.warn('The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in v0.16. As part of the V3 API transition, we\'ve moved away from input transformation. For more information, please see: https://greatexpectations.io/blog/why_we_dont_do_transformations_for_expectations/\n', DeprecationWarning)
        if (value_set is None):
            return F.lit(True)
        return column.isin(value_set)
