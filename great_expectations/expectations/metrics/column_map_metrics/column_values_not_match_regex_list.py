
import logging
import pandas as pd
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine, SqlAlchemyExecutionEngine
from great_expectations.expectations.metrics.import_manager import sa
from great_expectations.expectations.metrics.map_metric_provider import ColumnMapMetricProvider, column_condition_partial
from great_expectations.expectations.metrics.util import get_dialect_regex_expression
logger = logging.getLogger(__name__)

class ColumnValuesNotMatchRegexList(ColumnMapMetricProvider):
    condition_metric_name = 'column_values.not_match_regex_list'
    condition_value_keys = ('regex_list',)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, regex_list, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        regex_matches = []
        for regex in regex_list:
            regex_matches.append(column.astype(str).str.contains(regex))
        regex_match_df = pd.concat(regex_matches, axis=1, ignore_index=True)
        return (~ regex_match_df.any(axis='columns'))

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, regex_list, _dialect, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (len(regex_list) == 0):
            raise ValueError('At least one regex must be supplied in the regex_list.')
        regex_expression = get_dialect_regex_expression(column, regex_list[0], _dialect, positive=False)
        if (regex_expression is None):
            logger.warning(f'Regex is not supported for dialect {str(_dialect)}')
            raise NotImplementedError
        return sa.and_(*(get_dialect_regex_expression(column, regex, _dialect, positive=False) for regex in regex_list))

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, regex_list, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        compound = None
        for regex in regex_list:
            if (compound is None):
                compound = column.rlike(regex)
            else:
                compound = (compound & (~ column.rlike(regex)))
        return compound
