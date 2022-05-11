
import warnings
from dateutil.parser import parse
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine, SqlAlchemyExecutionEngine
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.metrics.map_metric_provider import ColumnPairMapMetricProvider, column_pair_condition_partial

class ColumnPairValuesAGreaterThanB(ColumnPairMapMetricProvider):
    condition_metric_name = 'column_pair_values.a_greater_than_b'
    condition_domain_keys = ('batch_id', 'table', 'column_A', 'column_B', 'row_condition', 'condition_parser', 'ignore_row_if')
    condition_value_keys = ('or_equal', 'parse_strings_as_datetimes', 'allow_cross_type_comparisons')

    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        allow_cross_type_comparisons: bool = (kwargs.get('allow_cross_type_comparisons') or False)
        if allow_cross_type_comparisons:
            raise NotImplementedError
        parse_strings_as_datetimes: bool = (kwargs.get('parse_strings_as_datetimes') or False)
        if parse_strings_as_datetimes:
            warnings.warn('The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in v0.16. As part of the V3 API transition, we\'ve moved away from input transformation. For more information, please see: https://greatexpectations.io/blog/why_we_dont_do_transformations_for_expectations/\n', DeprecationWarning)
            try:
                temp_column_A = column_A.map(parse)
            except TypeError:
                temp_column_A = column_A
            try:
                temp_column_B = column_B.map(parse)
            except TypeError:
                temp_column_B = column_B
        else:
            temp_column_A = column_A
            temp_column_B = column_B
        or_equal: bool = (kwargs.get('or_equal') or False)
        if or_equal:
            return (temp_column_A >= temp_column_B)
        else:
            return (temp_column_A > temp_column_B)

    @column_pair_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_A, column_B, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        allow_cross_type_comparisons: bool = (kwargs.get('allow_cross_type_comparisons') or False)
        if allow_cross_type_comparisons:
            raise NotImplementedError
        parse_strings_as_datetimes: bool = (kwargs.get('parse_strings_as_datetimes') or False)
        if parse_strings_as_datetimes:
            raise NotImplementedError
        or_equal: bool = (kwargs.get('or_equal') or False)
        if or_equal:
            return sa.or_((column_A >= column_B), sa.and_((column_A == None), (column_B == None)))
        else:
            return (column_A > column_B)

    @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_A, column_B, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        allow_cross_type_comparisons: bool = (kwargs.get('allow_cross_type_comparisons') or False)
        if allow_cross_type_comparisons:
            raise NotImplementedError
        parse_strings_as_datetimes: bool = (kwargs.get('parse_strings_as_datetimes') or False)
        if parse_strings_as_datetimes:
            warnings.warn('The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in v0.16. As part of the V3 API transition, we\'ve moved away from input transformation. For more information, please see: https://greatexpectations.io/blog/why_we_dont_do_transformations_for_expectations/\n', DeprecationWarning)
            temp_column_A = F.to_date(column_A)
            temp_column_B = F.to_date(column_B)
        else:
            temp_column_A = column_A
            temp_column_B = column_B
        or_equal: bool = (kwargs.get('or_equal') or False)
        if or_equal:
            return ((temp_column_A >= temp_column_B) | temp_column_A.eqNullSafe(temp_column_B))
        else:
            return (temp_column_A > temp_column_B)
