
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine, SqlAlchemyExecutionEngine
from great_expectations.execution_engine.execution_engine import MetricPartialFunctionTypes
from great_expectations.expectations.metrics.import_manager import F, Window, sa
from great_expectations.expectations.metrics.map_metric_provider import ColumnMapMetricProvider, column_condition_partial
from great_expectations.util import generate_temporary_table_name

class ColumnValuesUnique(ColumnMapMetricProvider):
    condition_metric_name = 'column_values.unique'

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
        return (~ column.duplicated(keep=False))

    @column_condition_partial(engine=SqlAlchemyExecutionEngine, partial_fn_type=MetricPartialFunctionTypes.WINDOW_CONDITION_FN)
    def _sqlalchemy_window(cls, column, _table, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        dialect = kwargs.get('_dialect', None)
        sql_engine = kwargs.get('_sqlalchemy_engine', None)
        if (sql_engine and dialect and (dialect.dialect.name == 'mysql')):
            temp_table_name = generate_temporary_table_name()
            temp_table_stmt = 'CREATE TEMPORARY TABLE {new_temp_table} AS SELECT tmp.{column_name} FROM {source_table} tmp'.format(new_temp_table=temp_table_name, source_table=_table, column_name=column.name)
            sql_engine.execute(temp_table_stmt)
            dup_query = sa.select([column]).select_from(sa.text(temp_table_name)).group_by(column).having((sa.func.count(column) > 1))
        else:
            dup_query = sa.select([column]).select_from(_table).group_by(column).having((sa.func.count(column) > 1))
        return column.notin_(dup_query)

    @column_condition_partial(engine=SparkDFExecutionEngine, partial_fn_type=MetricPartialFunctionTypes.WINDOW_CONDITION_FN)
    def _spark(cls, column, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return (F.count(F.lit(1)).over(Window.partitionBy(column)) <= 1)
