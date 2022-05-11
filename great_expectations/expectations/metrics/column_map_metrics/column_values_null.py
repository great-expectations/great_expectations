
from typing import Optional
from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine, SparkDFExecutionEngine, SqlAlchemyExecutionEngine
from great_expectations.expectations.metrics.map_metric_provider import ColumnMapMetricProvider, column_condition_partial
from great_expectations.expectations.metrics.metric_provider import MetricProvider, metric_value
from great_expectations.validator.metric_configuration import MetricConfiguration

class ColumnValuesNull(ColumnMapMetricProvider):
    condition_metric_name = 'column_values.null'
    filter_column_isnull = False

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
        return column.isnull()

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return (column == None)

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return column.isNull()

class ColumnValuesNullCount(MetricProvider):
    'A convenience class to provide an alias for easier access to the null count in a column.'
    metric_name = 'column_values.null.count'

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(*, metrics, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return metrics['column_values.nonnull.unexpected_count']

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(*, metrics, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return metrics['column_values.nonnull.unexpected_count']

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(*, metrics, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return metrics['column_values.nonnull.unexpected_count']

    @classmethod
    def _get_evaluation_dependencies(cls, metric: MetricConfiguration, configuration: Optional[ExpectationConfiguration]=None, execution_engine: Optional[ExecutionEngine]=None, runtime_configuration: Optional[dict]=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        dependencies: dict = super()._get_evaluation_dependencies(metric=metric, configuration=configuration, execution_engine=execution_engine, runtime_configuration=runtime_configuration)
        dependencies['column_values.nonnull.unexpected_count'] = MetricConfiguration(metric_name='column_values.nonnull.unexpected_count', metric_domain_kwargs=metric.metric_domain_kwargs)
        return dependencies
