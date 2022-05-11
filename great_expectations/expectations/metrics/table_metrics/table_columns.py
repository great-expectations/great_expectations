
from typing import Any, Dict, Optional
from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine, SparkDFExecutionEngine, SqlAlchemyExecutionEngine
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.table_metric_provider import TableMetricProvider
from great_expectations.validator.metric_configuration import MetricConfiguration
try:
    import pyspark.sql.types as sparktypes
except ImportError:
    sparktypes = None

class TableColumns(TableMetricProvider):
    metric_name = 'table.columns'

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(cls, execution_engine: PandasExecutionEngine, metric_domain_kwargs: Dict, metric_value_kwargs: Dict, metrics: Dict[(str, Any)], runtime_configuration: Dict):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        column_metadata = metrics['table.column_types']
        return [col['name'] for col in column_metadata]

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, execution_engine: SqlAlchemyExecutionEngine, metric_domain_kwargs: Dict, metric_value_kwargs: Dict, metrics: Dict[(str, Any)], runtime_configuration: Dict):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        column_metadata = metrics['table.column_types']
        return [col['name'] for col in column_metadata]

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(cls, execution_engine: SparkDFExecutionEngine, metric_domain_kwargs: Dict, metric_value_kwargs: Dict, metrics: Dict[(str, Any)], runtime_configuration: Dict):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        column_metadata = metrics['table.column_types']
        return [col['name'] for col in column_metadata]

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
        table_domain_kwargs: dict = {k: v for (k, v) in metric.metric_domain_kwargs.items() if (k != 'column')}
        dependencies['table.column_types'] = MetricConfiguration(metric_name='table.column_types', metric_domain_kwargs=table_domain_kwargs, metric_value_kwargs={'include_nested': True}, metric_dependencies=None)
        return dependencies
