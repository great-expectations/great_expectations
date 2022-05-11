
from typing import Any, Dict, Optional
import numpy as np
from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine, SparkDFExecutionEngine, SqlAlchemyExecutionEngine
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.expectations.metrics.column_aggregate_metric_provider import ColumnAggregateMetricProvider, column_aggregate_value
from great_expectations.expectations.metrics.import_manager import sa
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.validator.metric_configuration import MetricConfiguration

class ColumnMedian(ColumnAggregateMetricProvider):
    'MetricProvider Class for Aggregate Mean MetricProvider'
    metric_name = 'column.median'

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Pandas Median Implementation'
        return column.median()

    @metric_value(engine=SqlAlchemyExecutionEngine, metric_fn_type='value')
    def _sqlalchemy(cls, execution_engine: SqlAlchemyExecutionEngine, metric_domain_kwargs: Dict, metric_value_kwargs: Dict, metrics: Dict[(str, Any)], runtime_configuration: Dict):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        (selectable, compute_domain_kwargs, accessor_domain_kwargs) = execution_engine.get_compute_domain(metric_domain_kwargs, MetricDomainTypes.COLUMN)
        column_name = accessor_domain_kwargs['column']
        column = sa.column(column_name)
        sqlalchemy_engine = execution_engine.engine
        'SqlAlchemy Median Implementation'
        nonnull_count = metrics.get('column_values.nonnull.count')
        if (not nonnull_count):
            return None
        element_values = sqlalchemy_engine.execute(sa.select([column]).order_by(column).where((column != None)).offset(max(((nonnull_count // 2) - 1), 0)).limit(2).select_from(selectable))
        column_values = list(element_values.fetchall())
        if (len(column_values) == 0):
            column_median = None
        elif ((nonnull_count % 2) == 0):
            column_median = (float((column_values[0][0] + column_values[1][0])) / 2.0)
        else:
            column_median = column_values[1][0]
        return column_median

    @metric_value(engine=SparkDFExecutionEngine, metric_fn_type='value')
    def _spark(cls, execution_engine: SqlAlchemyExecutionEngine, metric_domain_kwargs: Dict, metric_value_kwargs: Dict, metrics: Dict[(str, Any)], runtime_configuration: Dict):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        (df, compute_domain_kwargs, accessor_domain_kwargs) = execution_engine.get_compute_domain(metric_domain_kwargs, MetricDomainTypes.COLUMN)
        column = accessor_domain_kwargs['column']
        'Spark Median Implementation'
        table_row_count = metrics.get('table.row_count')
        result = df.approxQuantile(column, [0.5, (0.5 + (1 / (2 + (2 * table_row_count))))], 0)
        return np.mean(result)

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
        'This should return a dictionary:\n        {\n          "dependency_name": MetricConfiguration,\n          ...\n        }\n        '
        dependencies: dict = super()._get_evaluation_dependencies(metric=metric, configuration=configuration, execution_engine=execution_engine, runtime_configuration=runtime_configuration)
        if isinstance(execution_engine, SqlAlchemyExecutionEngine):
            dependencies['column_values.nonnull.count'] = MetricConfiguration(metric_name='column_values.nonnull.count', metric_domain_kwargs=metric.metric_domain_kwargs)
        return dependencies
