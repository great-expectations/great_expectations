
from typing import Any, Dict
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine, SqlAlchemyExecutionEngine
from great_expectations.execution_engine.execution_engine import MetricDomainTypes, MetricPartialFunctionTypes
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.metrics.metric_provider import metric_partial, metric_value
from great_expectations.expectations.metrics.table_metric_provider import TableMetricProvider

class TableRowCount(TableMetricProvider):
    metric_name = 'table.row_count'

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(cls, execution_engine: 'PandasExecutionEngine', metric_domain_kwargs: Dict, metric_value_kwargs: Dict, metrics: Dict[(str, Any)], runtime_configuration: Dict):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        (df, _, _) = execution_engine.get_compute_domain(domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE)
        return df.shape[0]

    @metric_partial(engine=SqlAlchemyExecutionEngine, partial_fn_type=MetricPartialFunctionTypes.AGGREGATE_FN, domain_type=MetricDomainTypes.TABLE)
    def _sqlalchemy(cls, execution_engine: 'SqlAlchemyExecutionEngine', metric_domain_kwargs: Dict, metric_value_kwargs: Dict, metrics: Dict[(str, Any)], runtime_configuration: Dict):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return (sa.func.count(), metric_domain_kwargs, {})

    @metric_partial(engine=SparkDFExecutionEngine, partial_fn_type=MetricPartialFunctionTypes.AGGREGATE_FN, domain_type=MetricDomainTypes.TABLE)
    def _spark(cls, execution_engine: 'SqlAlchemyExecutionEngine', metric_domain_kwargs: Dict, metric_value_kwargs: Dict, metrics: Dict[(str, Any)], runtime_configuration: Dict):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return (F.count(F.lit(1)), metric_domain_kwargs, {})
