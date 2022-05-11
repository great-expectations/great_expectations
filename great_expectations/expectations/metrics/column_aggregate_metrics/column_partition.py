
from typing import Any, Dict, Optional
import numpy as np
from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine, SparkDFExecutionEngine, SqlAlchemyExecutionEngine
from great_expectations.expectations.metrics.column_aggregate_metric_provider import ColumnAggregateMetricProvider
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.validator.metric_configuration import MetricConfiguration

class ColumnPartition(ColumnAggregateMetricProvider):
    metric_name = 'column.partition'
    value_keys = ('bins', 'n_bins', 'allow_relative_error')
    default_kwarg_values = {'bins': 'uniform', 'n_bins': 10, 'allow_relative_error': False}

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
        bins = metric_value_kwargs.get('bins', cls.default_kwarg_values['bins'])
        n_bins = metric_value_kwargs.get('n_bins', cls.default_kwarg_values['n_bins'])
        return _get_column_partition_using_metrics(bins, n_bins, metrics)

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, execution_engine: PandasExecutionEngine, metric_domain_kwargs: Dict, metric_value_kwargs: Dict, metrics: Dict[(str, Any)], runtime_configuration: Dict):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        bins = metric_value_kwargs.get('bins', cls.default_kwarg_values['bins'])
        n_bins = metric_value_kwargs.get('n_bins', cls.default_kwarg_values['n_bins'])
        return _get_column_partition_using_metrics(bins, n_bins, metrics)

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(cls, execution_engine: PandasExecutionEngine, metric_domain_kwargs: Dict, metric_value_kwargs: Dict, metrics: Dict[(str, Any)], runtime_configuration: Dict):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        bins = metric_value_kwargs.get('bins', cls.default_kwarg_values['bins'])
        n_bins = metric_value_kwargs.get('n_bins', cls.default_kwarg_values['n_bins'])
        return _get_column_partition_using_metrics(bins, n_bins, metrics)

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
        bins = metric.metric_value_kwargs.get('bins', cls.default_kwarg_values['bins'])
        n_bins = metric.metric_value_kwargs.get('n_bins', cls.default_kwarg_values['n_bins'])
        allow_relative_error = metric.metric_value_kwargs['allow_relative_error']
        dependencies: dict = super()._get_evaluation_dependencies(metric=metric, configuration=configuration, execution_engine=execution_engine, runtime_configuration=runtime_configuration)
        if (bins == 'uniform'):
            dependencies['column.min'] = MetricConfiguration(metric_name='column.min', metric_domain_kwargs=metric.metric_domain_kwargs)
            dependencies['column.max'] = MetricConfiguration(metric_name='column.max', metric_domain_kwargs=metric.metric_domain_kwargs)
        elif (bins in ['ntile', 'quantile', 'percentile']):
            dependencies['column.quantile_values'] = MetricConfiguration(metric_name='column.quantile_values', metric_domain_kwargs=metric.metric_domain_kwargs, metric_value_kwargs={'quantiles': np.linspace(start=0, stop=1, num=(n_bins + 1)).tolist(), 'allow_relative_error': allow_relative_error})
        elif (bins == 'auto'):
            dependencies['column_values.nonnull.count'] = MetricConfiguration(metric_name='column_values.nonnull.count', metric_domain_kwargs=metric.metric_domain_kwargs)
            dependencies['column.quantile_values'] = MetricConfiguration(metric_name='column.quantile_values', metric_domain_kwargs=metric.metric_domain_kwargs, metric_value_kwargs={'quantiles': (0.0, 0.25, 0.75, 1.0), 'allow_relative_error': allow_relative_error})
        else:
            raise ValueError('Invalid parameter for bins argument')
        return dependencies

def _get_column_partition_using_metrics(bins, n_bins, _metrics):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if (bins == 'uniform'):
        min_ = _metrics['column.min']
        max_ = _metrics['column.max']
        bins = np.linspace(start=float(min_), stop=float(max_), num=(n_bins + 1)).tolist()
    elif (bins in ['ntile', 'quantile', 'percentile']):
        bins = _metrics['column.quantile_values']
    elif (bins == 'auto'):
        nonnull_count = _metrics['column_values.nonnull.count']
        sturges = np.log2((nonnull_count + 1))
        (min_, _25, _75, max_) = _metrics['column.quantile_values']
        iqr = (_75 - _25)
        if (iqr < 1e-10):
            n_bins = sturges
        else:
            fd = ((2 * float(iqr)) / (nonnull_count ** (1 / 3)))
            n_bins = max(int(np.ceil(sturges)), int(np.ceil((float((max_ - min_)) / fd))))
        bins = np.linspace(start=float(min_), stop=float(max_), num=(n_bins + 1)).tolist()
    else:
        raise ValueError('Invalid parameter for bins argument')
    return bins
