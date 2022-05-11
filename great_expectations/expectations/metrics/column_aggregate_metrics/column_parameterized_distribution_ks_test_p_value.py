
import logging
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.metrics.column_aggregate_metric_provider import ColumnAggregateMetricProvider, column_aggregate_value
from great_expectations.expectations.metrics.util import _scipy_distribution_positional_args_from_dict, validate_distribution_parameters
logger = logging.getLogger(__name__)
try:
    from pyspark.sql.functions import stddev_samp
except ImportError as e:
    logger.debug(str(e))
    logger.debug('Unable to load spark context; install optional spark dependency for support.')
from scipy import stats

class ColumnParameterizedDistributionKSTestPValue(ColumnAggregateMetricProvider):
    'MetricProvider Class for Aggregate Standard Deviation metric'
    metric_name = 'column.parameterized_distribution_ks_test_p_value'
    value_keys = ('distribution', 'p_value', 'params')

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, distribution, p_value=0.05, params=None, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if ((p_value <= 0) or (p_value >= 1)):
            raise ValueError('p_value must be between 0 and 1 exclusive')
        try:
            validate_distribution_parameters(distribution=distribution, params=params)
        except ValueError as e:
            raise e
        if isinstance(params, dict):
            positional_parameters = _scipy_distribution_positional_args_from_dict(distribution, params)
        else:
            positional_parameters = params
        ks_result = stats.kstest(column, distribution, args=positional_parameters)
        return ks_result
