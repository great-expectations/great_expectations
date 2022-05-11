
import logging
from great_expectations.core.util import get_or_create_spark_application
from great_expectations.datasource.batch_kwargs_generator.batch_kwargs_generator import BatchKwargsGenerator
logger = logging.getLogger(__name__)
try:
    from pyspark.sql import SparkSession
except ImportError:
    logger.debug('Unable to load spark context; install optional spark dependency for support.')

class DatabricksTableBatchKwargsGenerator(BatchKwargsGenerator):
    'Meant to be used in a Databricks notebook'

    def __init__(self, name='default', datasource=None, database='default') -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(name, datasource=datasource)
        self.database = database
        try:
            self.spark = get_or_create_spark_application()
        except Exception:
            logger.error('Unable to load spark context; install optional spark dependency for support.')
            self.spark = None

    def get_available_data_asset_names(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (self.spark is None):
            logger.warning('No sparkSession available to query for tables.')
            return {'names': []}
        tables = self.spark.sql(f'show tables in {self.database}')
        return {'names': [(row.tableName, 'table') for row in tables.collect()]}

    def _get_iterator(self, data_asset_name, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        query = f'select * from {self.database}.{data_asset_name}'
        if kwargs.get('partition'):
            if (not kwargs.get('date_field')):
                raise Exception('Must specify date_field when using partition.')
            query += ' where {} = "{}"'.format(kwargs.get('date_field'), kwargs.get('partition'))
        return iter([{'query': query}])
