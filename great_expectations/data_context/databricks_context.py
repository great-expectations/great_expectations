import logging

from .base import DataContext
from ..dataset.sparkdf_dataset import SparkDFDataset

logger = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession
except ImportError:
    logger.error("Unable to load spark context; install optional spark dependency for support.")
    raise

class DatabricksTableContext(DataContext):
    """Meant to be used in a Databricks notebook

    Unsure of if we want to keep this and other new, more esoteric data contexts in the
    main library or provide an easy way for people to write their own.
    """

    def __init__(self, options, *args, **kwargs):
        super(DatabricksTableContext, self).__init__(options, *args, **kwargs)
        # this should grab the already instantiated SparkSession available on Databricks notebooks
        self.spark = SparkSession.builder.getOrCreate()

    def connect(self, options):
        self.database = options

    def list_datasets(self):
        tables = self.spark.sql('show tables in {}'.format(self.database))
        return [row.tableName for row in tables.collect()]

    def get_dataset(self, dataset_name, caching=False, **kwargs):
        query = 'select * from {}.{}'.format(self.database, dataset_name)
        if kwargs.get('partition'):
            if not kwargs.get('date_field'):
                raise Exception('Must specify date_field when using partition.')
            query += ' where {} = "{}"'.format(kwargs.get('date_field'), kwargs.get('partition'))
        df = self.spark.sql(query)
        return SparkDFDataset(df, caching=caching)
