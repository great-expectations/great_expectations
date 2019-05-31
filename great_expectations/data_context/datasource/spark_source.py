import os
import logging

from .datasource import Datasource

logger = logging.getLogger(__name__)

try:
    from ...dataset.sparkdf_dataset import SparkDFDataset
    from pyspark.sql import SparkSession
except ImportError:
    logger.error("Unable to load spark context; install optional spark dependency for support.")
    raise

class SparkDFDatasource(Datasource):
    """For now, functions like PandasCSVDataContext
    """

    def __init__(self, name, type_, data_context=None, base_directory="/data", *args, **kwargs):
        super(SparkDFDatasource, self).__init__(name, type_, data_context, *args, **kwargs)
        self.spark = SparkSession.builder.getOrCreate()
        self.connect(base_directory)

    def connect(self, base_directory):
        self.directory = base_directory

    def list_datasets(self):
        return os.listdir(self.directory)

    def get_dataset(self, dataset_name, caching=False, **kwargs):
        reader = self.spark.read
        for option in kwargs.items():
            reader = reader.option(*option)
        df = reader.csv(os.path.join(self.directory, dataset_name))
        return SparkDFDataset(df, caching=caching)
