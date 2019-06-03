import os
import logging

from .datasource import Datasource
from .filesystem_path_generator import FilesystemPathGenerator
from .databricks_generator import DatabricksTableGenerator

logger = logging.getLogger(__name__)

try:
    from ...dataset.sparkdf_dataset import SparkDFDataset
    from pyspark.sql import SparkSession
except ImportError:
    logger.error("Unable to load pyspark; install optional spark dependency for support.")
    raise

class SparkDFDatasource(Datasource):
    """For now, functions like PandasCSVDataContext
    """

    def __init__(self, name="default", data_context=None, generators=None, **kwargs):
        if generators is None:
            # Provide a gentle way to build a datasource with a sane default, including ability to specify the base_directory
            base_directory = kwargs.pop("base_directory", "/data")
            generators = {
                "default": {"type": "filesystem", "base_directory": base_directory}
        }
        super(SparkDFDatasource, self).__init__(name, type_="spark", data_context=data_context, generators=generators)
        try:
            self.spark = SparkSession.builder.getOrCreate()
        except Exception:
            logger.error("Unable to load spark context; install optional spark dependency for support.")
            self.spark = None

        self._build_generators()

    def _get_generator_class(self, type_):
        if type_ == "filesystem":
            return FilesystemPathGenerator
        elif type_ == "databricks":
            return DatabricksTableGenerator
        else:
            raise ValueError("Unrecognized BatchGenerator type %s" % type_)


    def _get_data_asset(self, data_asset_name, batch_kwargs, expectations_config, caching=False, **kwargs):
        if self.spark is None:
            logger.error("No spark session available")
            return None

        if "path" in batch_kwargs:
            reader = self.spark.read
            for option in kwargs.items():
                reader = reader.option(*option)
            df = reader.csv(os.path.join(batch_kwargs["path"]))

        elif "query" in batch_kwargs:
            df = self.spark.sql(batch_kwargs.query)

        return SparkDFDataset(df,
            expectations_config=expectations_config,
            data_context=self._data_context,
            data_asset_name=data_asset_name,
            batch_kwargs=batch_kwargs,
            caching=caching)
