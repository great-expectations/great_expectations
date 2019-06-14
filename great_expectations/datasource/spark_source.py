import os
import logging

from .datasource import Datasource
from .filesystem_path_generator import SubdirReaderGenerator
from .databricks_generator import DatabricksTableGenerator

logger = logging.getLogger(__name__)

try:
    from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
    from pyspark.sql import SparkSession
except ImportError:
    # TODO: review logging more detail here
    logger.debug("Unable to load pyspark; install optional spark dependency for support.")


class SparkDFDatasource(Datasource):
    """The SparkDFDatasource produces spark dataframes and supports generators capable of interacting with local
    filesystem (the default subdir_reader generator) and databricks notebooks.
    """

    def __init__(self, name="default", data_context=None, generators=None, **kwargs):
        if generators is None:
            # Provide a gentle way to build a datasource with a sane default,
            # including ability to specify the base_directory
            base_directory = kwargs.pop("base_directory", "/data")
            reader_options = kwargs.pop("reader_options", {})
            generators = {
                "default": {
                    "type": "subdir_reader",
                    "base_directory": base_directory,
                    "reader_options": reader_options
                }
        }
        super(SparkDFDatasource, self).__init__(name, type_="spark", data_context=data_context, generators=generators)
        try:
            self.spark = SparkSession.builder.getOrCreate()
        except Exception:
            logger.error("Unable to load spark context; install optional spark dependency for support.")
            self.spark = None

        self._build_generators()

    def _get_generator_class(self, type_):
        if type_ == "subdir_reader":
            return SubdirReaderGenerator
        elif type_ == "databricks":
            return DatabricksTableGenerator
        else:
            raise ValueError("Unrecognized BatchGenerator type %s" % type_)

    def _get_data_asset(self, data_asset_name, batch_kwargs, expectation_suite, caching=False, **kwargs):
        if self.spark is None:
            logger.error("No spark session available")
            return None

        if "path" in batch_kwargs:
            path = batch_kwargs.pop("path")  # We remove this so it is not used as a reader option
            reader = self.spark.read
            batch_kwargs.update(kwargs)

            for option in batch_kwargs.items():
                reader = reader.option(*option)
            df = reader.csv(os.path.join(path))

        elif "query" in batch_kwargs:
            df = self.spark.sql(batch_kwargs.query)

        return SparkDFDataset(df,
                              expectation_suite=expectation_suite,
                              data_context=self._data_context,
                              data_asset_name=data_asset_name,
                              batch_kwargs=batch_kwargs,
                              caching=caching)
