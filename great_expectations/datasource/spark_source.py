import logging
import time
from six import string_types

from ..exceptions import BatchKwargsError

from .datasource import Datasource, ReaderMethods
from great_expectations.datasource.generator.filesystem_path_generator import SubdirReaderGenerator
from great_expectations.datasource.generator.databricks_generator import DatabricksTableGenerator
from great_expectations.datasource.generator.in_memory_generator import InMemoryGenerator

logger = logging.getLogger(__name__)

try:
    from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
    from pyspark.sql import SparkSession, DataFrame
except ImportError:
    # TODO: review logging more detail here
    logger.debug("Unable to load pyspark; install optional spark dependency for support.")


class SparkDFDatasource(Datasource):
    """The SparkDFDatasource produces SparkDFDatasets and supports generators capable of interacting with local
    filesystem (the default subdir_reader generator) and databricks notebooks.
    """

    def __init__(self, name="default", data_context=None, data_asset_type="SparkDFDataset", generators=None, **kwargs):
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
        super(SparkDFDatasource, self).__init__(name, type_="spark",
                                                data_context=data_context,
                                                data_asset_type=data_asset_type,
                                                generators=generators)
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
        elif type_ == "memory":
            return InMemoryGenerator
        else:
            raise ValueError("Unrecognized BatchGenerator type %s" % type_)

    def _get_data_asset(self, batch_kwargs, expectation_suite, caching=True, **kwargs):
        """class-private implementation of get_data_asset"""
        if self.spark is None:
            logger.error("No spark session available")
            return None

        batch_kwargs.update(kwargs)
        reader_options = batch_kwargs.copy()
        if "path" in batch_kwargs:
            path = reader_options.pop("path")  # We remove this so it is not used as a reader option
            reader_options.pop("timestamp", "")    # ditto timestamp (but missing ok)
            reader_method = reader_options.pop("reader_method", None)
            if reader_method is None:
                reader_method = self._guess_reader_method_from_path(path)
                if reader_method is None:
                    raise BatchKwargsError("Unable to determine reader for path: %s" % path, batch_kwargs)
            else:
                try:
                    reader_method = ReaderMethods[reader_method]
                except KeyError:
                    raise BatchKwargsError("Unknown reader method: %s" % reader_method, batch_kwargs)

            reader = self.spark.read

            for option in reader_options.items():
                reader = reader.option(*option)

            if reader_method == ReaderMethods.CSV:
                df = reader.csv(path)
            elif reader_method == ReaderMethods.parquet:
                df = reader.parquet(path)
            else:
                raise BatchKwargsError("Unsupported reader: %s" % reader_method.name, batch_kwargs)
            
        elif "query" in batch_kwargs:
            df = self.spark.sql(batch_kwargs.query)

        elif "df" in batch_kwargs and isinstance(batch_kwargs["df"], (DataFrame, SparkDFDataset)):
            df = batch_kwargs.pop("df")  # We don't want to store the actual DataFrame in kwargs
            if isinstance(df, SparkDFDataset):
                # Grab just the spark_df reference, since we want to override everything else
                df = df.spark_df
            batch_kwargs["SparkDFRef"] = True
        else:
            raise BatchKwargsError("Unrecognized batch_kwargs for spark_source", batch_kwargs)

        return SparkDFDataset(df,
                              expectation_suite=expectation_suite,
                              data_context=self._data_context,
                              batch_kwargs=batch_kwargs,
                              caching=caching)

    def build_batch_kwargs(self, *args, **kwargs):
        if len(args) > 0:
            if isinstance(args[0], (DataFrame, SparkDFDataset)):
                kwargs.update({
                    "df": args[0],
                    "timestamp": time.time()
                })
            elif isinstance(args[0], string_types):
                kwargs.update({
                    "path": args[0],
                    "timestamp": time.time()
                })
        else:
            kwargs.update({
                "timestamp": time.time()
            })
        return kwargs
