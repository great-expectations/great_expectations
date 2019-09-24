import time
import hashlib
import logging

# from builtins import str
from six import string_types

try:
    from io import StringIO
except ImportError:
    from StringIO import StringIO

from six import PY2

import pandas as pd

from .datasource import Datasource, ReaderMethods
from great_expectations.datasource.generator.in_memory_generator import InMemoryGenerator
from great_expectations.datasource.generator.subdir_reader_generator import SubdirReaderGenerator
from great_expectations.datasource.generator.glob_reader_generator import GlobReaderGenerator
from great_expectations.datasource.generator.s3_generator import S3Generator
from great_expectations.datasource.types import (
    PandasDatasourceMemoryBatchKwargs,
    PathBatchKwargs
)
from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.types import ClassConfig
from great_expectations.exceptions import BatchKwargsError
from great_expectations.data_context.types.resource_identifiers import DataAssetIdentifier
from great_expectations.data_context.types.base import NormalizedDataAssetName

logger = logging.getLogger(__name__)

HASH_THRESHOLD = 1e9

from .util import S3Url

logger = logging.getLogger(__name__)


class PandasDatasource(Datasource):
    """The PandasDatasource produces PandasDataset objects and supports generators capable of 
    interacting with the local filesystem (the default subdir_reader generator), and from
    existing in-memory dataframes.
    """

    @classmethod
    def build_configuration(cls, data_asset_type=None, generators=None, **kwargs):
        """
        Build a full configuration object for a datasource, potentially including generators with defaults.

        Args:
            data_asset_type: A ClassConfig dictionary
            generators: Generator configuration dictionary
            **kwargs: Additional kwargs to be part of the datasource constructor's initialization

        Returns:
            A complete datasource configuration.

        """
        if generators is None:
            # Provide a gentle way to build a datasource with a sane default,
            # including ability to specify the base_directory and reader_options
            base_directory = kwargs.pop("base_directory", "/data")
            # By default, use CSV sniffer to infer separator, which requires the python engine
            reader_options = kwargs.pop("reader_options", {
                "sep": None,
                "engine": "python"
            })
            generators = {
                "default": {
                    "class_name": "SubdirReaderGenerator",
                    "base_directory": base_directory,
                    "reader_options": reader_options
                }
            }
        if data_asset_type is None:
            data_asset_type = ClassConfig(
                class_name="PandasDataset")
        else:
            try:
                data_asset_type = ClassConfig(**data_asset_type)
            except TypeError:
                # In this case, we allow the passed config, for now, in case they're using a legacy string-only config
                pass

        configuration = kwargs
        configuration.update({
            "data_asset_type": data_asset_type,
            "generators": generators,
        })
        return configuration

    def __init__(self, name="pandas", data_context=None, data_asset_type=None, generators=None, **kwargs):
        configuration_with_defaults = PandasDatasource.build_configuration(data_asset_type, generators, **kwargs)
        data_asset_type = configuration_with_defaults.pop("data_asset_type")
        generators = configuration_with_defaults.pop("generators")
        super(PandasDatasource, self).__init__(name,
                                               data_context=data_context,
                                               data_asset_type=data_asset_type,
                                               generators=generators,
                                               **configuration_with_defaults)
        self._build_generators()

    def _get_generator_class_from_type(self, type_):
        if type_ == "subdir_reader":
            return SubdirReaderGenerator
        elif type_ == "glob_reader":
            return GlobReaderGenerator
        elif type_ == "memory":
            return InMemoryGenerator
        elif type_ == "s3":
            return S3Generator
        else:
            raise ValueError("Unrecognized BatchGenerator type %s" % type_)

    def _get_data_asset(self, batch_kwargs, expectation_suite, **kwargs):
        batch_kwargs.update(kwargs)
        # pandas cannot take unicode as a delimiter, which can happen in py2. Handle this case explicitly.
        # We handle it here so that the updated value will be in the batch_kwargs for transparency to the user.
        if PY2 and "sep" in batch_kwargs and batch_kwargs["sep"] is not None:
            batch_kwargs["sep"] = str(batch_kwargs["sep"])
        # We will use and manipulate reader_options along the way
        reader_options = batch_kwargs.copy()

        # We need to build a batch_id to be used in the dataframe
        batch_id = ({
            "timestamp": time.time()
        })

        if "data_asset_type" in batch_kwargs:
            data_asset_type_config = reader_options.pop("data_asset_type")  # Get and remove the config
            try:
                data_asset_type_config = ClassConfig(**data_asset_type_config)
            except TypeError:
                # We tried; we'll pass the config downstream, probably as a string, and handle an error later
                pass
        else:
            data_asset_type_config = self._data_asset_type

        data_asset_type = self._get_data_asset_class(data_asset_type_config)

        if not issubclass(data_asset_type, PandasDataset):
            raise ValueError("PandasDatasource cannot instantiate batch with data_asset_type: '%s'. It "
                             "must be a subclass of PandasDataset." % data_asset_type.__name__)

        if isinstance(batch_kwargs, PathBatchKwargs):
        # if "path" in batch_kwargs:
            path = reader_options.pop("path")  # We remove this so it is not used as a reader option
            reader_options.pop("timestamp", "")    # ditto timestamp (but missing ok)
            reader_options.pop("partition_id", "")

            reader_method = reader_options.pop("reader_method", None)
            reader_fn, reader_fn_options = self._get_reader_fn(reader_method, path, reader_options)
            try:
                df = getattr(pd, reader_fn)(path, **reader_fn_options)
            except AttributeError:
                raise BatchKwargsError("Unsupported reader: %s" % reader_method.name, batch_kwargs)

        elif "s3" in batch_kwargs:
            try:
                import boto3
                s3 = boto3.client("s3")
            except ImportError:
                raise BatchKwargsError("Unable to load boto3 client to read s3 asset.", batch_kwargs)
            raw_url = reader_options.pop("s3")  # We need to remove from the reader
            reader_options.pop("timestamp", "")  # ditto timestamp (but missing ok)
            reader_method = reader_options.pop("reader_method", None)
            url = S3Url(raw_url)
            logger.debug("Fetching s3 object. Bucket: %s Key: %s" % (url.bucket, url.key))
            s3_object = s3.get_object(Bucket=url.bucket, Key=url.key)
            reader_fn, reader_fn_options = self._get_reader_fn(reader_method, url.key, reader_options)

            try:
                df = getattr(pd, reader_fn)(
                    StringIO(s3_object["Body"].read().decode(s3_object.get("ContentEncoding", "utf-8"))),
                    **reader_fn_options
                )
            except AttributeError:
                raise BatchKwargsError("Unsupported reader: %s" % reader_method.name, batch_kwargs)
            except IOError:
                raise

        elif "df" in batch_kwargs and isinstance(batch_kwargs["df"], (pd.DataFrame, pd.Series)):
            df = batch_kwargs.pop("df")  # We don't want to store the actual dataframe in kwargs
            batch_id["PandasInMemoryDF"] = True
            if df.memory_usage().sum() < HASH_THRESHOLD:
                batch_id["fingerprint"] = hashlib.md5(pd.util.hash_pandas_object(df, index=True).values).hexdigest()
        else:
            raise BatchKwargsError("Invalid batch_kwargs: path, s3, or df is required for a PandasDatasource",
                                   batch_kwargs)

        # FIXME: currently, batch_id and batch_kwargs are overlapping and are both included completely
        return data_asset_type(df,
                               expectation_suite=expectation_suite,
                               data_context=self._data_context,
                               batch_kwargs=batch_kwargs,
                               batch_id=batch_id)

    def build_batch_kwargs(self, data_asset_name, *args, **kwargs):
        """
        Build batch kwargs for a requested data_asset. Try to use a generator where possible to support partitioning,
        but fall back to datasource-default behavior if the generator cannot be identified.

        Args:
            data_asset_name: the data asset for which to build batch_kwargs; if a normalized name is provided,
                use the named generator.
            *args: at most exactly one positional argument can be provided from which to build kwargs
            **kwargs: additional keyword arguments to be used to build the batch_kwargs

        Returns:
            A PandasDatasourceBatchKwargs object suitable for building a batch of data from this datasource

        """
        generator = None
        if isinstance(data_asset_name, (NormalizedDataAssetName, DataAssetIdentifier)):
            generator = self.get_generator(data_asset_name.generator)
        elif len(self._datasource_config["generators"]) == 1:
            logger.info("Using only configured generator to build batch_kwargs.")
            generator_name = list(self._datasource_config["generators"].keys())[0]
            generator = self.get_generator(generator_name)

        # Now that we know whether we can use a generator, build the kwargs
        if generator is not None:
            if len(args) == 1:  # We interpret a single argument as a partition_id
                batch_kwargs = generator.build_batch_kwargs_from_partition(args[0], batch_kwargs=kwargs)
            elif len(args) > 0:
                raise BatchKwargsError("Multiple positional arguments were provided to build_batch_kwargs, but only"
                                       "one is supported. Please provide named arguments to build_batch_kwargs.")
            elif "partition_id" in kwargs:
                batch_kwargs = generator.build_batch_kwargs_from_partition(kwargs["partition_id"], batch_kwargs=kwargs)
            else:
                batch_kwargs = generator.yield_batch_kwargs(data_asset_name, kwargs)
        else:
            logger.warning("Unable to determine generator; building batch_kwargs using datasource default logic."
                           "Partitions will not be available. Consider using a typed data_asset_name to specify the "
                           "generator to use.")

            if len(args) == 1:
                if isinstance(args[0], (pd.DataFrame, pd.Series)):
                    kwargs.update({
                        "df": args[0]
                    })
                    batch_kwargs = PandasDatasourceMemoryBatchKwargs(**kwargs)
                elif isinstance(args[0], str):
                    kwargs.update({
                        "path": args[0],
                    })
                    batch_kwargs = PathBatchKwargs(**kwargs)
            elif len(args) > 1:
                raise BatchKwargsError("Multiple positional arguments were provided to build_batch_kwargs, but only"
                                       "one is supported. Please provide named arguments to build_batch_kwargs.")
            else:
                # Only kwargs were specified
                if "path" in kwargs and isinstance(kwargs["path"], string_types):
                    batch_kwargs = PathBatchKwargs(**kwargs)
                elif "df" in kwargs and isinstance(kwargs["df"], (pd.DataFrame, pd.Series)):
                    batch_kwargs = PandasDatasourceMemoryBatchKwargs(**kwargs)
                else:
                    raise BatchKwargsError("Invalid kwargs provided to build_batch_kwargs: either a valid df or path"
                                           "key must be provided.")
        return batch_kwargs

    def _get_reader_fn(self, reader_method, path, reader_options):
        if reader_method is None:
            reader_method = self._guess_reader_method_from_path(path)
            if reader_method is None:
                raise BatchKwargsError("Unable to determine reader for path: %s" % path, reader_options)
        else:
            try:
                reader_method = ReaderMethods[reader_method]
            except KeyError:
                raise BatchKwargsError("Unknown reader method: %s" % reader_method, reader_options)

        if reader_method == ReaderMethods.CSV:
            return "read_csv", reader_options
        elif reader_method == ReaderMethods.parquet:
            return "read_parquet", reader_options
        elif reader_method == ReaderMethods.excel:
            return "read_excel", reader_options
        elif reader_method == ReaderMethods.JSON:
            return "read_json", reader_options
        elif reader_method == ReaderMethods.CSV_GZ:
            return "read_csv", reader_options.update({"compression": "gzip"})

        return None
