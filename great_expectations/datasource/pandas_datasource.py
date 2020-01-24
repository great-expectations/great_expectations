import datetime
import uuid
import hashlib
import logging
from functools import partial


try:
    from io import StringIO
except ImportError:
    from StringIO import StringIO

from six import PY2

import pandas as pd

from .datasource import Datasource
from great_expectations.datasource.types import BatchMarkers
from great_expectations.core.batch import Batch
from great_expectations.types import ClassConfig
from great_expectations.exceptions import BatchKwargsError
from .util import S3Url

logger = logging.getLogger(__name__)

HASH_THRESHOLD = 1e9


class PandasDatasource(Datasource):
    """The PandasDatasource produces PandasDataset objects and supports generators capable of 
    interacting with the local filesystem (the default subdir_reader generator), and from
    existing in-memory dataframes.
    """
    recognized_batch_parameters = {'reader_method', 'reader_options', 'limit'}

    @classmethod
    def build_configuration(cls, data_asset_type=None, generators=None, boto3_options=None, **kwargs):
        """
        Build a full configuration object for a datasource, potentially including generators with defaults.

        Args:
            data_asset_type: A ClassConfig dictionary
            generators: Generator configuration dictionary
            boto3_options: Optional dictionary with key-value pairs to pass to boto3 during instantiation.
            **kwargs: Additional kwargs to be part of the datasource constructor's initialization

        Returns:
            A complete datasource configuration.

        """
        if generators is None:
            # Provide a gentle way to build a datasource with a sane default,
            # including ability to specify the base_directory and reader_options
            base_directory = kwargs.pop("base_directory", "data")
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
                },
                "passthrough": {
                    "class_name": "PassthroughGenerator",
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
        if boto3_options is not None:
            if isinstance(boto3_options, dict):
                configuration.update(boto3_options)
            else:
                raise ValueError("boto3_options must be a dictionary of key-value pairs to pass to boto3 upon "
                                 "initialization.")
        return configuration

    def __init__(self, name="pandas", data_context=None, data_asset_type=None, generators=None,
                 boto3_options=None, **kwargs):
        configuration_with_defaults = PandasDatasource.build_configuration(data_asset_type, generators,
                                                                           boto3_options, **kwargs)
        data_asset_type = configuration_with_defaults.pop("data_asset_type")
        generators = configuration_with_defaults.pop("generators")
        super(PandasDatasource, self).__init__(name,
                                               data_context=data_context,
                                               data_asset_type=data_asset_type,
                                               generators=generators,
                                               **configuration_with_defaults)
        self._build_generators()
        self._boto3_options = configuration_with_defaults.get("boto3_options", {})

    def process_batch_parameters(self, reader_method=None, reader_options=None, limit=None):
        batch_parameters = self.config.get("batch_parameters", {})
        batch_kwargs = dict()

        # Apply globally-configured reader options first
        if reader_options:
            # Then update with any locally-specified reader options
            if not batch_parameters.get("reader_options"):
                batch_parameters["reader_options"] = dict()
            batch_parameters["reader_options"].update(reader_options)
        if batch_parameters.get("reader_options"):
            batch_kwargs["reader_options"] = batch_parameters["reader_options"]

        limit = batch_parameters.get("limit", limit)
        if limit is not None:
            batch_parameters["limit"] = limit
            if not batch_kwargs.get("reader_options"):
                batch_kwargs["reader_options"] = dict()
            batch_kwargs['reader_options']['nrows'] = limit

        reader_method = batch_parameters.get("reader_method", reader_method)
        if reader_method is not None:
            batch_parameters["reader_method"] = reader_method
            batch_kwargs["reader_method"] = reader_method

        return batch_parameters, batch_kwargs

    def get_batch(self, batch_kwargs, batch_parameters=None):
        # pandas cannot take unicode as a delimiter, which can happen in py2. Handle this case explicitly.
        # We handle it here so that the updated value will be in the batch_kwargs for transparency to the user.
        if PY2 and "reader_options" in batch_kwargs and "sep" in batch_kwargs['reader_options'] and \
                batch_kwargs['reader_options']['sep'] is not None:
            batch_kwargs['reader_options']['sep'] = str(batch_kwargs['reader_options']['sep'])
        # We will use and manipulate reader_options along the way
        reader_options = batch_kwargs.get("reader_options", {})

        # We need to build a batch_markers to be used in the dataframe
        batch_markers = BatchMarkers({
            "ge_load_time": datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")
        })

        if "path" in batch_kwargs:
            path = batch_kwargs['path']
            reader_method = batch_kwargs.get("reader_method")
            reader_fn = self._get_reader_fn(reader_method, path)
            df = reader_fn(path, **reader_options)

        elif "s3" in batch_kwargs:
            try:
                import boto3
                s3 = boto3.client("s3", **self._boto3_options)
            except ImportError:
                raise BatchKwargsError("Unable to load boto3 client to read s3 asset.", batch_kwargs)
            raw_url = batch_kwargs["s3"]
            reader_method = batch_kwargs.get("reader_method")
            url = S3Url(raw_url)
            logger.debug("Fetching s3 object. Bucket: %s Key: %s" % (url.bucket, url.key))
            s3_object = s3.get_object(Bucket=url.bucket, Key=url.key)
            reader_fn = self._get_reader_fn(reader_method, url.key)
            df = reader_fn(
                StringIO(s3_object["Body"].read().decode(s3_object.get("ContentEncoding", "utf-8"))),
                **reader_options
            )

        elif "dataset" in batch_kwargs and isinstance(batch_kwargs["dataset"], (pd.DataFrame, pd.Series)):
            df = batch_kwargs.get("dataset")
            # We don't want to store the actual dataframe in kwargs; copy the remaining batch_kwargs
            batch_kwargs = {k: batch_kwargs[k] for k in batch_kwargs if k != 'dataset'}
            batch_kwargs["PandasInMemoryDF"] = True
            batch_kwargs["ge_batch_id"] = uuid.uuid1()

        else:
            raise BatchKwargsError("Invalid batch_kwargs: path, s3, or df is required for a PandasDatasource",
                                   batch_kwargs)

        if df.memory_usage().sum() < HASH_THRESHOLD:
            batch_markers["pandas_data_fingerprint"] = hashlib.md5(pd.util.hash_pandas_object(
                df, index=True).values).hexdigest()

        return Batch(
            datasource_name=self.name,
            batch_kwargs=batch_kwargs,
            data=df,
            batch_parameters=batch_parameters,
            batch_markers=batch_markers,
            data_context=self._data_context
        )

    @staticmethod
    def _get_reader_fn(reader_method=None, path=None):
        """Static helper for parsing reader types. If reader_method is not provided, path will be used to guess the
        correct reader_method.

        Args:
            reader_method (str): the name of the reader method to use, if available.
            path (str): the to use to guess

        Returns:
            ReaderMethod to use for the filepath

        """
        if reader_method is None and path is None:
            raise BatchKwargsError("Unable to determine pandas reader function without reader_method or path.",
                                   {"reader_method": reader_method})

        if reader_method is not None:
            try:
                return getattr(pd, reader_method)
            except AttributeError:
                raise BatchKwargsError("Unable to find reader_method %s in pandas." % reader_method,
                                       {"reader_method": reader_method})

        if path.endswith(".csv") or path.endswith(".tsv"):
            return pd.read_csv
        elif path.endswith(".parquet"):
            return pd.read_parquet
        elif path.endswith(".xlsx") or path.endswith(".xls"):
            return pd.read_excel
        elif path.endswith(".json"):
            return pd.read_json
        elif path.endswith(".csv.gz") or path.endswith(".csv.gz"):
            return partial(pd.read_csv, compression="gzip")

        raise BatchKwargsError("Unknown reader method: %s" % reader_method, {"reader_method": reader_method})
