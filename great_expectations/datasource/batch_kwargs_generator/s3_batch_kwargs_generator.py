import datetime
import logging
import re
import warnings

from great_expectations.datasource.batch_kwargs_generator.batch_kwargs_generator import (
    BatchKwargsGenerator,
)
from great_expectations.datasource.types import S3BatchKwargs
from great_expectations.exceptions import BatchKwargsError, GreatExpectationsError

try:
    import boto3
except ImportError:
    boto3 = None
logger = logging.getLogger(__name__)


class S3GlobReaderBatchKwargsGenerator(BatchKwargsGenerator):
    '\n    S3 BatchKwargGenerator provides support for generating batches of data from an S3 bucket. For the S3 batch kwargs generator, assets must\n    be individually defined using a prefix and glob, although several additional configuration parameters are available\n    for assets (see below).\n\n    Example configuration::\n\n        datasources:\n          my_datasource:\n            ...\n            batch_kwargs_generator:\n              my_s3_generator:\n                class_name: S3GlobReaderBatchKwargsGenerator\n                bucket: my_bucket.my_organization.priv\n                reader_method: parquet  # This will be automatically inferred from suffix where possible, but can be explicitly specified as well\n                reader_options:  # Note that reader options can be specified globally or per-asset\n                  sep: ","\n                delimiter: "/"  # Note that this is the delimiter for the BUCKET KEYS. By default it is "/"\n                boto3_options:\n                  endpoint_url: $S3_ENDPOINT  # Use the S3_ENDPOINT environment variable to determine which endpoint to use\n                max_keys: 100  # The maximum number of keys to fetch in a single list_objects request to s3. When accessing batch_kwargs through an iterator, the iterator will silently refetch if more keys were available\n                assets:\n                  my_first_asset:\n                    prefix: my_first_asset/\n                    regex_filter: .*  # The regex filter will filter the results returned by S3 for the key and prefix to only those matching the regex\n                    directory_assets: True # if True, the contents of the directory will be treated as one batch. Notice that this option does not work with Pandas, since Pandas does not support loading multiple files from and S3 bucket into a data frame.\n                  access_logs:\n                    prefix: access_logs\n                    regex_filter: access_logs/2019.*\\.csv.gz\n                    sep: "~"\n                    max_keys: 100\n'
    recognized_batch_parameters = {
        "data_asset_name",
        "partition_id",
        "reader_method",
        "reader_options",
        "limit",
    }

    def __init__(
        self,
        name="default",
        datasource=None,
        bucket=None,
        reader_options=None,
        assets=None,
        delimiter="/",
        reader_method=None,
        boto3_options=None,
        max_keys=1000,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Initialize a new S3GlobReaderBatchKwargsGenerator\n\n        Args:\n            name: the name of the batch kwargs generator\n            datasource: the datasource to which it is attached\n            bucket: the name of the s3 bucket from which it generates batch_kwargs\n            reader_options: options passed to the datasource reader method\n            assets: asset configuration (see class docstring for more information)\n            delimiter: the BUCKET KEY delimiter\n            reader_method: the reader_method to include in generated batch_kwargs\n            boto3_options: dictionary of key-value pairs to use when creating boto3 client or resource objects\n            max_keys: the maximum number of keys to fetch in a single list_objects request to s3\n        "
        super().__init__(name, datasource=datasource)
        if reader_options is None:
            reader_options = {}
        if assets is None:
            assets = {"default": {"prefix": "", "regex_filter": ".*"}}
        self._bucket = bucket
        self._reader_method = reader_method
        self._reader_options = reader_options
        self._assets = assets
        self._delimiter = delimiter
        if boto3_options is None:
            boto3_options = {}
        self._max_keys = max_keys
        self._iterators = {}
        try:
            self._s3 = boto3.client("s3", **boto3_options)
        except TypeError:
            raise ImportError(
                "Unable to load boto3, which is required for S3 batch kwargs generator"
            )

    @property
    def reader_options(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._reader_options

    @property
    def assets(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._assets

    @property
    def bucket(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._bucket

    def get_available_data_asset_names(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return {"names": [(key, "file") for key in self._assets.keys()]}

    def _get_iterator(
        self, data_asset_name, reader_method=None, reader_options=None, limit=None
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        logger.debug(
            "Beginning S3GlobReaderBatchKwargsGenerator _get_iterator for data_asset_name: %s"
            % data_asset_name
        )
        if data_asset_name not in self._assets:
            batch_kwargs = {
                "data_asset_name": data_asset_name,
                "reader_method": reader_method,
                "reader_options": reader_options,
                "limit": limit,
            }
            raise BatchKwargsError(
                f"Unknown asset_name {data_asset_name}", batch_kwargs
            )
        if data_asset_name not in self._iterators:
            self._iterators[data_asset_name] = {}
        asset_config = self._assets[data_asset_name]
        return self._build_asset_iterator(
            asset_config=asset_config,
            iterator_dict=self._iterators[data_asset_name],
            reader_method=reader_method,
            reader_options=reader_options,
            limit=limit,
        )

    def _build_batch_kwargs_path_iter(
        self, path_list, reader_options=None, limit=None
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        for path in path_list:
            (
                yield self._build_batch_kwargs(
                    path, reader_options=reader_options, limit=limit
                )
            )

    def _build_batch_kwargs(self, batch_parameters):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        try:
            data_asset_name = batch_parameters.pop("data_asset_name")
        except KeyError:
            raise BatchKwargsError(
                "Unable to build BatchKwargs: no name provided in batch_parameters.",
                batch_kwargs=batch_parameters,
            )
        partition_id = batch_parameters.pop("partition_id", None)
        batch_kwargs = self._datasource.process_batch_parameters(batch_parameters)
        if partition_id:
            try:
                asset_config = self._assets[data_asset_name]
            except KeyError:
                raise GreatExpectationsError(
                    f"No asset config found for asset {data_asset_name}"
                )
            if data_asset_name not in self._iterators:
                self._iterators[data_asset_name] = {}
            iterator_dict = self._iterators[data_asset_name]
            for key in self._get_asset_options(asset_config, iterator_dict):
                if (
                    self._partitioner(key=key, asset_config=asset_config)
                    == partition_id
                ):
                    batch_kwargs = self._build_batch_kwargs_from_key(
                        key=key,
                        asset_config=asset_config,
                        reader_options=batch_parameters.get("reader_options"),
                        limit=batch_kwargs.get("limit"),
                    )
            if batch_kwargs is None:
                raise BatchKwargsError(
                    (
                        "Unable to identify partition %s for asset %s"
                        % (partition_id, data_asset_name)
                    ),
                    {data_asset_name: data_asset_name, partition_id: partition_id},
                )
            return batch_kwargs
        else:
            return self.yield_batch_kwargs(
                data_asset_name=data_asset_name, **batch_parameters, **batch_kwargs
            )

    def _build_batch_kwargs_from_key(
        self,
        key,
        asset_config=None,
        reader_method=None,
        reader_options=None,
        limit=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        batch_kwargs = {
            "s3": f"s3a://{self.bucket}/{key}",
            "reader_options": self.reader_options,
        }
        if asset_config.get("reader_options"):
            batch_kwargs["reader_options"].update(asset_config.get("reader_options"))
        if reader_options is not None:
            batch_kwargs["reader_options"].update(reader_options)
        if self._reader_method is not None:
            batch_kwargs["reader_method"] = self._reader_method
        if asset_config.get("reader_method"):
            batch_kwargs["reader_method"] = asset_config.get("reader_method")
        if reader_method is not None:
            batch_kwargs["reader_method"] = reader_method
        if limit:
            batch_kwargs["limit"] = limit
        return S3BatchKwargs(batch_kwargs)

    def _get_asset_options(self, asset_config, iterator_dict) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        query_options = {
            "Bucket": self.bucket,
            "Delimiter": asset_config.get("delimiter", self._delimiter),
            "Prefix": asset_config.get("prefix", None),
            "MaxKeys": asset_config.get("max_keys", self._max_keys),
        }
        directory_assets = asset_config.get("directory_assets", False)
        if "continuation_token" in iterator_dict:
            query_options.update(
                {"ContinuationToken": iterator_dict["continuation_token"]}
            )
        logger.debug(
            f"Fetching objects from S3 with query options: {str(query_options)}"
        )
        asset_options = self._s3.list_objects_v2(**query_options)
        if directory_assets:
            if "CommonPrefixes" not in asset_options:
                raise BatchKwargsError(
                    "Unable to build batch_kwargs. The asset may not be configured correctly. If directory assets are requested, then common prefixes must be returned.",
                    {
                        "asset_configuration": asset_config,
                        "contents": (
                            asset_options["Contents"]
                            if ("Contents" in asset_options)
                            else None
                        ),
                    },
                )
            keys = [item["Prefix"] for item in asset_options["CommonPrefixes"]]
        else:
            if "Contents" not in asset_options:
                raise BatchKwargsError(
                    "Unable to build batch_kwargs. The asset may not be configured correctly. If s3 returned common prefixes it may not have been able to identify desired keys, and they are included in the incomplete batch_kwargs object returned with this error.",
                    {
                        "asset_configuration": asset_config,
                        "common_prefixes": (
                            asset_options["CommonPrefixes"]
                            if ("CommonPrefixes" in asset_options)
                            else None
                        ),
                    },
                )
            keys = [
                item["Key"] for item in asset_options["Contents"] if (item["Size"] > 0)
            ]
        keys = [
            key
            for key in filter(
                (
                    lambda x: (
                        re.match(asset_config.get("regex_filter", ".*"), x) is not None
                    )
                ),
                keys,
            )
        ]
        (yield from keys)
        if asset_options["IsTruncated"]:
            iterator_dict["continuation_token"] = asset_options["NextContinuationToken"]
            (yield from self._get_asset_options(asset_config, iterator_dict))
        elif "continuation_token" in iterator_dict:
            del iterator_dict["continuation_token"]

    def _build_asset_iterator(
        self,
        asset_config,
        iterator_dict,
        reader_method=None,
        reader_options=None,
        limit=None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        for key in self._get_asset_options(asset_config, iterator_dict):
            (
                yield self._build_batch_kwargs_from_key(
                    key,
                    asset_config,
                    reader_method=None,
                    reader_options=reader_options,
                    limit=limit,
                )
            )

    def get_available_partition_ids(self, generator_asset=None, data_asset_name=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        assert (generator_asset and (not data_asset_name)) or (
            (not generator_asset) and data_asset_name
        ), "Please provide either generator_asset or data_asset_name."
        if generator_asset:
            warnings.warn(
                "The 'generator_asset' argument is deprecated as of v0.11.0 and will be removed in v0.16. Please use 'data_asset_name' instead.",
                DeprecationWarning,
            )
            data_asset_name = generator_asset
        if data_asset_name not in self._iterators:
            self._iterators[data_asset_name] = {}
        iterator_dict = self._iterators[data_asset_name]
        asset_config = self._assets[data_asset_name]
        available_ids = [
            self._partitioner(key=key, asset_config=asset_config)
            for key in self._get_asset_options(asset_config, iterator_dict)
        ]
        return available_ids

    def _partitioner(self, key, asset_config):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if "partition_regex" in asset_config:
            match_group_id = asset_config.get("match_group_id", 1)
            matches = re.match(asset_config["partition_regex"], key)
            if matches is None:
                logger.warning(f"No match found for key: {key}")
                return (
                    datetime.datetime.now(datetime.timezone.utc).strftime(
                        "%Y%m%dT%H%M%S.%fZ"
                    )
                    + "__unmatched"
                )
            else:
                try:
                    return matches.group(match_group_id)
                except IndexError:
                    logger.warning(
                        "No match group %d in key %s" % (match_group_id, key)
                    )
                    return (
                        datetime.datetime.now(datetime.timezone.utc).strftime(
                            "%Y%m%dT%H%M%S.%fZ"
                        )
                        + "__no_match_group"
                    )
        else:
            prefix = asset_config.get("prefix", "")
            if key.startswith(prefix):
                key = key[len(prefix) :]
            return key
