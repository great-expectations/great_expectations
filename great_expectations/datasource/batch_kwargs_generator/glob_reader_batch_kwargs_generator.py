import datetime
import glob
import logging
import os
import re
import warnings

from great_expectations.datasource.batch_kwargs_generator.batch_kwargs_generator import (
    BatchKwargsGenerator,
)
from great_expectations.datasource.types import PathBatchKwargs
from great_expectations.exceptions import BatchKwargsError

logger = logging.getLogger(__name__)


class GlobReaderBatchKwargsGenerator(BatchKwargsGenerator):
    'GlobReaderBatchKwargsGenerator processes files in a directory according to glob patterns to produce batches of data.\n\n    A more interesting asset_glob might look like the following::\n\n        daily_logs:\n          glob: daily_logs/*.csv\n          partition_regex: daily_logs/((19|20)\\d\\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01]))_(.*)\\.csv\n\n\n    The "glob" key ensures that every csv file in the daily_logs directory is considered a batch for this data asset.\n    The "partition_regex" key ensures that files whose basename begins with a date (with components hyphen, space,\n    forward slash, period, or null separated) will be identified by a partition_id equal to just the date portion of\n    their name.\n\n    A fully configured GlobReaderBatchKwargsGenerator in yml might look like the following::\n\n        my_datasource:\n          class_name: PandasDatasource\n          batch_kwargs_generators:\n            my_generator:\n              class_name: GlobReaderBatchKwargsGenerator\n              base_directory: /var/log\n              reader_options:\n                sep: %\n                header: 0\n              reader_method: csv\n              asset_globs:\n                wifi_logs:\n                  glob: wifi*.log\n                  partition_regex: wifi-((0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])-20\\d\\d).*\\.log\n                  reader_method: csv\n'
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
        base_directory="/data",
        reader_options=None,
        asset_globs=None,
        reader_method=None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        logger.debug(f"Constructing GlobReaderBatchKwargsGenerator {name!r}")
        super().__init__(name, datasource=datasource)
        if reader_options is None:
            reader_options = {}
        if asset_globs is None:
            asset_globs = {
                "default": {
                    "glob": "*",
                    "partition_regex": "^((19|20)\\d\\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01])_(.*))\\.csv",
                    "match_group_id": 1,
                    "reader_method": "read_csv",
                }
            }
        self._base_directory = base_directory
        self._reader_options = reader_options
        self._asset_globs = asset_globs
        self._reader_method = reader_method

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
    def asset_globs(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._asset_globs

    @property
    def reader_method(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._reader_method

    @property
    def base_directory(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if os.path.isabs(self._base_directory) or (
            self._datasource.data_context is None
        ):
            return self._base_directory
        else:
            return os.path.join(
                self._datasource.data_context.root_directory, self._base_directory
            )

    def get_available_data_asset_names(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        known_assets = []
        if not os.path.isdir(self.base_directory):
            return {"names": [(asset, "path") for asset in known_assets]}
        for data_asset_name in self.asset_globs.keys():
            batch_paths = self._get_data_asset_paths(data_asset_name=data_asset_name)
            if (len(batch_paths) > 0) and (data_asset_name not in known_assets):
                known_assets.append(data_asset_name)
        return {"names": [(asset, "path") for asset in known_assets]}

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
        glob_config = self._get_data_asset_config(data_asset_name)
        batch_paths = self._get_data_asset_paths(data_asset_name=data_asset_name)
        partition_ids = [
            self._partitioner(path, glob_config)
            for path in batch_paths
            if (self._partitioner(path, glob_config) is not None)
        ]
        return partition_ids

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
        if partition_id:
            glob_config = self._get_data_asset_config(data_asset_name)
            batch_paths = self._get_data_asset_paths(data_asset_name=data_asset_name)
            path = [
                path
                for path in batch_paths
                if (self._partitioner(path, glob_config) == partition_id)
            ]
            if len(path) != 1:
                raise BatchKwargsError(
                    (
                        "Unable to identify partition %s for asset %s"
                        % (partition_id, data_asset_name)
                    ),
                    {data_asset_name: data_asset_name, partition_id: partition_id},
                )
            batch_kwargs = self._build_batch_kwargs_from_path(
                path[0], glob_config, **batch_parameters
            )
            return batch_kwargs
        else:
            return self.yield_batch_kwargs(
                data_asset_name=data_asset_name, **batch_parameters
            )

    def _get_data_asset_paths(self, data_asset_name):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Returns a list of filepaths associated with the given data_asset_name\n\n        Args:\n            data_asset_name:\n\n        Returns:\n            paths (list)\n        "
        glob_config = self._get_data_asset_config(data_asset_name)
        return glob.glob(os.path.join(self.base_directory, glob_config["glob"]))

    def _get_data_asset_config(self, data_asset_name):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        try:
            return self.asset_globs[data_asset_name]
        except KeyError:
            batch_kwargs = {"data_asset_name": data_asset_name}
            raise BatchKwargsError(
                f"Unknown asset_name {data_asset_name}", batch_kwargs
            )

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
        glob_config = self._get_data_asset_config(data_asset_name)
        paths = glob.glob(os.path.join(self.base_directory, glob_config["glob"]))
        return self._build_batch_kwargs_path_iter(
            paths,
            glob_config,
            reader_method=reader_method,
            reader_options=reader_options,
            limit=limit,
        )

    def _build_batch_kwargs_path_iter(
        self,
        path_list,
        glob_config,
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
        for path in path_list:
            (
                yield self._build_batch_kwargs_from_path(
                    path,
                    glob_config,
                    reader_method=reader_method,
                    reader_options=reader_options,
                    limit=limit,
                )
            )

    def _build_batch_kwargs_from_path(
        self, path, glob_config, reader_method=None, reader_options=None, limit=None
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        batch_kwargs = self._datasource.process_batch_parameters(
            reader_method=(
                reader_method or glob_config.get("reader_method") or self.reader_method
            ),
            reader_options=(
                reader_options
                or glob_config.get("reader_options")
                or self.reader_options
            ),
            limit=(limit or glob_config.get("limit")),
        )
        batch_kwargs["path"] = path
        batch_kwargs["datasource"] = self._datasource.name
        return PathBatchKwargs(batch_kwargs)

    def _partitioner(self, path, glob_config):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if "partition_regex" in glob_config:
            match_group_id = glob_config.get("match_group_id", 1)
            matches = re.match(glob_config["partition_regex"], path)
            if matches is None:
                logger.warning(f"No match found for path: {path}")
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
                        "No match group %d in path %s" % (match_group_id, path)
                    )
                    return (
                        datetime.datetime.now(datetime.timezone.utc).strftime(
                            "%Y%m%dT%H%M%S.%fZ"
                        )
                        + "__no_match_group"
                    )
        else:
            if path.startswith(self.base_directory):
                path = path[len(self.base_directory) :]
                if path.startswith("/"):
                    path = path[1:]
            return path
