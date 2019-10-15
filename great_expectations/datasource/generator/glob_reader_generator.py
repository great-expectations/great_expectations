import os
import time
import glob
import re
import datetime
import logging
import warnings

from six import string_types

from great_expectations.datasource.generator.batch_generator import BatchGenerator
from great_expectations.datasource.types import PathBatchKwargs
from great_expectations.exceptions import BatchKwargsError

logger = logging.getLogger(__name__)


class GlobReaderGenerator(BatchGenerator):
    r"""GlobReaderGenerator processes files in a directory according to glob patterns to produce batches of data.

    A more interesting asset_glob might look like the following::

        daily_logs:
          glob: daily_logs/*.csv
          partition_regex: daily_logs/((19|20)\d\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01]))_(.*)\.csv


    The "glob" key ensures that every csv file in the daily_logs directory is considered a batch for this data asset.
    The "partition_regex" key ensures that files whose basename begins with a date (with components hyphen, space,
    forward slash, period, or null separated) will be identified by a partition_id equal to just the date portion of
    their name.

    A fully configured GlobReaderGenerator in yml might look like the following::

        my_datasource:
          class_name: PandasDatasource
          generators:
            my_generator:
              class_name: GlobReaderGenerator
              base_directory: /var/log
              reader_options:
                sep: %
                header: 0
              asset_globs:
                wifi_logs:
                  glob: wifi*.log
                  partition_regex: wifi-((0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])-20\d\d).*\.log

    """

    def __init__(self, name="default",
                 datasource=None,
                 base_directory="/data",
                 reader_options=None,
                 asset_globs=None):
        logger.debug("Constructing GlobReaderGenerator {!r}".format(name))
        super(GlobReaderGenerator, self).__init__(name, datasource=datasource)
        if reader_options is None:
            reader_options = {}

        if asset_globs is None:
            asset_globs = {
                "default": {
                    "glob": "*",
                    "partition_regex": r"^((19|20)\d\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01])_(.*))\.csv",
                    "match_group_id": 1
                }
            }

        self._base_directory = base_directory
        self._reader_options = reader_options
        self._asset_globs = asset_globs

    @property
    def reader_options(self):
        return self._reader_options

    @property
    def asset_globs(self):
        return self._asset_globs

    @property
    def base_directory(self):
        # If base directory is a relative path, interpret it as relative to the data context's
        # context root directory (parent directory of great_expectation dir)
        if os.path.isabs(self._base_directory) or self._datasource.get_data_context() is None:
            return self._base_directory
        else:
            return os.path.join(self._datasource.get_data_context().root_directory, self._base_directory)

    def get_available_data_asset_names(self):
        known_assets = []
        if not os.path.isdir(self.base_directory):
            return known_assets
        for generator_asset in self.asset_globs.keys():
            batch_paths = self._get_generator_asset_paths(generator_asset)
            if len(batch_paths) > 0 and generator_asset not in known_assets:
                known_assets.append(generator_asset)

        return known_assets

    def get_available_partition_ids(self, generator_asset):
        glob_config = self._get_generator_asset_config(generator_asset)
        batch_paths = self._get_generator_asset_paths(generator_asset)
        partition_ids = [
            self._partitioner(path, glob_config) for path in batch_paths
            if self._partitioner(path, glob_config) is not None
        ]
        return partition_ids

    def build_batch_kwargs_from_partition_id(self, generator_asset, partition_id=None, batch_kwargs=None, **kwargs):
        """Build batch kwargs from a partition id."""
        glob_config = self._get_generator_asset_config(generator_asset)
        batch_paths = self._get_generator_asset_paths(generator_asset)
        path = [path for path in batch_paths if self._partitioner(path, glob_config) == partition_id]
        if len(path) != 1:
            raise BatchKwargsError("Unable to identify partition %s for asset %s" % (partition_id, generator_asset),
                                   {
                                        generator_asset: generator_asset,
                                        partition_id: partition_id
                                    })
        new_kwargs = self._build_batch_kwargs_from_path(path[0], glob_config)
        if batch_kwargs is not None:
            new_kwargs.update(batch_kwargs)
        if kwargs is not None:
            new_kwargs.update(kwargs)
        return new_kwargs

    def _get_generator_asset_paths(self, generator_asset):
        """
        Returns a list of filepaths associated with the given generator_asset

        Args:
            generator_asset:

        Returns:
            paths (list)
        """
        glob_config = self._get_generator_asset_config(generator_asset)
        return glob.glob(os.path.join(self.base_directory, glob_config["glob"]))

    def _get_generator_asset_config(self, generator_asset):
        if generator_asset not in self._asset_globs:
            batch_kwargs = {
                "generator_asset": generator_asset,
            }
            raise BatchKwargsError("Unknown asset_name %s" % generator_asset, batch_kwargs)

        if isinstance(self.asset_globs[generator_asset], string_types):
            warnings.warn("String-only glob configuration has been deprecated and will be removed in a future"
                          "release. See GlobReaderGenerator docstring for more information on the new configuration"
                          "format.", DeprecationWarning)
            glob_config = {"glob": self.asset_globs[generator_asset]}
        else:
            glob_config = self.asset_globs[generator_asset]
        return glob_config

    def _get_iterator(self, generator_asset, **kwargs):
        glob_config = self._get_generator_asset_config(generator_asset)
        paths = glob.glob(os.path.join(self.base_directory, glob_config["glob"]))
        return self._build_batch_kwargs_path_iter(paths, glob_config)

    def _build_batch_kwargs_path_iter(self, path_list, glob_config):
        for path in path_list:
            yield self._build_batch_kwargs_from_path(path, glob_config)

    def _build_batch_kwargs_from_path(self, path, glob_config):
        # We could add MD5 (e.g. for smallish files)
        # but currently don't want to assume the extra read is worth it
        # unless it's configurable
        # with open(path,'rb') as f:
        #     md5 = hashlib.md5(f.read()).hexdigest()
        batch_kwargs = PathBatchKwargs({
            "path": path,
            "timestamp": time.time()
        })
        partition_id = self._partitioner(path, glob_config)
        if partition_id is not None:
            batch_kwargs.update({"partition_id": partition_id})

        batch_kwargs.update(self.reader_options)
        return batch_kwargs

    def _partitioner(self, path, glob_config):
        if "partition_regex" in glob_config:
            match_group_id = glob_config.get("match_group_id", 1)
            matches = re.match(glob_config["partition_regex"], path)
            # In the case that there is a defined regex, the user *wanted* a partition. But it didn't match.
            # So, we'll add a *sortable* id
            if matches is None:
                logger.warning("No match found for path: %s" % path)
                return datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ") + "__unmatched"
            else:
                try:
                    return matches.group(match_group_id)
                except IndexError:
                    logger.warning("No match group %d in path %s" % (match_group_id, path))
                    return datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ") + "__no_match_group"

        # If there is no partitioner defined, fall back on using the path as a partition_id
        else:
            if path.startswith(self.base_directory):
                path = path[len(self.base_directory):]
                # In case os.join had to add a "/"
                if path.startswith("/"):
                    path = path[1:]
            return path

