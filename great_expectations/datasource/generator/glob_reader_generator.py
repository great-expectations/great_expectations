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
    """GlobReaderGenerator processes files in a directory according to glob patterns to produce batches of data.

    A more interesting asset_glob might look like the following:

    daily_logs:
      glob: daily_logs/*.csv
      partition_regex: daily_logs/(19|20)\d\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])_(.*)\.csv

    """

    def __init__(self, name="default",
                 datasource=None,
                 base_directory="/data",
                 reader_options=None,
                 asset_globs=None):
        logger.debug("Constructing GlobReaderGenerator {!r}".format(name))
        super(GlobReaderGenerator, self).__init__(name, type_="glob_reader", datasource=datasource)
        if reader_options is None:
            reader_options = {}

        if asset_globs is None:
            asset_globs = {
                "default": {
                    "glob": "*",
                    "partition_regex": r"^(19|20)\d\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01])_(.*)\.csv",
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
        known_assets = set()
        if not os.path.isdir(self.base_directory):
            return known_assets
        for generator_asset in self.asset_globs.keys():
            if isinstance(self.asset_globs[generator_asset], string_types):
                warnings.warn("String-only glob configuration has been deprecated and will be removed in a future"
                              "release. See GlobReaderGenerator docstring for more information on the new configuration"
                              "format.", DeprecationWarning)
                glob_ = self.asset_globs[generator_asset]
            else:
                glob_ = self.asset_globs[generator_asset]["glob"]
            batch_paths = glob.glob(os.path.join(self.base_directory, glob_))
            if len(batch_paths) > 0:
                known_assets.add(generator_asset)

        return known_assets

    def _get_iterator(self, generator_asset, **kwargs):
        if generator_asset not in self._asset_globs:
            batch_kwargs = {
                "generator_asset": generator_asset,
            }
            batch_kwargs.update(kwargs)
            raise BatchKwargsError("Unknown asset_name %s" % generator_asset, batch_kwargs)

        if isinstance(self.asset_globs[generator_asset], string_types):
            warnings.warn("String-only glob configuration has been deprecated and will be removed in a future"
                          "release. See GlobReaderGenerator docstring for more information on the new configuration"
                          "format.", DeprecationWarning)
            glob_config = {"glob": self.asset_globs[generator_asset]}
        else:
            glob_config = self.asset_globs[generator_asset]

        paths = glob.glob(os.path.join(self.base_directory, glob_config["glob"]))
        return self._build_batch_kwargs_path_iter(paths, glob_config)

    def _build_batch_kwargs_path_iter(self, path_list, glob_config):
        for path in path_list:
            yield self._build_batch_kwargs(path, glob_config)

    def _build_batch_kwargs(self, path, glob_config):
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
            # So, we'll add a sortable id
            if matches is None:
                logger.warning("No match found for path: %s" % path)
                return datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ") + "__unmatched"
            else:
                try:
                    return matches.group(match_group_id)
                except IndexError:
                    logger.warning("No match group %d in path %s" % (match_group_id, path))
                    return datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ") + "__no_match_group"
        # There is no partitioner defined; do not add a partition_id
        return None
