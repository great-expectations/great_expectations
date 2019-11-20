import os
import logging

from great_expectations.datasource.generator.batch_generator import BatchGenerator
from great_expectations.datasource.types import PathBatchKwargs
from great_expectations.exceptions import BatchKwargsError

logger = logging.getLogger(__name__)

KNOWN_EXTENSIONS = ['.csv', '.tsv', '.parquet', '.xls', '.xlsx', '.json', '.csv.gz', '.tsv.gz']


class SubdirReaderGenerator(BatchGenerator):
    """The SubdirReaderGenerator inspects a filesystem and produces path-based batch_kwargs.

    SubdirReaderGenerator recognizes generator_assets using two criteria:
      - for files directly in 'base_directory' with recognized extensions (.csv, .tsv, .parquet, .xls, .xlsx, .json),
        it uses the name of the file without the extension
      - for other files or directories in 'base_directory', is uses the file or directory name

    SubdirReaderGenerator sees all files inside a directory of base_directory as batches of one datasource.

    SubdirReaderGenerator can also include configured reader_options which will be added to batch_kwargs generated
    by this generator.
    """

    _default_reader_options = {}

    def __init__(self, name="default",
                 datasource=None,
                 base_directory="/data",
                 reader_options=None,
                 known_extensions=None):
        super(SubdirReaderGenerator, self).__init__(name, datasource=datasource)
        if reader_options is None:
            reader_options = self._default_reader_options

        if known_extensions is None:
            known_extensions = KNOWN_EXTENSIONS

        self._known_extensions = known_extensions
        self._reader_options = reader_options
        self._base_directory = base_directory

    @property
    def reader_options(self):
        return self._reader_options

    @property
    def known_extensions(self):
        return self._known_extensions

    @property
    def base_directory(self):
        # If base directory is a relative path, interpret it as relative to the data context's
        # context root directory (parent directory of great_expectation dir)
        if os.path.isabs(self._base_directory) or self._datasource.get_data_context() is None:
            return self._base_directory
        else:
            return os.path.join(self._datasource.get_data_context().root_directory, self._base_directory)

    def get_available_data_asset_names(self):
        if not os.path.isdir(self.base_directory):
            return []
        known_assets = self._get_valid_file_options(base_directory=self.base_directory)
        return {"names": known_assets,
                "is_complete_list": True
                }

    def get_available_partition_ids(self, generator_asset):
        # If the generator asset names a single known *file*, return ONLY that
        for extension in self.known_extensions:
            if os.path.isfile(os.path.join(self.base_directory, generator_asset + extension)):
                return [generator_asset]
        if os.path.isfile(os.path.join(self.base_directory, generator_asset)):
            return [generator_asset]

        # Otherwise, subdir files are partition ids
        return self._get_valid_file_options(base_directory=os.path.join(self.base_directory, generator_asset))

    def build_batch_kwargs_from_partition_id(self, generator_asset, partition_id=None, reader_options=None, limit=None):
        path = None
        for extension in self.known_extensions:
            if os.path.isfile(os.path.join(self.base_directory, generator_asset, partition_id + extension)):
                path = os.path.join(self.base_directory, generator_asset, partition_id + extension)

        if path is None:
            # Fall through to this case in the event that there is not a subdir available, so partition_id is
            # the same as the generator asset
            if os.path.isfile(os.path.join(self.base_directory, generator_asset)):
                path = os.path.join(self.base_directory, generator_asset)

            for extension in self.known_extensions:
                if os.path.isfile(os.path.join(self.base_directory, generator_asset + extension)):
                    path = os.path.join(self.base_directory, generator_asset + extension)

        if path is None:
            raise BatchKwargsError("Unable to build batch kwargs from partition_id for asset '%s'" % generator_asset, {
                "partition_id": partition_id
            })

        return self._build_batch_kwargs_from_path(path, reader_options=reader_options, limit=limit,
                                                  partition_id=partition_id)

    # def _get_valid_file_options(self, base_directory=None):
    #     valid_options = []
    #     if base_directory is None:
    #         base_directory = self.base_directory
    #     file_options = os.listdir(base_directory)
    #     for file_option in file_options:
    #         for extension in self.known_extensions:
    #             if (file_option.endswith(extension) and not file_option.startswith(".") and
    #                     file_option[:-len(extension)] not in valid_options):
    #                 valid_options.append(file_option[:-len(extension)])
    #             elif os.path.isdir(os.path.join(self.base_directory, file_option)):
    #                 # Make sure there's at least one valid file inside the subdir
    #                 subdir_options = self._get_valid_file_options(base_directory=os.path.join(base_directory,
    #                                                                                           file_option))
    #                 if len(subdir_options) > 0 and file_option not in valid_options:
    #                     valid_options.append(file_option)
    #     return valid_options


    def _get_valid_file_options(self, base_directory=None):
        valid_options = []
        if base_directory is None:
            base_directory = self.base_directory
        file_options = os.listdir(base_directory)
        for file_option in file_options:
            for extension in self.known_extensions:
                if (file_option.endswith(extension) and not file_option.startswith(".") and
                        (file_option[:-len(extension)], "file") not in valid_options):
                    valid_options.append((file_option[:-len(extension)], "file"))
                elif os.path.isdir(os.path.join(self.base_directory, file_option)):
                    # Make sure there's at least one valid file inside the subdir
                    subdir_options = self._get_valid_file_options(base_directory=os.path.join(base_directory,
                                                                                              file_option))
                    if len(subdir_options) > 0 and (file_option, "directory") not in valid_options:
                        valid_options.append((file_option, "directory"))
        return valid_options

    def _get_iterator(self, generator_asset, reader_options=None, limit=None):
        logger.debug("Beginning SubdirReaderGenerator _get_iterator for generator_asset: %s" % generator_asset)
        # If the generator_asset is a file, then return the path.
        # Otherwise, use files in a subdir as batches
        if os.path.isdir(os.path.join(self.base_directory, generator_asset)):
            subdir_options = os.listdir(os.path.join(self.base_directory, generator_asset))
            batches = []
            for file_option in subdir_options:
                for extension in self.known_extensions:
                    if file_option.endswith(extension) and not file_option.startswith("."):
                        batches.append(os.path.join(self.base_directory, generator_asset, file_option))

            return self._build_batch_kwargs_path_iter(batches, reader_options=reader_options, limit=limit)
        else:
            for extension in self.known_extensions:
                path = os.path.join(self.base_directory, generator_asset + extension)
                if os.path.isfile(path):
                    return iter([
                        self._build_batch_kwargs_from_path(path, reader_options=reader_options, limit=limit)
                    ])
            # If we haven't returned yet, raise
            raise BatchKwargsError("No valid files found when searching {:s} using configured known_extensions: "
                                   "{:s} ".format(os.path.join(self.base_directory, generator_asset),
                                                  ', '.join(map(str, self.known_extensions))),
                                   batch_kwargs=PathBatchKwargs(
                                       path=os.path.join(self.base_directory, generator_asset))
                                   )

    def _build_batch_kwargs_path_iter(self, path_list, reader_options=None, limit=None):
        for path in path_list:
            yield self._build_batch_kwargs_from_path(path, reader_options=reader_options, limit=limit)

    def _build_batch_kwargs_from_path(self, path, reader_options=None, limit=None, partition_id=None):
        # We could add MD5 (e.g. for smallish files)
        # but currently don't want to assume the extra read is worth it
        # unless it's configurable
        # with open(path,'rb') as f:
        #     md5 = hashlib.md5(f.read()).hexdigest()
        batch_kwargs = PathBatchKwargs({
            "path": path
        })
        computed_partition_id = self._partitioner(path)
        if partition_id and computed_partition_id:
            if partition_id != computed_partition_id:
                logger.warning("Provided partition_id does not match computed partition_id; consider explicitly "
                               "defining the asset or updating your partitioner.")
            batch_kwargs["partition_id"] = partition_id
        elif partition_id:
            batch_kwargs["partition_id"] = partition_id
        elif computed_partition_id:
            batch_kwargs["partition_id"] = computed_partition_id

        # Apply globally-configured reader options first
        batch_kwargs['reader_options'] = self.reader_options
        if reader_options:
            # Then update with any locally-specified reader options
            batch_kwargs['reader_options'].update(reader_options)

        if limit is not None:
            batch_kwargs['limit'] = limit

        return batch_kwargs

    # noinspection PyMethodMayBeStatic
    def _partitioner(self, path):
        return os.path.basename(path).rpartition(".")[0]
