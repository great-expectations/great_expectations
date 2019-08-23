import os
import time
import logging

from great_expectations.datasource.generator.batch_generator import BatchGenerator
from great_expectations.datasource.types import PathBatchKwargs

logger = logging.getLogger(__name__)

KNOWN_EXTENSIONS = ['.csv', '.tsv', '.parquet', '.xls', '.xlsx', '.json']


class SubdirReaderGenerator(BatchGenerator):
    """The SubdirReaderGenerator inspects a filesytem and produces batch_kwargs with a path and timestamp.

    SubdirReaderGenerator recognizes generator_asset using two criteria:
      - for files directly in 'base_directory' with recognized extensions (.csv, .tsv, .parquet, .xls, .xlsx, .json),
        it uses the name of the file without the extension
      - for other files or directories in 'base_directory', is uses the file or directory name

    SubdirReaderGenerator sees all files inside a directory of base_directory as batches of one datasource.

    SubdirReaderGenerator can also include configured reader_options which will be added to batch_kwargs generated
    by this generator.
    """

    def __init__(self, name="default",
                 datasource=None,
                 base_directory="/data",
                 reader_options=None):
        super(SubdirReaderGenerator, self).__init__(name, type_="subdir_reader", datasource=datasource)
        if reader_options is None:
            reader_options = {}

        self._reader_options = reader_options
        self._base_directory = base_directory

    @property
    def reader_options(self):
        return self._reader_options

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
            return set()
        known_assets = self._get_valid_file_options(valid_options=set(), base_directory=self.base_directory)
        return known_assets

    def _get_valid_file_options(self, valid_options=set(), base_directory=None):
        if base_directory is None:
            base_directory = self.base_directory
        file_options = os.listdir(base_directory)
        for file_option in file_options:
            for extension in KNOWN_EXTENSIONS:
                if file_option.endswith(extension) and not file_option.startswith("."):
                    valid_options.add(file_option[:-len(extension)])
                elif os.path.isdir(os.path.join(self.base_directory, file_option)):
                    subdir_options = self._get_valid_file_options(valid_options=set(),
                                                                  base_directory=os.path.join(base_directory,
                                                                                              file_option))
                    if len(subdir_options) > 0:
                        valid_options.add(file_option)
                # Make sure there's at least one valid file inside the subdir
        return valid_options

    def _get_iterator(self, generator_asset, **kwargs):
        logger.debug("Beginning SubdirReaderGenerator _get_iterator for generator_asset: %s" % generator_asset)
        # If the generator_asset is a file, then return the path.
        # Otherwise, use files in a subdir as batches
        if os.path.isdir(os.path.join(self.base_directory, generator_asset)):
            subdir_options = os.listdir(os.path.join(self.base_directory, generator_asset))
            batches = []
            for file_option in subdir_options:
                for extension in KNOWN_EXTENSIONS:
                    if file_option.endswith(extension) and not file_option.startswith("."):
                        batches.append(os.path.join(self.base_directory, generator_asset, file_option))

            return self._build_batch_kwargs_path_iter(batches)
            # return self._build_batch_kwargs_path_iter(os.scandir(os.path.join(self.base_directory, generator_asset)))
            # return iter([{
            #     "path": os.path.join(self.base_directory, generator_asset, x)
            # } for x in os.listdir(os.path.join(self.base_directory, generator_asset))])
        # ONLY allow KNOWN_EXTENSIONS
        # elif os.path.isfile(os.path.join(self.base_directory, generator_asset)):
        #     path = os.path.join(self.base_directory, generator_asset)

        #     return iter([self._build_batch_kwargs(path)])
        else:
            for extension in KNOWN_EXTENSIONS:
                path = os.path.join(self.base_directory, generator_asset + extension)
                if os.path.isfile(path):
                    return iter([
                        self._build_batch_kwargs(path)
                    ])
        # If we haven't returned yet, raise
        raise IOError(os.path.join(self.base_directory, generator_asset))

    # def _build_batch_kwargs_path_iter(self, path_iter):
    def _build_batch_kwargs_path_iter(self, path_list):
        for path in path_list:
            yield self._build_batch_kwargs(path)
        # Use below if we have an iterator (e.g. from scandir)
        # try:
        #     while True:
        #         yield {
        #             "path": next(path_iter).path
        #         }
        # except StopIteration:
        #     return

    def _build_batch_kwargs(self, path):
        # We could add MD5 (e.g. for smallish files)
        # but currently don't want to assume the extra read is worth it
        # unless it's configurable
        # with open(path,'rb') as f:
        #     md5 = hashlib.md5(f.read()).hexdigest()
        batch_kwargs = PathBatchKwargs({
            "path": path,
            "timestamp": time.time()
        })
        partition_id = self._partitioner(path)
        if partition_id is not None:
            batch_kwargs.update({"partition_id": partition_id})
        batch_kwargs.update(self.reader_options)
        return batch_kwargs

    @staticmethod
    def _partitioner(path):
        return os.path.basename(path).rpartition(".")[0]
