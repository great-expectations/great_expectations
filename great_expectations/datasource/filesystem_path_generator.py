import os
import time
import re

from .batch_generator import BatchGenerator


class SubdirReaderGenerator(BatchGenerator):
    """The SubdirReaderGenerator inspects a filesytem and produces batch_kwargs with a path and timestamp.

    SubdirReaderGenerator recognizes data_asset_name using two criteria:
      - for files directly in 'base_directory' with recognized extensions (.csv), it uses the name of the file without
        the extension
      - for other files or directories in 'base_directory', is uses the file or directory name

    For directories in 'base_directory', SubdirReaderGenerator iterates over

    SubdirReaderGenerator also uses
    SubdirReaderGenerator
    /data/users/users_20180101.csv
    /data/users/users_20180102.csv
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
            return os.path.join(self._datasource.get_data_context().get_context_root_directory(), self._base_directory)

    def get_available_data_asset_names(self):
        known_assets = set()
        file_options = os.listdir(self.base_directory)
        for file_option in file_options:
            if file_option.endswith(".csv"):
                known_assets.add(file_option[:-4])
            else:
                known_assets.add(file_option)
        return known_assets

    def _get_iterator(self, data_asset_name, **kwargs):
        # If the data_asset_name is a file, then return the path.
        # Otherwise, use files in a subdir as batches
        if os.path.isdir(os.path.join(self.base_directory, data_asset_name)):
            return self._build_batch_kwargs_path_iter(
                [
                    os.path.join(self.base_directory, data_asset_name, path)
                    for path in os.listdir(os.path.join(self.base_directory, data_asset_name))
                ]
            )
            # return self._build_batch_kwargs_path_iter(os.scandir(os.path.join(self.base_directory, data_asset_name)))
            # return iter([{
            #     "path": os.path.join(self.base_directory, data_asset_name, x)
            # } for x in os.listdir(os.path.join(self.base_directory, data_asset_name))])
        elif os.path.isfile(os.path.join(self.base_directory, data_asset_name)):
            path = os.path.join(self.base_directory, data_asset_name)
            # with open(path,'rb') as f:
            #     md5 = hashlib.md5(f.read()).hexdigest()
            return iter([self._build_batch_kwargs(path)])
        elif os.path.isfile(os.path.join(self.base_directory, data_asset_name + ".csv")):
            path = os.path.join(self.base_directory, data_asset_name + ".csv")
            # with open(path,'rb') as f:
            #     md5 = hashlib.md5(f.read()).hexdigest()
            return iter([self._build_batch_kwargs(path)])
        else:
            raise IOError(os.path.join(self.base_directory, data_asset_name))

    # def _build_batch_kwargs_path_iter(self, path_iter):
    def _build_batch_kwargs_path_iter(self, path_list):
        for path in path_list:
            # with open(path,'rb') as f:
            #     md5 = hashlib.md5(f.read()).hexdigest()
            yield self._build_batch_kwargs(path)
        # try:
        #     while True:
        #         yield {
        #             "path": next(path_iter).path
        #         }
        # except StopIteration:
        #     return

    def _build_batch_kwargs(self, path):
        batch_kwargs = {
            "path": path,
            "timestamp": time.time()
        }
        batch_kwargs.update(self.reader_options)
        return batch_kwargs
