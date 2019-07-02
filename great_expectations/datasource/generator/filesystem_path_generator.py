import os
import time
import glob

from great_expectations.datasource.generator.batch_generator import BatchGenerator
from great_expectations.exceptions import BatchKwargsError

KNOWN_EXTENSIONS = ['.csv', '.tsv', '.parquet', '.xls', '.xlsx', '.json']


class GlobReaderGenerator(BatchGenerator):
    def __init__(self, name="default",
                 datasource=None,
                 base_directory="/data",
                 reader_options=None,
                 asset_globs=None):
        super(GlobReaderGenerator, self).__init__(name, type_="glob_reader", datasource=datasource)
        if reader_options is None:
            reader_options = {}
        
        if asset_globs is None:
            asset_globs = {
                "default": "*"
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
            batch_paths = glob.glob(os.path.join(self.base_directory, self.asset_globs[generator_asset]))
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

        glob_ = self.asset_globs[generator_asset]
        paths = glob.glob(os.path.join(self.base_directory, glob_))
        return self._build_batch_kwargs_path_iter(paths)

    def _build_batch_kwargs_path_iter(self, path_list):
        for path in path_list:
            yield self._build_batch_kwargs(path)

    def _build_batch_kwargs(self, path):
        # We could add MD5 (e.g. for smallish files)
        # but currently don't want to assume the extra read is worth it
        # unless it's configurable
        # with open(path,'rb') as f:
        #     md5 = hashlib.md5(f.read()).hexdigest()
        batch_kwargs = {
            "path": path,
            "timestamp": time.time()
        }
        batch_kwargs.update(self.reader_options)
        return batch_kwargs


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
                    subdir_options = self._get_valid_file_options(valid_options=set(), base_directory=os.path.join(base_directory, file_option))
                    if len(subdir_options) > 0:
                        valid_options.add(file_option)
                # Make sure there's at least one valid file inside the subdir
        return valid_options

    def _get_iterator(self, generator_asset, **kwargs):
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
        # ONLY allow KNOWN_EXPTENSIONS
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
        batch_kwargs = {
            "path": path,
            "timestamp": time.time()
        }
        batch_kwargs.update(self.reader_options)
        return batch_kwargs
