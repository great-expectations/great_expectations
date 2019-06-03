import os

from .batch_generator import BatchGenerator

class FilesystemPathGenerator(BatchGenerator):
    """
    /data/users/users_20180101.csv
    /data/users/users_20180102.csv
    """

    def __init__(self, name="default", datasource=None, base_directory="/data"):
        super(FilesystemPathGenerator, self).__init__(name, type_="filesystem", datasource=datasource)
        self._base_directory = base_directory

    def list_data_asset_names(self):
        known_assets = []
        file_options = os.listdir(self._get_current_base_directory())
        for file_option in file_options:
            if file_option.endswith(".csv"):
                known_assets.append(file_option[:-4])
            else:
                known_assets.append(file_option)
        return known_assets

    def _get_iterator(self, data_asset_name):
        # If the data_asset_name is a file, then return the path.
        # Otherwise, use files in a subdir as batches
        if os.path.isdir(os.path.join(self._get_current_base_directory(), data_asset_name)):
            return self._build_batch_kwargs_path_iter(os.scandir(os.path.join(self._get_current_base_directory(), data_asset_name)))
        elif os.path.isfile(os.path.join(self._get_current_base_directory(), data_asset_name)):
            return iter([
                {
                    "path": os.path.join(self._get_current_base_directory(), data_asset_name)
                }
                ])
        elif os.path.isfile(os.path.join(self._get_current_base_directory(), data_asset_name + ".csv")):
            return iter([
                {
                    "path": os.path.join(self._get_current_base_directory(), data_asset_name + ".csv")
                }
                ])
        else:
            raise FileNotFoundError(os.path.join(self._base_directory, data_asset_name))

    def _build_batch_kwargs_path_iter(self, path_iter):
        try:
            while True:
                yield {
                    "path": next(path_iter).path
                }
        except StopIteration:
            return

    # If base directory is a relative path, interpret it as relative to the data context's
    # context root directory (parent directory of great_expectation dir)
    def _get_current_base_directory(self):
        if os.path.isabs(self._base_directory):
            return self._base_directory
        else:
            return os.path.join(self._datasource.get_data_context().get_context_root_directory(), self._base_directory)
