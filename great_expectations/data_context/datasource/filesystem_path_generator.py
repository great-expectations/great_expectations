import os

from .batch_generator import BatchGenerator

class FilesystemPathGenerator(BatchGenerator):
    """
    /data/users/users_20180101.csv
    /data/users/users_20180102.csv
    """

    def __init__(self, name, type_, datasource, base_directory="/data"):
        super(FilesystemPathGenerator, self).__init__(name, type_, datasource)
        self._base_directory = base_directory

    def list_data_asset_names(self):
        known_assets = []
        file_options = os.listdir(self._base_directory)
        for file_option in file_options:
            if file_option.endswith(".csv"):
                known_assets.append(file_option[:-4])
            else:
                known_assets.append(file_option)
        return known_assets

    def _get_iterator(self, data_asset_name):
        # If the data_asset_name is a file, then return the path.
        # Otherwise, use files in a subdir as batches
        if os.path.isdir(os.path.join(self._base_directory, data_asset_name)):
            return self._build_batch_kwargs_path_iter(os.scandir(os.path.join(self._base_directory, data_asset_name)))
        elif os.path.isfile(os.path.join(self._base_directory, data_asset_name)):
            return iter([
                {
                    "path": os.path.join(self._base_directory, data_asset_name)
                }
                ])
        elif os.path.isfile(os.path.join(self._base_directory, data_asset_name + ".csv")):
            return iter([
                {
                    "path": os.path.join(self._base_directory, data_asset_name + ".csv")
                }
                ])
        else:
            return iter([{}])

    def _build_batch_kwargs_path_iter(self, path_iter):
        try:
            while True:
                yield {
                    "path": next(path_iter).path
                }
        except StopIteration:
            return
