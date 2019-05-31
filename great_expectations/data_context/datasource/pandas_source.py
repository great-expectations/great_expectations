import pandas as pd
import os

from .datasource import Datasource
from .batch_generator import BatchGenerator
from ...dataset.pandas_dataset import PandasDataset


class FilesystemPathGenerator(BatchGenerator):
    """
    /data/users/users_20180101.csv
    /data/users/users_20180102.csv
    """

    def __init__(self, name, type_, datasource):
        super(FilesystemPathGenerator, self).__init__(name, type_, datasource)
        self._base_directory = datasource._base_directory

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


class PandasCSVDatasource(Datasource):
    """
    A PandasDataSource makes it easy to create, manage and validate expectations on
    Pandas dataframes.

    Use with the FilesystemPathGenerator for simple cases.
    """

    def __init__(self, name, type_, data_context=None, generators=None, base_directory="/data", read_csv_kwargs=None):
        self._base_directory = base_directory
        if generators is None:
            generators = {
                "default": {"type": "filesystem"}
        }
        super(PandasCSVDatasource, self).__init__(name, type_, data_context, generators)
        self._datasource_config.update(
            {
                "base_directory": base_directory,
                "read_csv_kwargs": read_csv_kwargs or {}
            }
        )
        self._build_generators()

    def _get_generator_class(self, type_):
        if type_ == "filesystem":
            return FilesystemPathGenerator
        else:
            raise ValueError("Unrecognized DataAssetGenerator type %s" % type_)

    def _get_data_asset(self, data_asset_name, batch_kwargs, expectations_config):
        full_path = os.path.join(self._base_directory, batch_kwargs["path"])
        df = pd.read_csv(full_path, **self._datasource_config["read_csv_kwargs"])
        
        return PandasDataset(df, 
            expectations_config=expectations_config, 
            data_context=self._data_context, 
            data_asset_name=data_asset_name, 
            batch_kwargs=batch_kwargs)
