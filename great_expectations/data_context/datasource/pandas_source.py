import pandas as pd
import os

from .datasource import Datasource
from .filesystem_path_generator import FilesystemPathGenerator
from ...dataset.pandas_dataset import PandasDataset

class PandasCSVDatasource(Datasource):
    """
    A PandasDataSource makes it easy to create, manage and validate expectations on
    Pandas dataframes.

    Use with the FilesystemPathGenerator for simple cases.
    """

    def __init__(self, name, type_, data_context=None, generators=None, read_csv_kwargs=None, **kwargs):
        if generators is None:
            # Provide a gentle way to build a datasource with a sane default, including ability to specify the base_directory
            base_directory = kwargs.pop("base_directory", "/data")
            generators = {
                "default": {"type": "filesystem", "base_directory": base_directory}
        }
        super(PandasCSVDatasource, self).__init__(name, type_, data_context, generators)
        self._datasource_config.update(
            {
                "read_csv_kwargs": read_csv_kwargs or {}
            }
        )
        self._build_generators()

    def _get_generator_class(self, type_):
        if type_ == "filesystem":
            return FilesystemPathGenerator
        else:
            raise ValueError("Unrecognized BatchGenerator type %s" % type_)

    def _get_data_asset(self, data_asset_name, batch_kwargs, expectations_config, **kwargs):
        full_path = os.path.join(batch_kwargs["path"])
        df = pd.read_csv(full_path, **self._datasource_config["read_csv_kwargs"], **kwargs)
        
        return PandasDataset(df, 
            expectations_config=expectations_config, 
            data_context=self._data_context, 
            data_asset_name=data_asset_name, 
            batch_kwargs=batch_kwargs)
