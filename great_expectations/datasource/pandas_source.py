import os
import time

import pandas as pd

from .datasource import Datasource
from .filesystem_path_generator import FilesystemPathGenerator
from great_expectations.dataset.pandas_dataset import PandasDataset

from great_expectations.exceptions import BatchKwargsError

class PandasCSVDatasource(Datasource):
    """
    A PandasDataSource makes it easy to create, manage and validate expectations on
    Pandas dataframes.

    Use with the FilesystemPathGenerator for simple cases.
    """

    def __init__(self, name="default", data_context=None, generators=None, read_csv_kwargs=None, **kwargs):
        if generators is None:
            # Provide a gentle way to build a datasource with a sane default, including ability to specify the base_directory
            base_directory = kwargs.pop("base_directory", "/data")
            generators = {
                "default": {"type": "filesystem", "base_directory": base_directory}
        }
        super(PandasCSVDatasource, self).__init__(name, type_="pandas", data_context=data_context, generators=generators)
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
        try:
            full_path = os.path.join(batch_kwargs["path"])
        except KeyError:
            raise BatchKwargsError("Invalid batch_kwargs: path is required for a PandasCSVDatasource", batch_kwargs)
        
        all_kwargs = dict(**self._datasource_config["read_csv_kwargs"])
        all_kwargs.update(**kwargs)

        df = pd.read_csv(full_path, **all_kwargs)
        
        return PandasDataset(df, 
            expectations_config=expectations_config, 
            data_context=self._data_context, 
            data_asset_name=data_asset_name, 
            batch_kwargs=batch_kwargs)

    def build_batch_kwargs(self, filepath, **kwargs):
        batch_kwargs = {
            "path": filepath,
            "timestamp": time.time()
        }
        batch_kwargs.update(dict(**kwargs))
        return batch_kwargs