import os
import time

import pandas as pd

from .datasource import Datasource
from .filesystem_path_generator import SubdirPathGenerator
from .batch_generator import EmptyGenerator
from great_expectations.dataset.pandas_dataset import PandasDataset

from great_expectations.exceptions import BatchKwargsError


class MemoryPandasDatasource(Datasource):
    def __init__(self, name="default", data_context=None, generators=None):
        if generators is None:
            generators = {
                "default": {"type": "empty_generator"}
            }
        super(MemoryPandasDatasource, self).__init__(name, type_="memory_pandas",
                                                     data_context=data_context,
                                                     generators=generators)
        self._build_generators()

    def _get_generator_class(self, type_):
        if type_ == "empty_generator":
            return EmptyGenerator
        else:
            raise ValueError("Unrecognized BatchGenerator type %s" % type_)

    def _get_data_asset(self, data_asset_name, batch_kwargs, expectations_config, **kwargs):
        df = batch_kwargs.pop("df", None)
        
        return PandasDataset(df,
                             expectations_config=expectations_config,
                             data_context=self._data_context,
                             data_asset_name=data_asset_name,
                             batch_kwargs=batch_kwargs)

    def build_batch_kwargs(self, df, **kwargs):
        return {
            "df": df
        }


class FilesystemPandasDatasource(Datasource):
    """
    A FilesystemPandasDatasource makes it easy to create, manage and validate expectations on
    Pandas dataframes.

    Use with the SubdirPathGenerator for simple cases.
    """

    def __init__(self, name="default", data_context=None, generators=None, base_directory="/data", **kwargs):
        if generators is None:
            # Provide a gentle way to build a datasource with a sane default,
            # including ability to specify the base_directory
            reader_kwargs = kwargs.pop("reader_kwargs", {})
            generators = {
                "default": {"type": "subdir_reader", "reader_kwargs": reader_kwargs}
            }
        super(FilesystemPandasDatasource, self).__init__(name, type_="filesystem_pandas",
                                                         data_context=data_context,
                                                         generators=generators)
        self._datasource_config.update(
            {
                "base_directory": base_directory
            }
        )
        self._build_generators()

    @property
    def base_directory(self):
        return self._datasource_config["base_directory"]

    def _get_generator_class(self, type_):
        if type_ == "subdir_reader":
            return SubdirPathGenerator
        else:
            raise ValueError("Unrecognized BatchGenerator type %s" % type_)

    def _get_data_asset(self, data_asset_name, batch_kwargs, expectations_config, **kwargs):
        try:
            path = os.path.join(self.base_directory, batch_kwargs.pop("path"))
        except KeyError:
            raise BatchKwargsError("Invalid batch_kwargs: path is required for a FilesystemPandasDatasource",
                                   batch_kwargs)

        batch_kwargs.update(**kwargs)
        if path.endswith((".csv", ".tsv")):
            df = pd.read_csv(path, **batch_kwargs)
        elif path.endswith(".parquet"):
            df = pd.read_parquet(path, **batch_kwargs)
        elif path.endswith((".xls", ".xlsx")):
            df = pd.read_excel(path, **batch_kwargs)
        else:
            raise BatchKwargsError("Unrecognized path: no available reader.", batch_kwargs)
        
        return PandasDataset(df,
                             expectations_config=expectations_config,
                             data_context=self._data_context,
                             data_asset_name=data_asset_name,
                             batch_kwargs=batch_kwargs)

    def build_batch_kwargs(self, path, **kwargs):
        batch_kwargs = {
            "path": path,
            "timestamp": time.time()
        }
        batch_kwargs.update(dict(**kwargs))
        return batch_kwargs
