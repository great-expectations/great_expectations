import time
from six import string_types

import pandas as pd

from .datasource import Datasource, ReaderMethods
from great_expectations.datasource.generator.filesystem_path_generator import SubdirReaderGenerator, GlobReaderGenerator
from great_expectations.datasource.generator.in_memory_generator import InMemoryGenerator
from great_expectations.dataset.pandas_dataset import PandasDataset

from great_expectations.exceptions import BatchKwargsError


class PandasDatasource(Datasource):
    """The PandasDatasource produces PandasDataset objects and supports generators capable of 
    interacting with the local filesystem (the default subdir_reader generator), and from
    existing in-memory dataframes.
    """

    def __init__(self, name="pandas", data_context=None, data_asset_type="SqlAlchemyDataset", generators=None, **kwargs):
        if generators is None:
            # Provide a gentle way to build a datasource with a sane default,
            # including ability to specify the base_directory and reader_options
            base_directory = kwargs.pop("base_directory", "/data")
            # By default, use CSV sniffer to infer separator, which requires the python engine
            reader_options = kwargs.pop("reader_options", {
                "sep": None,
                "engine": "python"
            })
            generators = {
                "default": {
                    "type": "subdir_reader",
                    "base_directory": base_directory,
                    "reader_options": reader_options
                }
            }
        super(PandasDatasource, self).__init__(name, type_="pandas",
                                               data_context=data_context,
                                               data_asset_type=data_asset_type,
                                               generators=generators)
        self._build_generators()

    def _get_generator_class(self, type_):
        if type_ == "subdir_reader":
            return SubdirReaderGenerator
        elif type_ == "glob_reader":
            return GlobReaderGenerator
        elif type_ == "memory":
            return InMemoryGenerator
        else:
            raise ValueError("Unrecognized BatchGenerator type %s" % type_)

    def _get_data_asset(self, batch_kwargs, expectation_suite, **kwargs):
        batch_kwargs.update(kwargs)
        if "path" in batch_kwargs:
            reader_options = batch_kwargs.copy()
            path = reader_options.pop("path")  # We need to remove from the reader
            reader_options.pop("timestamp", "")    # ditto timestamp (but missing ok)

            reader_method = reader_options.pop("reader_method", None)
            if reader_method is None:
                reader_method = self._guess_reader_method_from_path(path)
                if reader_method is None:
                    raise BatchKwargsError("Unable to determine reader for path: %s" % path, batch_kwargs)
            else:
                try:
                    reader_method = ReaderMethods[reader_method]
                except KeyError:
                    raise BatchKwargsError("Unknown reader method: %s" % reader_method, batch_kwargs)

            if reader_method == ReaderMethods.CSV:
                df = pd.read_csv(path, **reader_options)
            elif reader_method == ReaderMethods.parquet:
                df = pd.read_parquet(path, **reader_options)
            elif reader_method == ReaderMethods.excel:
                df = pd.read_excel(path, **reader_options)
            elif reader_method == ReaderMethods.JSON:
                df = pd.read_json(path, **reader_options)
            else:
                raise BatchKwargsError("Unsupported reader: %s" % reader_method.name, batch_kwargs)

        elif "df" in batch_kwargs and isinstance(batch_kwargs["df"], (pd.DataFrame, pd.Series)):
            df = batch_kwargs.pop("df")  # We don't want to store the actual dataframe in kwargs
            batch_kwargs["PandasInMemoryDF"] = True
        else:
            raise BatchKwargsError("Invalid batch_kwargs: path or df is required for a PandasDatasource",
                                   batch_kwargs)

        return PandasDataset(df,
                             expectation_suite=expectation_suite,
                             data_context=self._data_context,
                             batch_kwargs=batch_kwargs)

    def build_batch_kwargs(self, *args, **kwargs):
        if len(args) > 0:
            if isinstance(args[0], (pd.DataFrame, pd.Series)):
                kwargs.update({
                    "df": args[0],
                    "timestamp": time.time()
                })
            elif isinstance(args[0], string_types):
                kwargs.update({
                    "path": args[0],
                    "timestamp": time.time()
                })
        else:
            kwargs.update({
                "timestamp": time.time()
            })
        return kwargs
