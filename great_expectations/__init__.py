import pandas as pd


from .util import *
from great_expectations import dataset

from .version import __version__

def list_sources():
    raise NotImplementedError

def connect_to_datasource():
    raise NotImplementedError

def connect_to_dataset():
    raise NotImplementedError

def read_csv(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataSet,
    expectations_config=None,
    *args, **kwargs
):
    df = pd.read_csv(filename, *args, **kwargs)
    if expectations_config is not None:
        # Cast the dataframe into the new class, and manually initialize expectations according to the provided configuration
        df.__class__ = dataset_class
        df.initialize_expectations(expectations_config)
    else:
        # Instantiate the new DataSet with default expectations
        try:
            df = dataset_class(df)
        except:
            raise NotImplementedError("read_csv requires a DataSet class that can be instantiated from a Pandas DataFrame")
    return df

# Removed. Preferred solution will be to use ge.dataset.PandasDataSet(df) instead.
# def df(df, dataset_config=None, *args, **kwargs):
#     df.__class__ = dataset.pandas_dataset.PandasDataSet
#     df.initialize_expectations(dataset_config)
#
#     return df

def expect(data_source_str, expectation):
    raise NotImplementedError
