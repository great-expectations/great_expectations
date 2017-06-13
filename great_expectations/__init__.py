import pandas as pd


from .util import *
import dataset

from pkg_resources import get_distribution
try:
    __version__ = get_distribution('great_expectations').version
except:
    pass

def list_sources():
    raise NotImplementedError

def connect_to_datasource():
    raise NotImplementedError

def connect_to_dataset():
    raise NotImplementedError

def read_csv(filename, dataset_config=None, *args, **kwargs):
    df = pd.read_csv(filename, *args, **kwargs)
    df.__class__ = dataset.pandas_dataset.PandasDataSet
    df.initialize_expectations(dataset_config)

    return df

def df(df, dataset_config=None, *args, **kwargs):
    df.__class__ = dataset.pandas_dataset.PandasDataSet
    df.initialize_expectations(dataset_config)

    return df

def expect(data_source_str, expectation):
    raise NotImplementedError
