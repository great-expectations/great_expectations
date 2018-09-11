import pandas as pd
import json

import great_expectations.dataset as dataset

def _convert_to_dataset_class(df, dataset_class, expectations_config=None):
    """
    Convert a (pandas) dataframe to a great_expectations dataset, with (optional) expectations_config
    """
    if expectations_config is not None:
        # Cast the dataframe into the new class, and manually initialize expectations according to the provided configuration
        df.__class__ = dataset_class
        df._initialize_expectations(expectations_config)
    else:
        # Instantiate the new Dataset with default expectations
        try:
            df = dataset_class(df)
        except:
            raise NotImplementedError("read_csv requires a Dataset class that can be instantiated from a Pandas DataFrame")

    return df


def read_csv(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataset,
    expectations_config=None,
    *args, **kwargs
):
    df = pd.read_csv(filename, *args, **kwargs)
    df = _convert_to_dataset_class(df, dataset_class, expectations_config)
    return df


def read_json(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataset,
    expectations_config=None,
    accessor_func=None,
    *args, **kwargs
):
    if accessor_func != None:
        json_obj = json.load(open(filename, 'rb'))
        json_obj = accessor_func(json_obj)
        df = pd.read_json(json.dumps(json_obj), *args, **kwargs)

    else:
        df = pd.read_json(filename, *args, **kwargs)

    df = _convert_to_dataset_class(df, dataset_class, expectations_config)
    return df

def read_excel(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataset,
    expectations_config=None,
    *args, **kwargs
):
    """Read a file using Pandas read_excel and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        dataset_class (Dataset class): class to which to convert resulting Pandas df
        expectations_config (string): path to great_expectations config file

    Returns:
        great_expectations dataset or ordered dict of great_expectations datasets,
        if multiple worksheets are imported
    """
    df = pd.read_excel(filename, *args, **kwargs)
    if isinstance(df, dict):
        for key in df:
            df[key] = _convert_to_dataset_class(df[key], dataset_class, expectations_config)
    else:
        df = _convert_to_dataset_class(df, dataset_class, expectations_config)
    return df


def read_table(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataset,
    expectations_config=None,
    *args, **kwargs
):
    """Read a file using Pandas read_table and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        dataset_class (Dataset class): class to which to convert resulting Pandas df
        expectations_config (string): path to great_expectations config file

    Returns:
        great_expectations dataset
    """
    df = pd.read_table(filename, *args, **kwargs)
    df = _convert_to_dataset_class(df, dataset_class, expectations_config)
    return df


def read_parquet(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataset,
    expectations_config=None,
    *args, **kwargs
):
    """Read a file using Pandas read_parquet and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        dataset_class (Dataset class): class to which to convert resulting Pandas df
        expectations_config (string): path to great_expectations config file

    Returns:
        great_expectations dataset
    """
    df = pd.read_parquet(filename, *args, **kwargs)
    df = _convert_to_dataset_class(df, dataset_class, expectations_config)
    return df


def from_pandas(pandas_df, expectations_config=None):
    return _convert_to_dataset_class(
        pandas_df,
        dataset.pandas_dataset.PandasDataset,
        expectations_config
    )


def validate(df, expectations_config, *args, **kwargs):
    #FIXME: I'm not sure that this should always default to PandasDataset
    dataset_ = _convert_to_dataset_class(df,
        dataset.pandas_dataset.PandasDataset,
        expectations_config
    )
    return dataset_.validate(*args, **kwargs)


class DotDict(dict):
    """dot.notation access to dictionary attributes"""
    def __getattr__(self, attr):
        return self.get(attr)
    __setattr__= dict.__setitem__
    __delattr__= dict.__delitem__
    def __dir__(self):
        return self.keys()
