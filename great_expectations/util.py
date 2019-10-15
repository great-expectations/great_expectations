import os
import importlib
import json
import logging

from six import string_types

logger = logging.getLogger(__name__)


def load_class(class_name, module_name):
    # Get the class object itself from strings.
    loaded_module = importlib.import_module(module_name)
    try:
        class_ = getattr(loaded_module, class_name)
    except AttributeError as e:
        raise AttributeError("Module : {} has no class named : {}".format(
            module_name,
            class_name,
        ))
    return class_


def _convert_to_dataset_class(
        df,
        dataset_class,
        expectation_suite=None,
        profiler=None):
    """
    Convert a (pandas) dataframe to a great_expectations dataset, with (optional) expectation_suite

    Args:
        df: the DataFrame object to convert
        dataset_class: the class to which to convert the existing DataFrame
        expectation_suite: the expectation suite that should be attached to the resulting dataset
        profiler: the profiler to use to generate baseline expectations, if any

    Returns:
        A new Dataset object
    """

    if expectation_suite is not None:
        # Create a dataset of the new class type, and manually initialize expectations according to
        # the provided expectation suite
        new_df = dataset_class.from_dataset(df)
        new_df._initialize_expectations(expectation_suite)
    else:
        # Instantiate the new Dataset with default expectations
        new_df = dataset_class.from_dataset(df)
        if profiler is not None:
            new_df.profile(profiler)

    return new_df


def _load_and_convert_to_dataset_class(df, class_name, module_name, expectation_suite=None, profiler=None):
    """
    Convert a (pandas) dataframe to a great_expectations dataset, with (optional) expectation_suite

    Args:
        df: the DataFrame object to convert
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        expectation_suite: the expectation suite that should be attached to the resulting dataset
        profiler: the profiler to use to generate baseline expectations, if any

    Returns:
        A new Dataset object
    """
    dataset_class = load_class(class_name, module_name)
    return _convert_to_dataset_class(df, dataset_class, expectation_suite, profiler)


def read_csv(
    filename,
    class_name="PandasDataset",
    module_name="great_expectations.dataset",
    dataset_class=None,
    expectation_suite=None,
    profiler=None,
    *args, **kwargs
):
    """Read a file using Pandas read_csv and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        dataset_class (Dataset): If specified, the class to which to convert the resulting Dataset object;
            if not specified, try to load the class named via the class_name and module_name parameters
        expectation_suite (string): path to great_expectations expectation suite file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset
    """
    import pandas as pd

    df = pd.read_csv(filename, *args, **kwargs)
    if dataset_class is not None:
        return _convert_to_dataset_class(
            df=df,
            dataset_class=dataset_class,
            expectation_suite=expectation_suite,
            profiler=profiler
        )
    else:
        return _load_and_convert_to_dataset_class(
            df=df,
            class_name=class_name,
            module_name=module_name,
            expectation_suite=expectation_suite,
            profiler=profiler
        )


def read_json(
    filename,
    class_name="PandasDataset",
    module_name="great_expectations.dataset",
    dataset_class=None,
    expectation_suite=None,
    accessor_func=None,
    profiler=None,
    *args, **kwargs
):
    """Read a file using Pandas read_json and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        dataset_class (Dataset): If specified, the class to which to convert the resulting Dataset object;
            if not specified, try to load the class named via the class_name and module_name parameters
        expectation_suite (string): path to great_expectations expectation suite file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset
    """
    import pandas as pd

    if accessor_func is not None:
        json_obj = json.load(open(filename, 'rb'))
        json_obj = accessor_func(json_obj)
        df = pd.read_json(json.dumps(json_obj), *args, **kwargs)

    else:
        df = pd.read_json(filename, *args, **kwargs)

    if dataset_class is not None:
        return _convert_to_dataset_class(
            df=df,
            dataset_class=dataset_class,
            expectation_suite=expectation_suite,
            profiler=profiler
        )
    else:
        return _load_and_convert_to_dataset_class(
            df=df,
            class_name=class_name,
            module_name=module_name,
            expectation_suite=expectation_suite,
            profiler=profiler
        )

def read_excel(
    filename,
    class_name="PandasDataset",
    module_name="great_expectations.dataset",
    dataset_class=None,
    expectation_suite=None,
    profiler=None,
    *args, **kwargs
):
    """Read a file using Pandas read_excel and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        dataset_class (Dataset): If specified, the class to which to convert the resulting Dataset object;
            if not specified, try to load the class named via the class_name and module_name parameters
        expectation_suite (string): path to great_expectations expectation suite file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset or ordered dict of great_expectations datasets,
        if multiple worksheets are imported
    """
    import pandas as pd

    df = pd.read_excel(filename, *args, **kwargs)
    if dataset_class is None:
        dataset_class = load_class(class_name=class_name, module_name=module_name)
    if isinstance(df, dict):
        for key in df:
            df[key] = _convert_to_dataset_class(
                df=df[key],
                dataset_class=dataset_class,
                expectation_suite=expectation_suite,
                profiler=profiler)
    else:
        df = _convert_to_dataset_class(
            df=df,
            dataset_class=dataset_class,
            expectation_suite=expectation_suite,
            profiler=profiler)
    return df


def read_table(
    filename,
    class_name="PandasDataset",
    module_name="great_expectations.dataset",
    dataset_class=None,
    expectation_suite=None,
    profiler=None,
    *args, **kwargs
):
    """Read a file using Pandas read_table and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        dataset_class (Dataset): If specified, the class to which to convert the resulting Dataset object;
            if not specified, try to load the class named via the class_name and module_name parameters
        expectation_suite (string): path to great_expectations expectation suite file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset
    """
    import pandas as pd

    df = pd.read_table(filename, *args, **kwargs)
    if dataset_class is not None:
        return _convert_to_dataset_class(
            df=df,
            dataset_class=dataset_class,
            expectation_suite=expectation_suite,
            profiler=profiler
        )
    else:
        return _load_and_convert_to_dataset_class(
            df=df,
            class_name=class_name,
            module_name=module_name,
            expectation_suite=expectation_suite,
            profiler=profiler
        )


def read_parquet(
    filename,
    class_name="PandasDataset",
    module_name="great_expectations.dataset",
    dataset_class=None,
    expectation_suite=None,
    profiler=None,
    *args, **kwargs
):
    """Read a file using Pandas read_parquet and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        dataset_class (Dataset): If specified, the class to which to convert the resulting Dataset object;
            if not specified, try to load the class named via the class_name and module_name parameters
        expectation_suite (string): path to great_expectations expectation suite file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset
    """
    import pandas as pd

    df = pd.read_parquet(filename, *args, **kwargs)
    if dataset_class is not None:
        return _convert_to_dataset_class(
            df=df,
            dataset_class=dataset_class,
            expectation_suite=expectation_suite,
            profiler=profiler
        )
    else:
        return _load_and_convert_to_dataset_class(
            df=df,
            class_name=class_name,
            module_name=module_name,
            expectation_suite=expectation_suite,
            profiler=profiler)


def from_pandas(pandas_df,
                class_name="PandasDataset",
                module_name="great_expectations.dataset",
                dataset_class=None,
                expectation_suite=None,
                profiler=None
                ):
    """Read a Pandas data frame and return a great_expectations dataset.

    Args:
        pandas_df (Pandas df): Pandas data frame
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        dataset_class (Dataset): If specified, the class to which to convert the resulting Dataset object;
            if not specified, try to load the class named via the class_name and module_name parameters
        expectation_suite (string) = None: path to great_expectations expectation suite file
        profiler (profiler class) = None: The profiler that should 
            be run on the dataset to establish a baseline expectation suite.

    Returns:
        great_expectations dataset
    """
    if dataset_class is not None:
        return _convert_to_dataset_class(
            df=pandas_df,
            dataset_class=dataset_class,
            expectation_suite=expectation_suite,
            profiler=profiler
        )
    else:
        return _load_and_convert_to_dataset_class(
            df=pandas_df,
            class_name=class_name,
            module_name=module_name,
            expectation_suite=expectation_suite,
            profiler=profiler
        )


def validate(
        data_asset,
        expectation_suite=None,
        data_asset_name=None,
        expectation_suite_name=None,
        data_context=None,
        data_asset_class_name=None,
        data_asset_module_name="great_expectations.dataset",
        data_asset_class=None,
        *args, **kwargs):
    """Validate the provided data asset. Validate can accept an optional data_asset_name to apply, data_context to use
    to fetch an expectation_suite if one is not provided, and data_asset_class_name/data_asset_module_name or
    data_asset_class to use to provide custom expectations.

    Args:
        data_asset: the asset to validate
        expectation_suite: the suite to use, or None to fetch one using a DataContext
        data_asset_name: the name of the data asset to use
        expectation_suite_name: the name of the expectation_suite to use
        data_context: data context to use to fetch an an expectation suite, or the path from which to obtain one
        data_asset_class_name: the name of a class to dynamically load a DataAsset class
        data_asset_module_name: the name of the module to dynamically load a DataAsset class
        data_asset_class: a class to use. overrides data_asset_class_name/ data_asset_module_name if provided
        *args:
        **kwargs:

    Returns:

    """
    # Get an expectation suite if not provided
    if expectation_suite is None and data_context is None:
        raise ValueError(
            "Either an expectation suite or a DataContext is required for validation.")

    if expectation_suite is None:
        logger.info("Using expectation suite from DataContext.")
        # Allow data_context to be a string, and try loading it from path in that case
        if isinstance(data_context, string_types):
            from great_expectations.data_context import DataContext
            data_context = DataContext(data_context)
        expectation_suite = data_context.get_expectation_suite(
            data_asset_name=data_asset_name,
            expectation_suite_name=expectation_suite_name
        )
    else:
        data_asset_name = expectation_suite.get("data_asset_name", data_asset_name)
        expectation_suite["data_asset_name"] = data_asset_name
        expectation_suite_name = expectation_suite.get("expectation_suite_name", expectation_suite_name)
        expectation_suite["expectation_suite_name"] = expectation_suite_name
        logger.info(
            "Validating data_asset_name %s with expectation_suite_name %s" % (data_asset_name, expectation_suite_name)
        )

    # If the object is already a DataAsset type, then this is purely a convenience method
    # and no conversion is needed; try to run validate on the given object
    if data_asset_class_name is None and data_asset_class is None:
        return data_asset.validate(expectation_suite=expectation_suite, data_context=data_context, *args, **kwargs)

    # Otherwise, try to convert and validate the dataset
    if data_asset_class is None:
        data_asset_class = load_class(data_asset_class_name, data_asset_module_name)

    import pandas as pd
    from great_expectations.dataset import Dataset, PandasDataset
    if data_asset_class is None:
        # Guess the GE data_asset_type based on the type of the data_asset
        if isinstance(data_asset, pd.DataFrame):
            data_asset_class = PandasDataset
        # Add other data_asset_type conditions here as needed

    # Otherwise, we will convert for the user to a subclass of the
    # existing class to enable new expectations, but only for datasets
    if not isinstance(data_asset, (Dataset, pd.DataFrame)):
        raise ValueError(
            "The validate util method only supports dataset validations, including custom subclasses. For other data "
            "asset types, use the object's own validate method."
        )

    if not issubclass(type(data_asset), data_asset_class):
        if isinstance(data_asset, pd.DataFrame) and issubclass(data_asset_class, PandasDataset):
            pass  # This is a special type of allowed coercion
        else:
            raise ValueError(
                "The validate util method only supports validation for subtypes of the provided data_asset_type.")

    data_asset_ = _convert_to_dataset_class(
        data_asset, dataset_class=data_asset_class, expectation_suite=expectation_suite, profiler=None)
    return data_asset_.validate(*args, data_context=data_context, **kwargs)


# https://stackoverflow.com/questions/9727673/list-directory-tree-structure-in-python
def gen_directory_tree_str(startpath):
    """Print the structure of directory as a tree:

    Ex:
    project_dir0/
        AAA/
        BBB/
            aaa.txt
            bbb.txt

    #Note: files and directories are sorted alphabetically, so that this method can be used for testing.
    """

    output_str = ""

    tuples = list(os.walk(startpath))
    tuples.sort()

    for root, dirs, files in tuples:
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        output_str += '{}{}/\n'.format(indent, os.path.basename(root))
        subindent = ' ' * 4 * (level + 1)

        files.sort()
        for f in files:
            output_str += '{}{}\n'.format(subindent, f)
    
    return output_str


def file_relative_path(dunderfile, relative_path):
    """
    This function is useful when one needs to load a file that is
    relative to the position of the current file. (Such as when
    you encode a configuration file path in source file and want
    in runnable in any current working directory)

    It is meant to be used like the following:
    file_relative_path(__file__, 'path/relative/to/file')

    H/T https://github.com/dagster-io/dagster/blob/8a250e9619a49e8bff8e9aa7435df89c2d2ea039/python_modules/dagster/dagster/utils/__init__.py#L34
    """
    return os.path.join(os.path.dirname(dunderfile), relative_path)
