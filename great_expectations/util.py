import os

import pandas as pd
import json
import logging

from six import string_types

import great_expectations.dataset as dataset
from great_expectations.data_context import DataContext
from great_expectations.data_context.types.metrics import NamespaceAwareValidationMetric


logger = logging.getLogger(__name__)


def _convert_to_dataset_class(df, dataset_class, expectation_suite=None, profiler=None):
    """
    Convert a (pandas) dataframe to a great_expectations dataset, with (optional) expectation_suite
    """
    # TODO: Refactor this method to use the new ClassConfig (module_name and class_name convention).
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


def read_csv(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataset,
    expectation_suite=None,
    profiler=None,
    *args, **kwargs
):
    # TODO: Refactor this method to use the new ClassConfig (module_name and class_name convention).
    df = pd.read_csv(filename, *args, **kwargs)
    df = _convert_to_dataset_class(
        df, dataset_class, expectation_suite, profiler)
    return df


def read_json(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataset,
    expectation_suite=None,
    accessor_func=None,
    profiler=None,
    *args, **kwargs
):
    if accessor_func is not None:
        json_obj = json.load(open(filename, 'rb'))
        json_obj = accessor_func(json_obj)
        df = pd.read_json(json.dumps(json_obj), *args, **kwargs)

    else:
        df = pd.read_json(filename, *args, **kwargs)

    df = _convert_to_dataset_class(
        df, dataset_class, expectation_suite, profiler)
    return df


def read_excel(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataset,
    expectation_suite=None,
    profiler=None,
    *args, **kwargs
):
    """Read a file using Pandas read_excel and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        dataset_class (Dataset class): class to which to convert resulting Pandas df
        expectation_suite (string): path to great_expectations expectation suite file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset or ordered dict of great_expectations datasets,
        if multiple worksheets are imported
    """
    df = pd.read_excel(filename, *args, **kwargs)
    if isinstance(df, dict):
        for key in df:
            df[key] = _convert_to_dataset_class(
                df[key], dataset_class, expectation_suite, profiler)
    else:
        df = _convert_to_dataset_class(
            df, dataset_class, expectation_suite, profiler)
    return df


def read_table(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataset,
    expectation_suite=None,
    profiler=None,
    *args, **kwargs
):
    """Read a file using Pandas read_table and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        dataset_class (Dataset class): class to which to convert resulting Pandas df
        expectation_suite (string): path to great_expectations expectation suite file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset
    """
    df = pd.read_table(filename, *args, **kwargs)
    df = _convert_to_dataset_class(
        df, dataset_class, expectation_suite, profiler)
    return df


def read_parquet(
    filename,
    dataset_class=dataset.pandas_dataset.PandasDataset,
    expectation_suite=None,
    profiler=None,
    *args, **kwargs
):
    """Read a file using Pandas read_parquet and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        dataset_class (Dataset class): class to which to convert resulting Pandas df
        expectation_suite (string): path to great_expectations expectation suite file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset
    """
    df = pd.read_parquet(filename, *args, **kwargs)
    df = _convert_to_dataset_class(
        df, dataset_class, expectation_suite, profiler)
    return df


def from_pandas(pandas_df,
                dataset_class=dataset.pandas_dataset.PandasDataset,
                expectation_suite=None,
                profiler=None
                ):
    """Read a Pandas data frame and return a great_expectations dataset.

    Args:
        pandas_df (Pandas df): Pandas data frame
        dataset_class (Dataset class) = dataset.pandas_dataset.PandasDataset:
            class to which to convert resulting Pandas df
        expectation_suite (string) = None: path to great_expectations expectation suite file
        profiler (profiler class) = None: The profiler that should 
            be run on the dataset to establish a baseline expectation suite.

    Returns:
        great_expectations dataset
    """
    return _convert_to_dataset_class(
        pandas_df,
        dataset_class,
        expectation_suite,
        profiler
    )


def validate(data_asset, expectation_suite=None, data_asset_name=None, data_context=None, data_asset_type=None, *args, **kwargs):
    """Validate the provided data asset using the provided expectation suite"""
    if expectation_suite is None and data_context is None:
        raise ValueError(
            "Either an expectation suite or a DataContext is required for validation.")

    if expectation_suite is None:
        logger.info("Using expectation suite from DataContext.")
        # Allow data_context to be a string, and try loading it from path in that case
        if isinstance(data_context, string_types):
            data_context = DataContext(data_context)                
        expectation_suite = data_context.get_expectation_suite(data_asset_name)
    else:
        if data_asset_name in expectation_suite:
            logger.info("Using expectation suite with name %s" %
                        expectation_suite["data_asset_name"])
        else:
            logger.info("Using expectation suite with no data_asset_name")

    # If the object is already a Dataset type, then this is purely a convenience method
    # and no conversion is needed
    if isinstance(data_asset, dataset.Dataset) and data_asset_type is None:
        return data_asset.validate(expectation_suite=expectation_suite, data_context=data_context, *args, **kwargs)
    elif data_asset_type is None:
        # Guess the GE data_asset_type based on the type of the data_asset
        if isinstance(data_asset, pd.DataFrame):
            data_asset_type = dataset.PandasDataset
        # Add other data_asset_type conditions here as needed

    # Otherwise, we will convert for the user to a subclass of the
    # existing class to enable new expectations, but only for datasets
    if not isinstance(data_asset, (dataset.Dataset, pd.DataFrame)):
        raise ValueError(
            "The validate util method only supports dataset validations, including custom subclasses. For other data asset types, use the object's own validate method.")

    if not issubclass(type(data_asset), data_asset_type):
        if isinstance(data_asset, (pd.DataFrame)) and issubclass(data_asset_type, dataset.PandasDataset):
            pass  # This is a special type of allowed coercion
        else:
            raise ValueError(
                "The validate util method only supports validation for subtypes of the provided data_asset_type.")

    data_asset_ = _convert_to_dataset_class(
        data_asset, data_asset_type, expectation_suite)
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

def get_data_context(path=None):
    """Given a path, try to guess where the DataContext is located.
    """
    pass

def acts_as_a_number(var):
    try:
        0 + var
    except TypeError:
        return False
    else:
        return True


def result_contains_numeric_observed_value(result):
    """

    :param result:
    :return:
    """
    return ('observed_value' in result['result'] \
            and acts_as_a_number(result['result'].get('observed_value'))) \
           and set(result['result'].keys()) <= set(
        ['observed_value', 'element_count', 'missing_count', 'missing_percent'])


def result_contains_unexpected_pct(result):
    """

    :param result:
    :return:
    """
    return 'unexpected_percent' in result['result'] \
           and result['expectation_config']['expectation_type'] != 'expect_column_values_to_be_in_set'


def get_metrics_for_expectation(result, data_asset_name, batch_fingerprint):
    """
    Extract metrics from a validation result of one expectation.
    Depending on the type of the expectation, this method chooses the key
    in the result dictionary that should be returned as a metric
    (e.g., "observed_value" or "unexpected_percent").

    :param result: a validation result dictionary of one expectation
    :param data_asset_name:
    :param batch_fingerprint:
    :return: a dict {metric_name -> metric_value}
    """
    expectation_metrics = {
        # 'expect_column_distinct_values_to_be_in_set'
        # 'expect_column_kl_divergence_to_be_less_than',
        'expect_column_max_to_be_between': {
            'observed_value': 'column_max'
        },
        'expect_column_mean_to_be_between': {
            'observed_value': 'column_mean'
        },
        'expect_column_median_to_be_between': {
            'observed_value': 'column_median'
        },
        'expect_column_min_to_be_between': {
            'observed_value': 'column_min'
        },
        'expect_column_proportion_of_unique_values_to_be_between': {
            'observed_value': 'column_proportion_of_unique_values'
        },
        # 'expect_column_quantile_values_to_be_between',
        'expect_column_stdev_to_be_between': {
            'observed_value': 'column_stdev'
        },
        'expect_column_unique_value_count_to_be_between': {
            'observed_value': 'column_unique_count'
        },
        # 'expect_column_values_to_be_between',
        # 'expect_column_values_to_be_in_set',
        # 'expect_column_values_to_be_in_type_list',
        'expect_column_values_to_be_unique': {

        },
        # 'expect_table_columns_to_match_ordered_list',
        'expect_table_row_count_to_be_between': {
            'observed_value': 'row_count'
        }

    }


    metrics = []
    if result.get('result'):
        entry = expectation_metrics.get(result['expectation_config']['expectation_type'])
        if entry:
            for key in result['result'].keys():
                metric_name = entry.get(key)
                if metric_name:
                    metric_kwargs = {"column": result['expectation_config']['kwargs']['column']} if result['expectation_config'][
                'kwargs'].get('column') else {}

                    new_metric = NamespaceAwareValidationMetric(
                        data_asset_name=data_asset_name,
                        batch_fingerprint=batch_fingerprint,
                        metric_name=metric_name,
                        metric_kwargs=metric_kwargs,
                        metric_value=result['result'][key])
                    metrics.append(new_metric)

        elif result_contains_numeric_observed_value(result):
            metric_kwargs = {"column": result['expectation_config']['kwargs']['column']} if \
            result['expectation_config']['kwargs'].get('column') else {}

            new_metric = NamespaceAwareValidationMetric(
                data_asset_name=data_asset_name,
                batch_fingerprint=batch_fingerprint,
                metric_name=result['expectation_config']['expectation_type'] + "__obsval",
                metric_kwargs=metric_kwargs,
                metric_value=result.get('result').get('observed_value'))
            metrics.append(new_metric)
        elif result_contains_unexpected_pct(result):
            metric_kwargs = {"column": result['expectation_config']['kwargs']['column']} if result['expectation_config'][
                'kwargs'].get('column') else {}

            new_metric = NamespaceAwareValidationMetric(
                data_asset_name=data_asset_name,
                batch_fingerprint=batch_fingerprint,
                metric_name=result['expectation_config']['expectation_type'] + "__unexp_pct",
                metric_kwargs=metric_kwargs,
                metric_value=result.get('result').get('unexpected_percent'))
            metrics.append(new_metric)

    return metrics
