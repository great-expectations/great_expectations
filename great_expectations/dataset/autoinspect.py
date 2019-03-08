"""
Autoinspect utilities to automatically generate expectations by evaluating a data_asset.
"""
from __future__ import division

import warnings
from six import string_types

from .util import create_multiple_expectations


class AutoInspectError(Exception):
    """Exception raised for errors in autoinspection.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


def columns_exist(inspect_dataset):
    """
    This function will take a dataset and add expectations that each column present exists.

    Args:
        inspect_dataset (great_expectations.dataset): The dataset to inspect and to which to add expectations.
    """

    # Attempt to get column names. For pandas, columns is just a list of strings
    if not hasattr(inspect_dataset, "columns"):
        warnings.warn(
            "No columns list found in dataset; no autoinspection performed.")
        return
    elif isinstance(inspect_dataset.columns[0], string_types):
        columns = inspect_dataset.columns
    elif isinstance(inspect_dataset.columns[0], dict) and "name" in inspect_dataset.columns[0]:
        columns = [col['name'] for col in inspect_dataset.columns]
    else:
        raise AutoInspectError(
            "Unable to determine column names for this dataset.")

    create_multiple_expectations(
        inspect_dataset, columns, "expect_column_to_exist")
