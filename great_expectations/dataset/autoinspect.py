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
    if not hasattr(inspect_dataset, 'get_table_columns'):
        warnings.warn(
            "No columns list found in dataset; no autoinspection performed.")
        raise NotImplementedError("columns_exist autoinspection is not implemented for data assests without the table_columns property")
    table_columns = inspect_dataset.get_table_columns()
    if table_columns is None:
        warnings.warn(
            "No columns list found in dataset; no autoinspection performed.")
        raise NotImplementedError("columns_exist autoinspection is not implemented for data assests without the table_columns property")
    create_multiple_expectations(inspect_dataset, table_columns, "expect_column_to_exist")
