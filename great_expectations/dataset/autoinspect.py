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

#!!! This method is invoked by pseudo_pandas_profiling
#!!! We probably want to refactor autoinspectors to be full classes, rather than just functions.
#!!! That will allow for subfunctions and inheritance.
#!!! However, we probably *don't* want autoinspection to be stateful.
def _get_column_type_and_cardinality(df, column):
    
    if df.expect_column_values_to_be_of_type(column, "int")["success"]:
        type_ = "int"
        
    elif df.expect_column_values_to_be_of_type(column, "float")["success"]:
        type_ = "float"
    
    elif df.expect_column_values_to_be_of_type(column, "string")["success"]:
        type_ = "string"
        
    else:
        type_ = "unknown"

    unique = df.expect_column_unique_value_count_to_be_between(column, 0, None)['result']['observed_value']
    pct_unique = df.expect_column_proportion_of_unique_values_to_be_between(column, 0, None)['result']['observed_value']
    
    if pct_unique == 1.0:
        cardinality = "unique"
    
    elif pct_unique > .1:
        cardinality = "many"

    elif pct_unique > .02:
        cardinality = "lots"

    else:
        if unique == 0:
            cardinality = "none"

        elif unique == 1:
            cardinality = "one"

        elif unique == 2:
            cardinality = "two"

        elif unique < 10:
            cardinality = "very few"
            
        elif unique < 200:
            cardinality = "few"
            
        else:
            cardinality = "unknown"
#             print(
#                 column, '\t',
#                 unique,
#                 pct_unique
#             )

    return (type_, cardinality)

def pseudo_pandas_profiling(dataset):
    df = dataset

    for column in df.columns:
        df.expect_column_to_exist(column)

        type_, cardinality = _get_column_type_and_cardinality(df, column)
        df.expect_column_values_to_not_be_null(column)
        df.expect_column_values_to_be_in_set(column, [], result_format="SUMMARY")

        if type_ == "int":
            df.expect_column_values_to_not_be_in_set(column, [0])

            if cardinality == "unique":
                df.expect_column_values_to_be_unique(column)
                df.expect_column_values_to_be_increasing(column)
            
    #         elif cardinality == "very few":
    # #             print(df[column].value_counts())
    #             pass
                
            else:
                print(column, cardinality)
        
        elif type_ == "float":
    #         print(column, type_, cardinality)
            pass

        elif type_ == "string":
            #Check for leading and tralining whitespace.
            #!!! It would be nice to build additional Expectations here, but
            #!!! the default logic for remove_expectations prevents us.
            df.expect_column_values_to_not_match_regex(column, "^\s+|\s+$")

            if cardinality == "unique":
                df.expect_column_values_to_be_unique(column)
            
            elif cardinality in ["one", "two", "very few", "few"]:
                
    #             print(df[column].value_counts())
                pass
                
            else:
                print(column, type_, cardinality)

        else:
            print("??????", column, type_, cardinality)
