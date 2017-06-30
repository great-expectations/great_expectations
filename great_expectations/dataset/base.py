## Run these tests with:
## python -m unittest tests.test_pandas_dataset_unittest

import json
import inspect
import copy

import pandas as pd
import numpy as np

from .util import DotDict, ensure_json_serializable

from functools import wraps

class DataSet(object):

    def __init__(self, *args, **kwargs):
        super(DataSet, self).__init__(*args, **kwargs)
        self.initialize_expectations()

    def initialize_expectations(self, config=None, name=None):

        if config != None:
            #!!! Should validate the incoming config with jsonschema here

            # Copy the original so that we don't overwrite it by accident
            self._expectations_config = DotDict(copy.deepcopy(config))

        else:
            self._expectations_config = DotDict({
                "dataset_name" : None,
                "expectations" : []
            })

            for col in self.columns:
                self._expectations_config.expectations.append({
                    "expectation_type" : "expect_column_to_exist",
                    "kwargs" : {
                        "column" : col
                    }
                })

        self._expectations_config.dataset_name = name

    def append_expectation(self, expectation_config):
        expectation_type = expectation_config['expectation_type']

        #Drop existing expectations with the same expectation_type.
        #!!! This is good default behavior, but
        #!!!    it needs to be documented, and
        #!!!    we need to provide syntax to override it.
        #self._expectations_config.expectations = filter(
        #    lambda exp: exp['expectation_type'] != expectation_type,
        #    self._expectations_config.expectations
        #)

        ## Changed to use list comprehension for python 3 support
        self._expectations_config.expecations = [
            exp for exp in self._expectations_config.expectations if lambda exp: exp['expectation_type'] != expectation_type
        ]

        self._expectations_config.expectations.append(expectation_config)

    @staticmethod
    def expectation(func):

        def wrapper(self, *args, **kwargs):

            #Get the name of the method
            method_name = func.__name__

            #Fetch argument names
            method_arg_names = inspect.getargspec(func)[0][1:]

            #Construct the expectation_config object
            expectation_config = dict(
                zip(method_arg_names, args)+\
                kwargs.items()
            )

            #Add the expectation_method key
            expectation_config['expectation_type'] = method_name

            #Append the expectation to the config.
            self.append_expectation(expectation_config)

            #Finally, execute the expectation method itself
            return func(self, *args, **kwargs)

        wrapper.__doc__ = func.__doc__
        return wrapper


    @staticmethod
    def column_expectation(func):

        def wrapper(self, column, *args, **kwargs):
            #Get the name of the method
            method_name = func.__name__

            #Fetch argument names
            method_arg_names = inspect.getargspec(func)[0][2:]

            # Combine all arguments into a single new "kwargs"
            all_args = dict(zip(method_arg_names, args))
            all_args.update(kwargs)

            all_args = ensure_json_serializable(all_args)

            #Construct the expectation_config object
            expectation_config = DotDict({
                "expectation_type" : method_name,
                ## Changed to support python 3, but note that there may be ambiguity here.
                ## TODO: ensure this is the intended logic
                "kwargs" : all_args
            })
            expectation_config['kwargs']['column'] = column

            #Append the expectation to the table config.
            self.append_expectation(expectation_config)

            #Now execute the expectation method itself
            return func(self, column, *args, **kwargs)

        wrapper.__doc__ = func.__doc__
        return wrapper

    def get_expectations_config(self):
        return self._expectations_config

    def save_expectations_config(self, filepath=None):
        if filepath==None:
            #!!! Fetch the proper filepath from the project config
            pass

        expectation_config_str = json.dumps(self.get_expectations_config(), indent=2)
        open(filepath, 'w').write(expectation_config_str)

    def validate(self):
        results = []
        for expectation in self.get_expectations_config()['expectations']:
            print(expectation)
            expectation_method = getattr(self, expectation['expectation_type'])
            result = expectation_method(**expectation['kwargs'])
            print(result)


    ##### Table shape expectations #####

    def expect_column_to_exist(self, column, suppress_exceptions=False):
        """Expect the specified column to exist in the data set.

        Args:
            column: The column name
            suppress_exceptions: Only return a boolean success value, not a dictionary with other results.

        Returns:
            By default: a dict containing "success" and "result" (an empty dictionary)
            On suppress_exceptions=True: a boolean success value only
        """
        raise NotImplementedError

    def expect_table_row_count_to_be_between(self, min_value, max_value,suppress_exceptions=False):
        """
        docstring
        should we count null values?
        """
        raise NotImplementedError

    def expect_table_row_count_to_equal(self, value, suppress_exceptions=False):
        """
        docstring
        """
        raise NotImplementedError

    ##### Missing values, unique values, and types #####

    def expect_column_values_to_be_unique(self, column, mostly=None, suppress_exceptions=False):
        """
        Expect each not_null value in this column to be unique.

        Display multiple duplicated items.
        ['2','2','2'] will return `['2','2']` for the exceptions_list.

        !!! Prevent division-by-zero errors in the `mostly` logic
        """
        raise NotImplementedError

    def expect_column_values_to_not_be_null(self, column, mostly=None, suppress_exceptions=False):
        """
        Expect values in this column to not be null.

        Instead of reinventing our own system for handling missing data, we use pandas.Series.isnull
        and notnull to define "null."
        See the pandas documentation for details.

        Note: When returning the list of exceptions, replace np.nan with None.

        !!! Prevent division-by-zero errors in the `mostly` logic
        """
        raise NotImplementedError

    def expect_column_values_to_be_null(self, column, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        raise NotImplementedError

    def expect_column_values_to_be_of_type(self, column, dtype, mostly=None, suppress_exceptions=False):
        """
        NOT STABLE
        docstring
        """
        raise NotImplementedError

    ##### Sets and ranges #####

    def expect_column_values_to_be_in_set(self, column, values_set, mostly=None, suppress_exceptions=False):
        """
        !!! Prevent division-by-zero errors in the `mostly` logic
        """
        raise NotImplementedError

    def expect_column_values_to_not_be_in_set(self, column, values_set, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        raise NotImplementedError

    def expect_column_values_to_be_between(self, column, min_value, max_value, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        raise NotImplementedError

    ##### String matching #####

    def expect_column_value_lengths_to_be_between(self, column, min_value, max_value, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        raise NotImplementedError

    def expect_column_values_to_match_regex(self, column, regex, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        raise NotImplementedError

    def expect_column_values_to_not_match_regex(self, column, regex, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        raise NotImplementedError

    def expect_column_values_to_match_regex_list(self, column, regex_list, mostly=None, suppress_exceptions=False):
        """
        NOT STABLE
        docstring
        define test function first
        """
        raise NotImplementedError

    ##### Datetime and JSON parsing #####

    def expect_column_values_to_match_strftime_format(self, column, format, mostly=None, suppress_exceptions=False):
        """
        Expect values in this column to match the user-provided datetime format.
        WARNING: Note that strftime formats are not universally portable across implementations.
        For example, the %z directive may not be implemented before python 3.2.

        Args:
            col: The column name
            format: The format string against which values should be validated
            mostly (float): The proportion of values that must match the condition for success to be true.
            suppress_exceptions: Only return a boolean success value, not a dictionary with other results.

        Returns:
            By default: a dict containing "success" and "result" (an empty dictionary)
            On suppress_exceptions=True: a boolean success value only
        """
        raise NotImplementedError

    def expect_column_values_to_be_dateutil_parseable(self, column, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        raise NotImplementedError

    def expect_column_values_to_be_valid_json(self, column, suppress_exceptions=False):
        """
        docstring
        """
        raise NotImplementedError

    def expect_column_values_to_match_json_schema(self, column, json_schema, suppress_exceptions=False):
        """
        docstring
        """
        raise NotImplementedError

    ##### Aggregate functions #####

    def expect_column_mean_to_be_between(self, column, min_value, max_value):
        """
        docstring
        """
        raise NotImplementedError

    def expect_column_median_to_be_between(self, column, min_value, max_value):
        """
        docstring
        """
        raise NotImplementedError

    def expect_column_stdev_to_be_between(self, column, min_value, max_value, suppress_exceptions=False):
        """
        docstring
        """
        raise NotImplementedError

    @wraps
    def expect_column_frequency_distribution_to_be(self, column, vals, expected_frequencies, p=0.05, suppress_exceptions=False):
        raise NotImplementedError

    @wraps
    def expect_column_numerical_distribution_to_be(self, column, partition, cdf_vals, sample_size=0, p=0.05, suppress_exceptions=False):
        """
        Expect the values in this column to match the density of the provided scipy.stats kde estimate.

        Args:
            col (string): The column name
            kde: A kernel density estimate built which we expect to match the distribution of data provided.
            p (float) = 0.05: The p-value threshold below which this expectation will return false.
            suppress_exceptions: Only return a boolean success value, not a dictionary with other results.

        Returns:
            By default: a dict containing "success" and "exception_list" (the pvalue from the Kolmogorov-Smirnov Test)
            On suppress_exceptions=True: a boolean success value only
        """
        raise NotImplementedError

    # def expect_column_frequency_distribution_to_be():

    ##### Column pairs #####

    # def expect_two_column_values_to_be_equal():

    def expect_two_column_values_to_be_subsets(self, column_1, column_2, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        raise NotImplementedError

    def expect_two_column_values_to_be_many_to_one(self, column_1, column_2, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        raise NotImplementedError

    # def expect_two_column_crosstabs_to_be

    #!!! Deprecated
    # def expect_values_to_be_equal_across_columns(self, column, regex, mostly=None, suppress_exceptions=False):
    #     """
    #     """
    #     raise NotImplementedError

    #!!! Deprecated
    # def expect_column_values_to_be_equal_across_columns(self, column_1, column_2, suppress_exceptions=False):
    #     """
    #     docstring
    #     """
    #     raise NotImplementedError

    ##### Multicolumn relations #####

    # def expect_multicolumn_values_to_be_unique
