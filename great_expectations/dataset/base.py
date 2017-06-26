import json
import inspect
import copy

import pandas as pd
import numpy as np

from util import DotDict

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
        self._expectations_config.expectations = filter(
            lambda exp: exp['expectation_type'] != expectation_type,
            self._expectations_config.expectations 
        )

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

            #Construct the expectation_config object
            expectation_config = DotDict({
                "expectation_type" : method_name,
                "kwargs" : dict(
                    zip(method_arg_names, args)+\
                    kwargs.items()
                )
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
        file(filepath, 'w').write(expectation_config_str)

    def validate(self):
        results = []
        for expectation in self.get_expectations_config()['expectations']:
            print expectation
            expectation_method = getattr(self, expectation['expectation_type'])
            result = expectation_method(**expectation['kwargs'])
            print result


    ##### Table shape expectations #####

    def expect_column_to_exist(self, column, suppress_exceptions=False):
        """Expect the specified column to exist in the data set.

        Args:
            column (str): The column name.

        Keyword Args:
            suppress_exceptions=False: Return the boolean of "success" instead of the entire dictionary.

        Returns:
            result (bool)
            
            If suppress_exceptions=True, return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    def expect_table_row_count_to_be_between(self, min_value, max_value, suppress_exceptions=False):
        """Expect the number of rows in a data set to be between two values.

        Args:
            min_value (int): the minimum number of rows.
            max_value (int): the maximum number of rows.

        Keyword Args:
            suppress_exceptions=False: Return the boolean of "success" instead of the entire dictionary.

        Returns:
            result (bool)

            If suppress_exceptions=True, return the boolean value in "success" instead of dict.

        See Also:
            expect_table_row_count_to_equal

        """
        raise NotImplementedError

    def expect_table_row_count_to_equal(self, value, suppress_exceptions=False):
        """Expect the number of rows to be equal to a value.

        Args:
	    value (int): The value that should equal the number of rows.

        Keyword Args:
            suppress_exceptions=False: Return the boolean of "success" instead of the entire dictionary.

        Returns:
	    result (bool)

            If suppress_exceptions=True, return the boolean value in "success" instead of dict.

        See Also:
            expect_table_row_count_to_be_between

        """
        raise NotImplementedError

    ##### Missing values, unique values, and types #####

    def expect_column_values_to_be_unique(self, column, mostly=None, suppress_exceptions=False):
        """Expect each nonempty column entry to be unique (no duplicates).

        Args:
            column (str): The column name.

        Keyword Args:
            mostly=None: Return "success": True if the percentage of unique values is greater than or equal to mostly (a float between 0 and 1).
            suppress_exceptions=False: Return the boolean of "success" instead of the entire dictionary.

        Returns:
	    result (bool)
            
            If suppress_exceptions=True then the method returns the boolean value in "success" instead of dict.

        Examples:
	    Display multiple duplicated items.
	    ['2','2','2'] will return `['2','2']` for the exceptions_list.


        """
        raise NotImplementedError

    def expect_column_values_to_not_be_null(self, column, mostly=None, suppress_exceptions=False):
        """Expect each column entry to be nonempty.

        Args:
            column (str): The column name.

        Keyword Args:
            mostly=None: Return "success": True if the percentage of not null values is greater than or equal to mostly (a float between 0 and 1).
            suppress_exceptions (bool): Return the boolean of "success" instead of the entire dictionary.

        Returns:
	    result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    def expect_column_values_to_be_null(self, column, mostly=None, suppress_exceptions=False):
        """Expect the column entries to be empty.

        Args:
            column (str): The column name.

        Keyword Args:
            mostly=None: Return "success": True if the percentage of null values is greater than or equal to mostly (a float between 0 and 1).
            suppress_exceptions=False: Return the boolean of "success" instead of the entire dictionary.

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    def expect_column_values_to_be_of_type(self, column, dtype, mostly=None, suppress_exceptions=False):
        """Expect each column entry to be a specified data type.

        Q: Will dtype be a string or a __type__ attribute?

        Args:
            column (str): The column name.
            dtype (str): The data type that each column should have as entries.

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    ##### Sets and ranges #####

    def expect_column_values_to_be_in_set(self, column, values_set, mostly=None, suppress_exceptions=False):
        """Expect each entry in a column to be in a given set.

        Args:
            column (str): The column name.
            values_set (set-like): The set of objects or unique data points corresponding to the column.

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        !!! Prevent division-by-zero errors in the `mostly` logic

        """
        raise NotImplementedError

    def expect_column_values_to_not_be_in_set(self, column, values_set, mostly=None, suppress_exceptions=False):
        """Expect column entries to not be in the set.

        Args:
            column (str): The column name.
            values_set (list): The set of objects or unique data points that should not correspond to the column.

        Keyword Args:

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    def expect_column_values_to_be_between(self, column, min_value, max_value, mostly=None, suppress_exceptions=False):
        """Expect column entries to be a number between a minimum value and a maximum value.

        Args:
            column (str): The column name.
            min_value (int): The minimum value for a column entry.
            max_value (int): The maximum value for a column entry.

        Keyword Args:

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    ##### String matching #####

    def expect_column_value_lengths_to_be_between(self, column, min_value, max_value, mostly=None, suppress_exceptions=False):
        """Expect column entries to have a measurable length which lies between a minimum value and a maximum value.

        Args:
            column (str): The column name.
            min_value (int): The minimum value for a column entry length.
            max_value (int): The maximum value for a column entry length.

        Keyword Args:

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    def expect_column_values_to_match_regex(self, column, regex, mostly=None, suppress_exceptions=False):
        """Expect column entries to be strings that match a given regular expression.

        Args:
            column (str): The column name.
            regex (str): The regular expression that the column entry should match.

        Keyword Args:

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    def expect_column_values_to_not_match_regex(self, column, regex, mostly=None, suppress_exceptions=False):
        """Expect column entries to be strings that do not match a given regular expression.

        Q: Emphasize the not?

        Args:
            column (str): The column name.
            regex (str): The regular expression that the column entry should NOT match.

        Keyword Args:

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    def expect_column_values_to_match_regex_list(self, column, regex_list, mostly=None, suppress_exceptions=False):
        """Expect the column entries to be strings that match at least one of a list of regular expressions.

        Q: Is it sufficient for the column value to match at least one regex in the list?

        Args:
            column (str): The column name.
            regex_list (list): The list of regular expressions in which the column entries should match at least one.

        Keyword Args:

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    ##### Datetime and JSON parsing #####

    def expect_column_values_to_match_strftime_format(self, column, strftime_format, mostly=None, suppress_exceptions=False):
        """Expect column entries to be strings representing a date or time with a given format.

        Args:
            column (str): The column name.
            strftime_format (str): The time format that the column entries should match.

        Keyword Args:

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        Expect values in this column to match the user-provided datetime format.
        WARNING: Note that strftime formats are not universally portable across implementations.
        For example, the %z directive may not be implemented before python 3.2.

        Args:
            col: The column name
            format: The format string against which values should be validated
            mostly (float): The proportion of values that must match the condition for success to be true.
            suppress_exceptions: Only return a boolean success value, not a dictionary with other results.

        """
        raise NotImplementedError

    def expect_column_values_to_be_dateutil_parseable(self, column, mostly=None, suppress_exceptions=False):
        """Expect column entries to be interpretable as a dateutil object.

        Args:
            column (str): The column name.

        Keyword Args:

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    def expect_column_values_to_be_valid_json(self, column, suppress_exceptions=False):
        """Expect column entries to be data written in JavaScript Object Notation.

        Args:
            column (str): The column name.

        Keyword Args:

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    def expect_column_values_to_match_json_schema(self, column, json_schema, suppress_exceptions=False):
        """Expect column entries to be JSON objects with a given JSON schema.

        Q: What kind of data type is the json_schema variable?

        Args:
            column (str): The column name.
            json_schema (): The JSON schema that each column entry should resemble.

        Keyword Args:

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    ##### Aggregate functions #####

    def expect_column_mean_to_be_between(self, column, min_value, max_value):
        """Expect the column mean to be between a minimum value and a maximum value.

        Args:
            column (str): The column name.
            min_value (int): The minimum value for the column mean.
            max_value (int): The maximum value for the column mean.

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    def expect_column_median_to_be_between(self, column, min_value, max_value):
        """Expect the column median to be between a minimum value and a maximum value.

        Args:
            column (str): The column name.
            min_value (int): The minimum value for the column median.
            max_value (int): The maximum value for the column median.

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    def expect_column_stdev_to_be_between(self, column, min_value, max_value, suppress_exceptions=False):
        """Expect the column standard deviation to be between a minimum value and a maximum value.

        Args:
            column (str): The column name.
            min_value (int): The minimum value for the column standard deviation.
            max_value (int): The maximum value for the column standard deviation.

        Keyword Args:

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    # def expect_column_numerical_distribution_to_be(self, column, min_value, max_value, suppress_exceptions=False):
    # def expect_column_frequency_distribution_to_be():

    ##### Column pairs #####

    # def expect_two_column_values_to_be_equal():

    def expect_two_column_values_to_be_subsets(self, column_1, column_2, mostly=None, suppress_exceptions=False):
        """Given two columns, expect one column to have entries such that the entries are a subset of the other column's entries.

        Q: Should the subset come first or second? Does it matter?

        Args:
            column_1 (str): The first column name to compare entries.
            column_2 (str): The second column name to compare entries.

        Keyword Args:

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

        """
        raise NotImplementedError

    def expect_two_column_values_to_be_many_to_one(self, column_1, column_2, mostly=None, suppress_exceptions=False):
        """Given two columns, expect one column to map multiple entries to a single entry of the other column.

        Q: What is a use case? Multiple values per entry corresponding to one value in another column as in gps coords to a name?

        Args:
            column_1 (str): the column with multiple values per entry
            column_2 (str): the column with one value per entry

        Keyword Args:

        Returns:
            result (bool)
            
            If suppress_exceptions=True then return the boolean value in "success" instead of dict.

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




