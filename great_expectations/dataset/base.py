import json
import inspect
import copy
from functools import wraps
import traceback

import pandas as pd
import numpy as np

from .util import DotDict, ensure_json_serializable

class DataSet(object):

    def __init__(self, *args, **kwargs):
        super(DataSet, self).__init__(*args, **kwargs)
        self.initialize_expectations()

    @classmethod
    def expectation(cls, method_arg_names):
        """
        The core expectation decorator, this method takes a single parameter which it uses to build and save the
        expectation config to the DataSet object. The parameter defines an ordered list of the positional arguments to
        be used by the method implementing the expectation.

        Note that intermediate decorators that call the core @expectation decorator will most likely need to pass their
        decorated methods' signature up to the expectation decorator. For example, the MetaPandasDataSet column_map_expectation
        decorator relies on the DataSet expectation decorator, but will pass through the signature from the implementing method.

        When decorated with @expectation, a method will:
            1. Build and update the expectation config.
            2. Handle the "include_config" boolean parameter, which allows a caller to retrieve the generated
                configuration immediately after running the expectation.
            3. Handle the "catch_excpetions" parameter, which allows a caller to catch any exception and report an
                aggregate trace, useful for validation.
            4. Handle the "output_format" parameter, and pass it down to the implementing method if its signature expects it.
                By handing down the output_format, methods implementing expectations can optionally provide additional output formats
                specific to the use cases that they handle.
        """
        def outer_wrapper(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):

                #Get the name of the method
                method_name = func.__name__

                # Combine all arguments into a single new "kwargs"
                all_args = dict(zip(method_arg_names, args))
                all_args.update(kwargs)

                #Unpack display parameters; remove them from all_args if appropriate
                if "include_config" in kwargs:
                    include_config = kwargs["include_config"]
                    del all_args["include_config"]
                else:
                    include_config = self.default_expectation_args["include_config"]

                if "catch_exceptions" in kwargs:
                    catch_exceptions = kwargs["catch_exceptions"]
                    del all_args["catch_exceptions"]
                else:
                    catch_exceptions = self.default_expectation_args["catch_exceptions"]

                if "output_format" in kwargs:
                    output_format = kwargs["output_format"]
                else:
                    output_format = self.default_expectation_args["output_format"]

                # This intends to get the signature of the inner wrapper, if there is one.
                if "output_format" in inspect.getargspec(func)[0][1:]:
                    all_args["output_format"] = output_format
                else:
                    if "output_format" in all_args:
                        del all_args["output_format"]

                all_args = ensure_json_serializable(all_args)

                #Construct the expectation_config object
                expectation_config = DotDict({
                    "expectation_type": method_name,
                    "kwargs": all_args
                })

                #Add the expectation_method key
                expectation_config['expectation_type'] = method_name

                #Append the expectation to the config.
                self.append_expectation(expectation_config)

                raised_exception = False
                exception_traceback = None

                #Finally, execute the expectation method itself
                try:
                    return_obj = func(self, **all_args)

                except Exception as err:
                    if catch_exceptions:
                        raised_exception = True
                        exception_traceback = traceback.format_exc()

                        if output_format != "BOOLEAN_ONLY":
                            return_obj = {
                                "success": False
                            }
                        else:
                            return_obj = False
                    else:
                        raise(err)

                if output_format != 'BOOLEAN_ONLY':
                    if include_config:
                        return_obj["expectation_type"] = expectation_config["expectation_type"]
                        return_obj["expectation_kwargs"] = copy.deepcopy(dict(expectation_config["kwargs"]))

                    if catch_exceptions:
                        return_obj["raised_exception"] = raised_exception
                        return_obj["exception_traceback"] = exception_traceback

                    # print json.dumps(return_obj, indent=2)

                return return_obj

            # wrapper.__name__ = func.__name__
            # wrapper.__doc__ = func.__doc__
            return wrapper

        return outer_wrapper

    @classmethod
    def column_map_expectation(cls, func):
        raise NotImplementedError

    @classmethod
    def column_aggregate_expectation(cls, func):
        raise NotImplementedError

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

            #!!! This is specific to pandas.DataFame, so should live in that subclass
            #!!! ...or not at all, if we decide this is too much autoinspection.
            for col in self.columns:
                self._expectations_config.expectations.append({
                    "expectation_type" : "expect_column_to_exist",
                    "kwargs" : {
                        "column" : col
                    }
                })

        self.default_expectation_args = {
            "include_config" : False,
            "catch_exceptions" : False,
            "output_format" : 'BASIC',
        }

        self._expectations_config.dataset_name = name

    def append_expectation(self, expectation_config):
        expectation_type = expectation_config['expectation_type']

        #Drop existing expectations with the same expectation_type.
        #For column_expectations, append_expectation should only replace expectations
        # where the expectation_type AND the column match
        #!!! This is good default behavior, but
        #!!!    it needs to be documented, and
        #!!!    we need to provide syntax to override it.

        if 'column' in expectation_config['kwargs']:
            column  = expectation_config['kwargs']['column']

            self._expectations_config.expectations = [f for f in filter(
                lambda exp: (exp['expectation_type'] != expectation_type) | (exp['kwargs']['column'] != column),
                self._expectations_config.expectations
            )]
        else:
            self._expectations_config.expectations = [f for f in filter(
                lambda exp: exp['expectation_type'] != expectation_type,
                self._expectations_config.expectations
            )]

        self._expectations_config.expectations.append(expectation_config)

    def get_default_expectation_arguments(self):
        return self.default_expectation_args

    def set_default_expectation_argument(self, argument, value):
        #!!! Maybe add a validation check here?

        self.default_expectation_args[argument] = value

    def get_expectations_config(self):
        return self._expectations_config

    def save_expectations_config(self, filepath=None):
        if filepath==None:
            #!!! Fetch the proper filepath from the project config
            pass

        expectation_config_str = json.dumps(self.get_expectations_config(), indent=2)
        open(filepath, 'w').write(expectation_config_str)

    def validate(self, expectations_config=None, catch_exceptions=True, output_format=None, include_config=None):
        results = []

        if expectations_config is None:
            expectations_config = self.get_expectations_config()

        for expectation in expectations_config['expectations']:
            expectation_method = getattr(self, expectation['expectation_type'])
            if output_format is not None:
                expectation['kwargs'].update({"output_format": output_format})
            if include_config is not None:
                expectation['kwargs'].update({"include_config": include_config})
            result = expectation_method(
                catch_exceptions=catch_exceptions,
                **expectation['kwargs']
            )

            results.append(
                dict(list(expectation.items()) + list(result.items()))
            )

        return {
            "results" : results
        }


    ##### Output generation #####
    def format_column_map_output(self,
        output_format, success,
        element_count,
        nonnull_values, nonnull_count,
        boolean_mapped_success_values, success_count,
        exception_list, exception_index_list
    ):
        if output_format == "BOOLEAN_ONLY":
            return_obj = success

        elif output_format == "BASIC":
            exception_count = len(exception_list)

            return_obj = {
                "success": success,
                "summary_obj": {
                    "partial_exception_list": exception_list[:20],
                    "exception_count": exception_count,
                    "exception_percent": float(exception_count) / nonnull_count,
                }
            }

        elif output_format == "COMPLETE":
            return_obj = {
                "success": success,
                "exception_list": exception_list,
                "exception_index_list": exception_index_list,
            }

        elif output_format == "SUMMARY":
            # element_count = int(len(series))
            missing_count = element_count-int(len(nonnull_values))#int(null_indexes.sum())
            exception_count = len(exception_list)

            exception_value_series = pd.Series(exception_list).value_counts()
            exception_counts = dict(zip(
                list(exception_value_series.index),
                list(exception_value_series.values),
            ))

            if element_count > 0:
                missing_percent = float(missing_count) / element_count

                if nonnull_count > 0:
                    exception_percent = float(exception_count) / element_count
                    exception_percent_nonmissing = float(exception_count) / nonnull_count

            else:
                missing_percent = None
                nonmissing_count = None
                exception_percent = None
                exception_percent_nonmissing = None

            return_obj = {
                "success": success,
                "exception_list": exception_list,
                "exception_index_list": exception_index_list,
                "summary_obj": {
                    "element_count": element_count,
                    "missing_count": missing_count,
                    "missing_percent": missing_percent,
                    "exception_count": exception_count,
                    "exception_percent": exception_percent,
                    "exception_percent_nonmissing": exception_percent_nonmissing,
                    "exception_counts": exception_counts,
                }
            }

        else:
            print ("Warning: Unknown output_format %s. Defaulting to BASIC." % (output_format,))
            return_obj = {
                "success" : success,
                "exception_list" : exception_list,
            }

        return return_obj

    def calc_map_expectation_success(self, success_count, nonnull_count, exception_count, mostly):
        if nonnull_count > 0:
            percent_success = float(success_count)/nonnull_count

            if mostly:
                success = bool(percent_success >= mostly)

            else:
                success = bool(exception_count == 0)

        else:
            success = True
            percent_success = None

        return success, percent_success

    ##### Table shape expectations #####

    def expect_column_to_exist(self, column):
        """Expect the specified column to exist in the data set.
        Args:
            column (str): The column name.
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }

        """
        raise NotImplementedError

    def expect_table_row_count_to_be_between(self, min_value, max_value):
        """Expect the number of rows in a data set to be between two values.
        Args:
            min_value (int or None): the minimum number of rows.
            max_value (int or None): the maximum number of rows.
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_table_row_count_to_equal
        """
        raise NotImplementedError

    def expect_table_row_count_to_equal(self, value, suppress_exceptions=False):
        """Expect the number of rows to be equal to a value.
        Args:
	    value (int): The value that should equal the number of rows.
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
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
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }

        Examples:
	    Display multiple duplicated items. For example, ['2','2','2'] will return `['2','2']` for the exceptions_list.
        """
        raise NotImplementedError

    def expect_column_values_to_not_be_null(self, column, mostly=None, suppress_exceptions=False):
        """Expect each column entry to be nonempty.
        Args:
            column (str): The column name.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of not null values is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_be_null

        """
        raise NotImplementedError

    def expect_column_values_to_be_null(self, column, mostly=None, suppress_exceptions=False):
        """Expect the column entries to be empty.
        Args:
            column (str): The column name.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of null values is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_not_be_null

        """
        raise NotImplementedError

    def expect_column_values_to_be_of_type(self, column, type_, target_datasource, mostly=None, suppress_exceptions=False):
        """Expect each column entry to be a specified data type.
        Args:
            column (str): The column name.
            type_ (str): A string representing the data type that each column should have as entries.
                For example, "double integer" refers to an integer with double precision.
            target_datasource (str): The data source that specifies the implementation in the type_ parameter.
                For example, options include "numpy", "sql", or "spark".
        Keyword Args:
            mostly=None: Return "success": True if the percentage of values of type_ is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }

        """
        raise NotImplementedError

    ##### Sets and ranges #####

    def expect_column_values_to_be_in_set(self, column, values_set, mostly=None, suppress_exceptions=False):
        """Expect each entry in a column to be in a given set.
        Args:
            column (str): The column name.
            values_set (set-like): The set of objects or unique data points corresponding to the column.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of values in values_set is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_not_be_in_set

        """
        raise NotImplementedError

    def expect_column_values_to_not_be_in_set(self, column, values_set, mostly=None, suppress_exceptions=False):
        """Expect column entries to not be in the set.
        Args:
            column (str): The column name.
            values_set (list): The set of objects or unique data points that should not correspond to the column.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of values not in values_set is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_not_be_in_set

        """
        raise NotImplementedError

    def expect_column_values_to_be_between(self, column, min_value, max_value, mostly=None, suppress_exceptions=False):
        """Expect column entries to be a number between a minimum value and a maximum value.
        Args:
            column (str): The column name.
            min_value (int or None): The minimum value for a column entry.
            max_value (int or None): The maximum value for a column entry.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of values between min_value and max_value is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_value_lengths_to_be_between

        """
        raise NotImplementedError

    ##### String matching #####

    def expect_column_value_lengths_to_be_between(self, column, min_value, max_value, mostly=None, suppress_exceptions=False):
        """Expect column entries to have a measurable length which lies between a minimum value and a maximum value.
        Args:
            column (str): The column name.
            min_value (int or None): The minimum value for a column entry length.
            max_value (int or None): The maximum value for a column entry length.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of value lengths between min_value and max_value is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_be_between

        """
        raise NotImplementedError

    def expect_column_values_to_match_regex(self, column, regex, mostly=None, suppress_exceptions=False):
        """Expect column entries to be strings that match a given regular expression.
        Args:
            column (str): The column name.
            regex (str): The regular expression that the column entry should match.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of matches is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_not_match_regex
            expect_column_values_to_match_regex_list
        """
        raise NotImplementedError

    def expect_column_values_to_not_match_regex(self, column, regex, mostly=None, suppress_exceptions=False):
        """Expect column entries to be strings that do NOT match a given regular expression.
        Args:
            column (str): The column name.
            regex (str): The regular expression that the column entry should NOT match.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of NOT matches is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_match_regex
            expect_column_values_to_match_regex_list
        """
        raise NotImplementedError

    def expect_column_values_to_match_regex_list(self, column, regex_list, required_match="any", mostly=None, suppress_exceptions=False):
        """Expect the column entries to be strings that match at least one of a list of regular expressions.
        Args:
            column (str): The column name.
            regex_list (list): The list of regular expressions in which the column entries should match according to required_match.
        Keyword Args:
            required_match="any": Use "any" if the value should match at least one regular expression in the list. Use "all" if it should match each regular expression in the list.
            mostly=None: Return "success": True if the percentage of matches is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_match_regex
            expect_column_values_to_not_match_regex
        """
        raise NotImplementedError

    ##### Datetime and JSON parsing #####

    def expect_column_values_to_match_strftime_format(self, column, strftime_format, mostly=None, suppress_exceptions=False):
        """Expect column entries to be strings representing a date or time with a given format.
        WARNING: Note that strftime formats are not universally portable across implementations.
        For example, the %z directive may not be implemented before Python 3.2.
        Args:
            column (str): The column name.
            strftime_format (str): The datetime format that the column entries should match.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of matches is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        """
        raise NotImplementedError

    def expect_column_values_to_be_dateutil_parseable(self, column, mostly=None, suppress_exceptions=False):
        """Expect column entries to be interpretable as a dateutil object.
        Args:
            column (str): The column name.
        Keyword Args:
            mostly=None: Return "success": True if the percentage of parseable values is greater than or equal to mostly (a float between 0 and 1).
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        """
        raise NotImplementedError

    def expect_column_values_to_be_valid_json(self, column, suppress_exceptions=False):
        """Expect column entries to be data written in JavaScript Object Notation.
        Args:
            column (str): The column name.
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_match_json_schema
        """
        raise NotImplementedError

    def expect_column_values_to_match_json_schema(self, column, json_schema, suppress_exceptions=False):
        """Expect column entries to be JSON objects with a given JSON schema.
        Args:
            column (str): The column name.
            json_schema (JSON object): The JSON schema that each column entry should resemble.
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "exceptions_list": (list) the values that did not pass the expectation
                }
        See Also:
            expect_column_values_to_be_valid_json

        """
        raise NotImplementedError

    ##### Aggregate functions #####

    def expect_column_mean_to_be_between(self, column, min_value, max_value):
        """Expect the column mean to be between a minimum value and a maximum value.
        Args:
            column (str): The column name.
            min_value (int or None): The minimum value for the column mean.
            max_value (int or None): The maximum value for the column mean.
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "true_mean": (float) the column mean
                }
        """
        raise NotImplementedError

    def expect_column_median_to_be_between(self, column, min_value, max_value):
        """Expect the column median to be between a minimum value and a maximum value.
        Args:
            column (str): The column name.
            min_value (int or None): The minimum value for the column median.
            max_value (int or None): The maximum value for the column median.
        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "true_median": (float) the column median
                }
        """
        raise NotImplementedError

    def expect_column_stdev_to_be_between(self, column, min_value, max_value, suppress_exceptions=False):
        """Expect the column standard deviation to be between a minimum value and a maximum value.
        Args:
            column (str): The column name.
            min_value (int or None): The minimum value for the column standard deviation.
            max_value (int or None): The maximum value for the column standard deviation.

        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "true_stdev": (float) the true standard deviation
                }
        """
        raise NotImplementedError

    def expect_column_unique_value_count_to_be_between(self, column, min_value, max_value, output_format=None):
        """Expect the number of unique values to be between a minimum value and a maximum value.

        Args:
            column (str): The column name.
            min_value (int or None): The minimum number of unique values. If None, then there is no minimium expected value.
            max_value (int or None): The maximum number of unique values. If None, then there is no maximum expected value.

        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "true_value": (float) the column mean
                }
        """
        raise NotImplementedError

    def expect_column_proportion_of_unique_values_to_be_between(self, column, min_value, max_value, output_format=None):
        """Expect the proportion of unique values to be between a minimum value and a maximum value.

        Args:
            column (str): The column name.
            min_value (float or None): The minimum proportion of unique values. (Proportions are on the range 0 to 1)
            max_value (float or None): The maximum proportion of unique values. (Proportions are on the range 0 to 1)

        Returns:
            dict:
                {
                    "success": (bool) True if the column passed the expectation,
                    "true_value": (float) the proportion of unique values
                }
        """
        raise NotImplementedError

    def expect_column_chisquare_test_p_value_greater_than(self, series, partition_object=None, p=0.05):
        """
        Expect the values in this column to match the distribution of the specified categorical vals and their expected_frequencies. \

        Args:
            column (str): The column name
            vals (list): A list of categorical values.
            partition_object (dict): A dictionary containing partition (categorical values) and associated weights.
                - partition (list): A list of values that correspond to the provided categorical values.
                - weights (list): A list of weights. They should sum to one. The test will scale the expected frequencies by the weights and size of the new sample.
            p (float) = 0.05: The p-value threshold for the Chai Squareed test.\
                For values below the specified threshold the expectation will return false, rejecting the null hypothesis that the distributions are the same.
            suppress_exceptions: Only return a boolean success value, not a dictionary with other results.

        Returns:
            {
                "success": (bool) True if the column passed the expectation,
                "true_value": (float) the true pvalue of the ChiSquared test
            }
        """
        raise NotImplementedError

    def expect_column_bootstrapped_ks_test_p_value_greater_than(self, series, partition_object=None, p=0.05, bootstrap_samples=0):
        """
        Expect the values in this column to match the distribution implied by the specified partition and cdf_vals. \
        The implied CDF is constructed as a linear interpolation of the provided cdf_vals.

        Args:
            column (str): The column name
            partition_object (dict): A dictionary containing partition (bin edges) and associated weights.
                - partition (list): A list of values that correspond to the endpoints of an implied partition on the real number line.
                - weights (list): A list of weights. They should sum to one.
            p (float) = 0.05: The p-value threshold for the Kolmogorov-Smirnov test.\
                For values below the specified threshold the expectation will return false, rejecting the null hypothesis that the distributions are the same.
            bootstrap_samples (int) = 0: The number of bootstrapping rounds to use in building the estimated pvalue. \
                If zero, chooses a default number of rounds (currently 1000, but may be altered based on size of dataset).
        Returns:
            {
                "success": (bool) True if the column passed the expectation,
                "true_value": (float) the true pvalue of the KS test
            }
        """
        raise NotImplementedError

    def expect_column_kl_divergence_less_than(self, column, partition_object=None, threshold=None):
        """
        Expect the values in this column to have lower Kulback-Leibler divergence (relative entropy) with the distriution provided in partition_object of less than the provided threshold.

        Args:
            column (str): The column name
            partition_object (dict): A dictionary containing partition (bin edges) and associated weights.
                - partition (list): A list of values that correspond to the endpoints of an implied partition on the real number line.
                - weights (list): A list of weights. They should sum to one.
            threshold (float) = 0.1: The threshold of relative entropy.\
                For values above the specified threshold the expectation will return false.
            suppress_exceptions: Only return a boolean success value, not a dictionary with other results.

        Returns:
            {
                "success": (bool) True if the column passed the expectation,
                "true_value": (float) the true value of the KL divergence
            }
        """
        raise NotImplementedError
