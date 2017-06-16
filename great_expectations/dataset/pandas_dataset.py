import pandas as pd
import numpy as np
import re
from dateutil.parser import parser
from datetime import datetime
import json

from base import DataSet

class PandasDataSet(DataSet, pd.DataFrame):

    def __init__(self, *args, **kwargs):
        super(PandasDataSet, self).__init__(*args, **kwargs)

    ### Expectation methods ###

    @DataSet.column_expectation
    def expect_column_to_exist(self, column, suppress_exceptions=False):
        """Expect the specified column to exist in the data set.

        Args:
            column: The column name
            suppress_exceptions: Only return a boolean success value, not a dictionary with other results.

        Returns:
            By default: a dict containing "success" and "result" (an empty dictionary)
            On suppress_exceptions=True: a boolean success value only
        """

        if suppress_exceptions:
            column in self
        else:
            return {
                "success" : column in self
            }

    @DataSet.column_expectation
    def expect_column_values_to_be_unique(self, column, mostly=None, suppress_exceptions=False):
        """
        Expect each not_null value in this column to be unique.

        Display multiple duplicated items.
        ['2','2','2'] will return `['2','2']` for the exceptions_list.

        !!! Prevent division-by-zero errors in the `mostly` logic
        """
        not_null = self[column].notnull()
        not_null_values = self[not_null][column]
        unique_not_null_values = set(not_null_values)

        if len(not_null_values) == 0:
            # print 'Warning: All values are null'
            return {
                'success': True,
                'exception_list':[]
            }

        if suppress_exceptions:
            exceptions = None
        elif list(not_null_values.duplicated()) == []:
            # If all values are null .duplicated returns no boolean values
            exceptions = None
        else:
            exceptions = list(not_null_values[not_null_values.duplicated()])

        if mostly:
            percent_unique = 1 - float(len(unique_not_null_values))/len(not_null_values)
            return {
                'success' : (percent_unique >= mostly),
                'exception_list' : exceptions
            }
        else:
            return {
                'success' : (len(unique_not_null_values) == len(not_null_values)),
                'exception_list' : exceptions
            }

    @DataSet.column_expectation
    def expect_column_values_to_not_be_null(self, column, mostly=None, suppress_exceptions=False):
        """
        Expect values in this column to not be null.

        Instead of reinventing our own system for handling missing data, we use pandas.Series.isnull
        and notnull to define "null."
        See the pandas documentation for details.

        Note: When returning the list of exceptions, replace np.nan with None.

        !!! Prevent division-by-zero errors in the `mostly` logic
        """

        not_null = self[column].notnull()
        null_count = (not_null==False).sum()

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = [None for i in range(null_count)]

        if mostly:
            percent_not_null = 1 - float(null_count)/len(self[column])
            return {
                'success' : (percent_not_null >= mostly),
                'exception_list' : exceptions
            }
        else:
            return {
                'success' : not_null.all(),
                'exception_list' : exceptions
            }


    @DataSet.column_expectation
    def expect_column_values_to_match_regex(self, column, regex, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        not_null = self[column].notnull()
        not_null_values = self[not_null][column]

        if len(not_null_values) == 0:
            # print 'Warning: All values are null'
            return {
                'success':True,
                'exception_list':[]
            }

        matches = not_null_values.map(lambda x: re.findall(regex, str(x)) != [])

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[matches==False])

        if mostly:
            #Prevent division-by-zero errors
            if len(not_null_values) == 0:
                return {
                    'success':True,
                    'exception_list':exceptions
                }

            percent_matching = float(matches.sum())/len(not_null_values)
            return {
                "success" : percent_matching >= mostly,
                "exception_list" : exceptions
            }
        else:
            return {
                "success" : matches.all(),
                "exception_list" : exceptions
            }

    @DataSet.column_expectation
    def expect_column_values_to_match_strftime_format(self, column, format, mostly=None, suppress_exceptions=False):
        """
        Expect values in this column to match the user-provided datetime format.
        WARNING: Note that strftime formats are not universally portable across implementations.
        For example, the %z directive may not be implemented before python 3.2.

        Args:
            col (string): The column name
            format (string): The format string against which values should be validated
            mostly (float): The proportion of values that must match the condition for success to be true.
            suppress_exceptions: Only return a boolean success value, not a dictionary with other results.

        Returns:
            By default: a dict containing "success" and "result" (an empty dictionary)
            On suppress_exceptions=True: a boolean success value only
        """

        if (not (column in self)):
            raise LookupError("The specified column does not exist.")

        # Below is a simple validation that the provided format can both format and parse a datetime object.
        # %D is an example of a format that can format but not parse, e.g.
        try:
            datetime.strptime(datetime.strftime(datetime.now(), format), format)
        except ValueError as e:
            raise ValueError("Unable to use provided format. " + e.message)

        def is_parseable_by_format(val):
            try:
                # Note explicit cast of val to str type
                datetime.strptime(str(val), format)
                return True
            except ValueError as e:
                return False

        ## TODO: Should null values be considered exceptions?
        not_null = self[column].notnull()
        not_null_values = self[not_null][column]

        properly_formatted = not_null_values.map(is_parseable_by_format)

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[properly_formatted==False])

        if mostly:
            #Prevent division-by-zero errors
            if len(not_null_values) == 0:
                return {
                    'success':True,
                    'exception_list':exceptions
                }

            percent_properly_formatted = float(sum(properly_formatted))/len(not_null_values)
            return {
                "success" : percent_properly_formatted >= mostly,
                "exception_list" : exceptions
            }
        else:
            return {
                "success" : sum(properly_formatted) == len(not_null_values),
                "exception_list" : exceptions
            }

    @DataSet.column_expectation
    def expect_column_values_to_be_in_set(self, column, values_set, mostly=None, suppress_exceptions=False):
        """
        !!! Prevent division-by-zero errors in the `mostly` logic
        """
        not_null = self[column].notnull()
        not_null_values = self[not_null][column]

        #Convert to set, if passed a list
        unique_values_set = set(values_set)

        unique_values = set(not_null_values.unique())

        if len(not_null_values) == 0:
            # print 'Warning: All values are null'
            return {
                'success':True,
                'exception_list':[]
            }

        exceptions_set = list(unique_values - unique_values_set)
        exceptions_list = list(not_null_values[not_null_values.map(lambda x: x in exceptions_set)])

        if mostly:

            percent_in_set = 1 - (float(len(exceptions_list)) / len(not_null_values))
            if suppress_exceptions:
                return percent_in_set > mostly
            else:
                return {
                    "success" : percent_in_set > mostly,
                    "exception_list" : exceptions_list
                }

        else:
            if suppress_exceptions:
                return (len(exceptions_set) == 0)
            else:
                return {
                    "success" : (len(exceptions_set) == 0),
                    "exception_list" : exceptions_list
                }

    #!!! Deprecated
    # @DataSet.column_expectation
    # def expect_values_to_be_equal_across_columns(self, column, regex, mostly=None, suppress_exceptions=False):
    #     """
    #     """
    #     not_null = self[column].notnull()
    #     not_null_values = self[not_null][column]

    #     if len(not_null_values) == 0:
    #         # print 'Warning: All values are null'
    #         return (True, [])

    #     matches = not_null_values.map(lambda x: re.findall(regex, str(x)) != [])

    #     if suppress_exceptions:
    #         exceptions = None
    #     else:
    #         exceptions = list(not_null_values[matches==False])

    #     if mostly:
    #         #Prevent division-by-zero errors
    #         if len(not_null_values) == 0:
    #             return {
    #                 'success' : True,
    #                 'exception_list' : exceptions
    #             }

    #         percent_matching = float(matches.sum())/len(not_null_values)
    #         return {
    #             'success' : (percent_matching >= mostly),
    #             'exception_list' : exceptions
    #         }
    #     else:
    #         return {
    #             'success' : matches.all(),
    #             'exception_list' : exceptions
    #         }

    @DataSet.column_expectation
    def expect_column_value_lengths_to_be_less_than_or_equal_to(self, column, length, suppress_exceptions=False):
        """
        DEPRECATED: see expect_column_value_lengths_to_be_between
        col: the name of the column
        N: (int) the value to compare with the column values

        Should this compare the number of digits in an integer or float or just compare directly to N?
        """
        not_null = self[column].notnull()
        not_null_values = self[not_null][column]

        #result = not_null_values.map(lambda x: len(str(x)) <= N)

        dtype = self[column].dtype
        # case that dtype is a numpy int or a float
        if dtype in set([np.dtype('float64'), np.dtype('int64'), int, float]):
            result = not_null_values < length
        else:
            try:
                result = not_null_values.map(lambda x: len(str(x)) <= length)
            except:
                raise TypeError("PandasDataSet.expect_column_value_lengths_to_be_less_than_or_equal_to cannot handle columns with dtype %s" % str(dtype), )

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[result==False])

        return {
            'success' : result.all(),
            'exception_list' : exceptions
        }

    @DataSet.column_expectation
    def expect_column_mean_to_be_between(self, column, min_value, max_value):
        """
        docstring
        """
        dtype = self[column].dtype
        not_null = self[column].notnull()
        not_null_values = self[not_null][column]
        try:
            result = (not_null_values.mean() >= min_value) and (not_null_values.mean() <= max_value)
            return {
                'success' : result,
                'exception_list' : not_null_values.mean()
            }
        except:
            return {
                'success' : False,
                'exception_list' : None
            }

    @DataSet.column_expectation
    def expect_column_values_to_be_null(self, column, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        null = self[column].isnull()
        not_null = self[column].notnull()
        null_values = self[null][column]
        not_null_values = self[not_null][column]

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values)

        if mostly:
            #Prevent division-by-zero errors
            percent_matching = float(null.sum())/len(self[column])
            return {
                'success':(percent_matching >= mostly),
                'exception_list':exceptions
            }
        else:
            return {
                'success':null.all(),
                'exception_list':exceptions
            }

    @DataSet.column_expectation
    def expect_column_values_to_not_match_regex(self, column, regex, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        not_null = self[column].notnull()
        not_null_values = self[not_null][column]

        if len(not_null_values) == 0:
            # print 'Warning: All values are null'
            return {
                'success':True,
                'exception_list':[]
            }

        matches = not_null_values.map(lambda x: re.findall(regex, str(x)) != [])
        does_not_match = not_null_values.map(lambda x: re.findall(regex, str(x)) == [])

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[matches==True])

        if mostly:
            #Prevent division-by-zero errors
            if len(not_null_values) == 0:
                return {
                    'success':True,
                    'exception_list':exceptions
                }

            percent_matching = float(does_not_match.sum())/len(not_null_values)
            return {
                'success':(percent_matching >= mostly),
                'exception_list':exceptions
            }
        else:
            return {
                'success':does_not_match.all(),
                'exception_list':exceptions
            }

    @DataSet.column_expectation
    def expect_column_values_to_be_between(self, column, min_value, max_value, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        not_null = self[column].notnull()
        not_null_values = self[not_null][column]
        result = (not_null_values >= min_value) & (not_null_values <= max_value)

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[result==False])

        if mostly:
            #Prevent division-by-zero errors
            if len(not_null_values) == 0:
                return {
                    'success':True,
                    'exception_list':exceptions
                }

            percent_true = float(result.sum())/len(not_null_values)
            return {
                'success':(percent_true >= mostly),
                'exception_list':exceptions
            }
        
        else:
            return {
                'success':result.all(),
                'exception_list':exceptions
            }

    @DataSet.column_expectation
    def expect_column_values_to_be_of_type(self, column, dtype, mostly=None, suppress_exceptions=False):
        """
        NOT STABLE
        docstring
        """
        raise NotImplementedError("This method is under development.")
        #dtype_dict = {np.dtype("float64"):"double precision",np.dtype("O"):"text",np.dtype("bool"):"boolean",np.dtype("int64"):"integer"}
        #not_null = self[col].notnull()
        #not_null_values = self[not_null][col]
        #result = not_null_values.map(lambda x: type(x) == dtype)

        #if suppress_exceptions:
        #    exceptions = None
        #else:
        #    exceptions = not_null_values[~result]

        #if mostly:
        #    # prevent division by zero error
        #    if len(not_null_values) == 0:
        #        return True,exceptions

        #    percent_true = float(result.sum())/len(not_null_values)
        #    return (percent_true >= mostly),exceptions
        #else:
        #    return result.all(),exceptions

    @DataSet.column_expectation
    def expect_column_values_to_not_be_in_set(self, column, values_set, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        not_null = self[column].notnull()
        not_null_values = self[not_null][column]
        result = not_null_values.isin(values_set)

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[result])

        if mostly:
            # prevent division by zero
            if len(not_null_values) == 0:
                return {
                    'success':True,
                    'exception_list':exceptions
                }

            percent_not_in_set = 1 - (float(result.sum())/len(not_null_values))
            return {
                'success':(percent_not_in_set >= mostly),
                'exception_list':exceptions
            }

        else:
            return {
                'success':(~result).all(),
                'exception_list':exceptions
            }


    #!!! Deprecated
    # @DataSet.column_expectation
    # def expect_column_values_to_be_equal_across_columns(self, column_1, column_2, suppress_exceptions=False):
    #     """
    #     docstring
    #     """
    #     result = self[column_1] == self[column_2]

    #     if suppress_exceptions:
    #         exceptions = None
    #     else:
    #         exceptions = self[[column_1, column_2]][~result]

    #     return {
    #         'success': result.all(),
    #         'exception_list': exceptions
    #     }


    @DataSet.column_expectation
    def expect_table_row_count_to_be_between(self, min_value, max_value,suppress_exceptions=False):
        """
        docstring
        should we count null values?
        """
        outcome = False
        if self.shape[0] >= min_value and self.shape[0] <= max_value:
            outcome = True

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = self.shape[0]

        return {
            'success':outcome,
            'true_row_count':exceptions
        }


    @DataSet.column_expectation
    def expect_table_row_count_to_equal(self, value, suppress_exceptions=False):
        """
        docstring
        """
        outcome = False
        if self.shape[0] == value:
            outcome = True

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = self.shape[0]

        return {
            'success':outcome,
            'true_row_count':self.shape[0]
        }


    @DataSet.column_expectation
    def expect_column_value_lengths_to_be_between(self, column, min_value, max_value, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        not_null = self[column].notnull()
        not_null_values = self[column][not_null]
        not_null_value_lengths = not_null_values.map(lambda x: len(x))

        if min_value != None and max_value != None:
            outcome = (not_null_value_lengths >= min_value) & (not_null_value_lengths <= max_value)

        elif min_value == None:
            outcome = not_null_value_lengths < max_value

        elif max_value == None:
            outcome = not_null_value_lengths > min_value

        else:
            raise ValueError("Undefined interval: min_value and max_value are both None")

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[~outcome])

        if mostly:
            # prevent divide by zero error
            if len(not_null_values) == 0:
                return {
                    'success' : True,
                    'exception_list' : exceptions
                }

            percent_true = float(sum(outcome))/len(outcome)
            
            return {
                'success' : (percent_true >= mostly),
                'exception_list' : exceptions
            }

        else:
            return {
                'success' : outcome.all(),
                'exception_list' : exceptions
            }


    @DataSet.column_expectation
    def expect_column_values_to_be_dateutil_parseable(self, column, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        def is_parseable(val):
            try:
                parser().parse(val)
                return True
            except:
                return False

        not_null = self[column].notnull()
        not_null_values = self[column][not_null]
        outcome = not_null_values.map(is_parseable)

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = not_null_values[~outcome]

        if mostly:
            # prevent divide by zero error
            if len(not_null_values) == 0:
                return {
                    'success' : True,
                    'exception_list' : exceptions
                }

            percent_true = float(sum(outcome))/len(outcome)
            
            return {
                'success' : (percent_true >= mostly),
                'exception_list' : exceptions
            }

        else:
            return {
                'success' : outcome.all(),
                'exception_list' : exceptions
            }


    @DataSet.column_expectation
    def expect_column_values_to_be_valid_json(self, column, suppress_exceptions=False):
        """
        docstring
        """
        def is_json(val):
            try:
                json.loads(str(val))
                return True
            except:
                return False

        not_null = self[column].notnull()
        not_null_values = self[column][not_null]
        outcome = not_null_values.map(is_json)

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = not_null_values[~outcome]

        return {
            'success' : outcome.all(),
            'exception_list' : exceptions
        }


    @DataSet.column_expectation
    def expect_column_stdev_to_be_between(self, column, min_value, max_value, suppress_exceptions=False):
        """
        docstring
        """
        outcome = False
        if self[column].std() >= min_value and self[column].std() <= max_value:
            outcome = True

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = self[column].std()

        return {
            'success':outcome,
            'true_stdev':exceptions
        }


    @DataSet.column_expectation
    def expect_two_column_values_to_be_subsets(self, column_1, column_2, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        C1 = set(self[column_1])
        C2 = set(self[column_2])

        outcome = False
        if C1.issubset(C2) or C2.issubset(C1):
            outcome = True

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = C1.union(C2) - C1.intersection(C2)

        if mostly:
            subset_proportion = 1 - float(len(C1.intersection(C2)))/len(C1.union(C2))
            return {
                'success':(subset_proportion >= mostly),
                'not_in_subset':exceptions
            }
        else:
            return {
                'success':outcome,
                'not_in_subset':exceptions
            }


    @DataSet.column_expectation
    def expect_two_column_values_to_be_many_to_one(self, column_1, column_2, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        raise NotImplementedError("Expectation is not yet implemented")


    @DataSet.column_expectation
    def expect_column_values_to_match_regex_list(self, column, regex_list, mostly=None, suppress_exceptions=False):
        """
        NOT STABLE
        docstring
        define test function first
        """
        outcome = list()
        exceptions = dict()
        for r in regex_list:
            out = expect_column_values_to_match_regex(column,r,mostly,suppress_exceptions)
            outcome.append(out['success'])
            exceptions[r] = out['result']['exception_list']

        if suppress_exceptions:
            exceptions = None

        if mostly:
            if len(outcome) == 0:
                return {
                    'success':True,
                    'exception_list':exceptions
                }

            percent_true = float(sum(outcome))/len(outcome)
            return {
                'success':(percent_true >= mostly),
                'exception_list':exceptions
            }
        else:
            return {
                'success':outcome.all(),
                'exception_list':exceptions
            }
