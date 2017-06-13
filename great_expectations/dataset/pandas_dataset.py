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
    def expect_column_to_exist(self, col, suppress_exceptions=False):
        """Expect the specified column to exist in the data set.

        Args:
            col: The column name
            suppress_exceptions: Only return a boolean success value, not a dictionary with other results.

        Returns:
            By default: a dict containing "success" and "result" (an empty dictionary)
            On suppress_exceptions=True: a boolean success value only
        """

        if suppress_exceptions:
            col in self
        else:
            return {
                "success" : col in self,
                "result" : {}
            }

    @DataSet.column_expectation
    def expect_column_values_to_be_unique(self, col, mostly=None, suppress_exceptions=False):
        """
        Expect each not_null value in this column to be unique.

        Display multiple duplicated items.
        ['2','2','2'] will return `['2','2']` for the exceptions_list.

        !!! Prevent division-by-zero errors in the `mostly` logic
        """
        not_null = self[col].notnull()
        not_null_values = self[not_null][col]
        unique_not_null_values = set(not_null_values)

        if len(not_null_values) == 0:
            # print 'Warning: All values are null'
            return {'success':True,
                    'result':{'exception_list':[]}}

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
                'result' : {'exception_list' : exceptions}
            }
        else:
            return {
                'success' : (len(unique_not_null_values) == len(not_null_values)),
                'result' : {'exception_list' : exceptions}
            }

    @DataSet.column_expectation
    def expect_column_values_to_not_be_null(self, col, mostly=None, suppress_exceptions=False):
        """
        Expect values in this column to not be null.

        Instead of reinventing our own system for handling missing data, we use pandas.Series.isnull
        and notnull to define "null."
        See the pandas documentation for details.

        Note: When returning the list of exceptions, replace np.nan with None.

        !!! Prevent division-by-zero errors in the `mostly` logic
        """

        not_null = self[col].notnull()
        null_count = (not_null==False).sum()

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = [None for i in range(null_count)]

        if mostly:
            percent_not_null = 1 - float(null_count)/len(self[col])
            return {
                    'success' : (percent_not_null >= mostly),
                    'result' : {'exception_list' : exceptions}
                    }
        else:
            return {'success' : not_null.all(),
                    'result' : {'exception_list' : exceptions}}

    @DataSet.column_expectation
    def expect_column_values_to_match_regex(self, col, regex, mostly=None, suppress_exceptions=False):
        """
        docstring
        """
        not_null = self[col].notnull()
        not_null_values = self[not_null][col]

        if len(not_null_values) == 0:
            # print 'Warning: All values are null'
            return {'success':True,
                    'result':{'exception_list':[]}}

        matches = not_null_values.map(lambda x: re.findall(regex, str(x)) != [])

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[matches==False])

        if mostly:
            #Prevent division-by-zero errors
            if len(not_null_values) == 0:
                return {'success':True,
                        'result':{'exception_list':exceptions}}

            percent_matching = float(matches.sum())/len(not_null_values)
            return {
                "success" : percent_matching >= mostly,
                "result" : {
                    "exception_list" : exceptions
                }
            }
        else:
            return {
                "success" : matches.all(),
                "result" : {
                    "exception_list" : exceptions
                }
            }

    @DataSet.column_expectation
    def expect_column_values_to_match_strftime_format(self, col, format, mostly=None, suppress_exceptions=False):
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

        if (not (col in self)):
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
        not_null = self[col].notnull()
        not_null_values = self[not_null][col]

        properly_formatted = not_null_values.map(is_parseable_by_format)

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[properly_formatted==False])

        if mostly:
            #Prevent division-by-zero errors
            if len(not_null_values) == 0:
                return {'success':True,
                        'result':{'exception_list':exceptions}}

            percent_properly_formatted = float(sum(properly_formatted))/len(not_null_values)
            return {
                "success" : percent_properly_formatted >= mostly,
                "result" : {
                    "exception_list" : exceptions
                }
            }
        else:
            return {
                "success" : sum(properly_formatted) == len(not_null_values),
                "result" : {
                    "exception_list" : exceptions
                }
            }

    @DataSet.column_expectation
    def expect_column_values_to_be_in_set(self, col, values_set, mostly=None, suppress_exceptions=False):
        """
        !!! Prevent division-by-zero errors in the `mostly` logic
        """
        not_null = self[col].notnull()
        not_null_values = self[not_null][col]

        #Convert to set, if passed a list
        unique_values_set = set(values_set)

        unique_values = set(not_null_values.unique())

        if len(not_null_values) == 0:
            # print 'Warning: All values are null'
            return {'success':True,
                    'result':{'exception_list':[]}}

        exceptions_set = list(unique_values - unique_values_set)
        exceptions_list = list(not_null_values[not_null_values.map(lambda x: x in exceptions_set)])

        if mostly:

            percent_in_set = 1 - (float(len(exceptions_list)) / len(not_null_values))
            if suppress_exceptions:
                return percent_in_set > mostly
            else:
                return {
                    "success" : percent_in_set > mostly,
                    "result" : {
                        "exception_list" : exceptions_list
                    }
                }

        else:
            if suppress_exceptions:
                return (len(exceptions_set) == 0)
            else:
                return {
                    "success" : (len(exceptions_set) == 0),
                    "result" : {
                        "exception_list" : exceptions_list
                    }
                }

    @DataSet.column_expectation
    def expect_values_to_be_equal_across_columns(self, col, regex, mostly=None, suppress_exceptions=False):
        """
        """
        not_null = self[col].notnull()
        not_null_values = self[not_null][col]

        if len(not_null_values) == 0:
            # print 'Warning: All values are null'
            return (True, [])

        matches = not_null_values.map(lambda x: re.findall(regex, str(x)) != [])

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[matches==False])

        if mostly:
            #Prevent division-by-zero errors
            if len(not_null_values) == 0:
                return {'success' : True,
                        'result' : {'exception_list' : exceptions}}

            percent_matching = float(matches.sum())/len(not_null_values)
            return {'success' : (percent_matching >= mostly),
                    'result' : {'exception_list' : exceptions}}
        else:
            return {'success' : matches.all(),
                    'result' : {'exception_list' : exceptions}}

    @DataSet.column_expectation
    def expect_column_value_lengths_to_be_less_than_or_equal_to(self,col,N,suppress_exceptions=False):
        """
        DEPRECATED: see expect_column_value_lengths_to_be_between
        col: the name of the column
        N: (int) the value to compare with the column values

        Should this compare the number of digits in an integer or float or just compare directly to N?
        """
        not_null = self[col].notnull()
        not_null_values = self[not_null][col]

        #result = not_null_values.map(lambda x: len(str(x)) <= N)

        dtype = self[col].dtype
        # case that dtype is a numpy int or a float
        if dtype in set([np.dtype('float64'), np.dtype('int64'), int, float]):
            result = not_null_values < N
        else:
            try:
                result = not_null_values.map(lambda x: len(str(x)) <= N)
            except:
                raise TypeError("PandasDataSet.expect_column_value_lengths_to_be_less_than_or_equal_to cannot handle columns with dtype %s" % str(dtype), )

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[result==False])

        return {'success' : result.all(),
                'result' : {'exception_list' : exceptions}}

    @DataSet.column_expectation
    def expect_column_mean_to_be_between(self,col,M,N):
        """
        docstring
        """
        dtype = self[col].dtype
        not_null = self[col].notnull()
        not_null_values = self[not_null][col]
        try:
            result = (not_null_values.mean() >= M) and (not_null_values.mean() <= N)
            return {'success' : result,
                    'result' : {'exception_list' : not_null_values.mean()}}
        except:
            return {'success' : False,
                    'result' : {'exception_list' : None}}

    @DataSet.column_expectation
    def expect_column_values_to_be_null(self,col,mostly=None,suppress_exceptions=False):
        """
        docstring
        """
        null = self[col].isnull()
        not_null = self[col].notnull()
        null_values = self[null][col]
        not_null_values = self[not_null][col]

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values)

        if mostly:
            #Prevent division-by-zero errors
            percent_matching = float(null.sum())/len(self[col])
            return {'success':(percent_matching >= mostly),
                    'result':{'exception_list':exceptions}}
        else:
            return {'success':null.all(),
                    'result':{'exception_list':exceptions}}

    @DataSet.column_expectation
    def expect_column_values_to_not_match_regex(self,col,regex,mostly=None,suppress_exceptions=False):
        """
        docstring
        """
        not_null = self[col].notnull()
        not_null_values = self[not_null][col]

        if len(not_null_values) == 0:
            # print 'Warning: All values are null'
            return {'success':True,
                    'result':{'exception_list':[]}}

        matches = not_null_values.map(lambda x: re.findall(regex, str(x)) != [])
        does_not_match = not_null_values.map(lambda x: re.findall(regex, str(x)) == [])

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[matches==True])

        if mostly:
            #Prevent division-by-zero errors
            if len(not_null_values) == 0:
                return {'success':True,
                        'result':{'exception_list':exceptions}}

            percent_matching = float(does_not_match.sum())/len(not_null_values)
            return {'success':(percent_matching >= mostly),
                    'result':{'exception_list':exceptions}}
        else:
            return {'success':does_not_match.all(),
                    'result':{'exception_list':exceptions}}

    @DataSet.column_expectation
    def expect_column_values_to_be_between(self,col,M,N,mostly=None,suppress_exceptions=False):
        """
        docstring
        """
        not_null = self[col].notnull()
        not_null_values = self[not_null][col]
        result = (not_null_values >= M) & (not_null_values <= N)

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[result==False])

        if mostly:
            #Prevent division-by-zero errors
            if len(not_null_values) == 0:
                return {'success':True,
                        'result':{'exception_list':exceptions}}

            percent_true = float(result.sum())/len(not_null_values)
            return {'success':(percent_true >= mostly),
                    'result':{'exception_list':exceptions}}
        else:
            return {'success':result.all(),
                    'result':{'exception_list':exceptions}}

    @DataSet.column_expectation
    def expect_column_values_to_be_of_type(self,col,dtype,mostly=None,suppress_exceptions=False):
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
    def expect_column_values_to_not_be_in_set(self,col,S,mostly=None,suppress_exceptions=False):
        """
        docstring
        add negation kwarg to expectations?
        """
        not_null = self[col].notnull()
        not_null_values = self[not_null][col]
        result = not_null_values.isin(S)

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[result])

        if mostly:
            # prevent division by zero
            if len(not_null_values) == 0:
                return {'success':True,
                        'result':{'exception_list':exceptions}}

            percent_not_in_set = 1 - (float(result.sum())/len(not_null_values))
            return {'success':(percent_not_in_set >= mostly),
                    'result':{'exception_list':exceptions}}
        else:
            return {'success':(~result).all(),
                    'result':{'exception_list':exceptions}}


    @DataSet.column_expectation
    def expect_column_values_to_be_equal_across_columns(self,col1,col2,suppress_exceptions=False):
        """
        docstring
        """
        result = self[col1] == self[col2]

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = self[[col1,col2]][~result]

        return {'success':result.all(),
                'result':{'exception_list':exceptions}}


    @DataSet.column_expectation
    def expect_table_row_count_to_be_between(self,M,N,suppress_exceptions=False):
        """
        docstring
        should we count null values?
        """
        outcome = False
        if self.shape[0] >= M and self.shape[0] <= N:
            outcome = True

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = self.shape[0]

        return {'success':outcome,
                'result':{'true_row_count':exceptions}}


    @DataSet.column_expectation
    def expect_table_row_count_to_equal(self,N,suppress_exceptions=False):
        """
        docstring
        """
        outcome = False
        if self.shape[0] == N:
            outcome = True

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = self.shape[0]

        return {'success':outcome,
                'result':{'true_row_count':self.shape[0]}}


    @DataSet.column_expectation
    def expect_column_value_lengths_to_be_between(self,col,M=None,N=None,mostly=None,suppress_exceptions=False):
        """
        docstring
        """
        not_null = self[col].notnull()
        not_null_values = self[col][not_null]
        not_null_value_lengths = not_null_values.map(lambda x: len(x))

        if M != None and N != None:
            outcome = (not_null_value_lengths >= M) & (not_null_value_lengths <= N)
        elif M == None:
            outcome = not_null_value_lengths < N
        elif N == None:
            outcome = not_null_value_lengths > M
        else:
            raise ValueError("Undefined interval: M and N are None")

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[~outcome])

        if mostly:
            # prevent divide by zero error
            if len(not_null_values) == 0:
                return {'success' : True,
                        'result' : {'exception_list' : exceptions}}
            percent_true = float(sum(outcome))/len(outcome)
            return {'success' : (percent_true >= mostly),
                    'result' : {'exception_list' : exceptions}}
        else:
            return {'success' : outcome.all(),
                    'result' : {'exception_list' : exceptions}}


    @DataSet.column_expectation
    def expect_column_values_to_be_dateutil_parseable(self,col,mostly=None,suppress_exceptions=False):
        """
        docstring
        """
        def is_parseable(val):
            try:
                parser().parse(val)
                return True
            except:
                return False

        not_null = self[col].notnull()
        not_null_values = self[col][not_null]
        outcome = not_null_values.map(is_parseable)

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = not_null_values[~outcome]

        if mostly:
            # prevent divide by zero error
            if len(not_null_values) == 0:
                return {'success' : True,
                        'result' : {'exception_list' : exceptions}}

            percent_true = float(sum(outcome))/len(outcome)
            return {'success' : (percent_true >= mostly),
                    'result' : {'exception_list' : exceptions}}
        else:
            return {'success' : outcome.all(),
                    'result' : {'exception_list' : exceptions}}


    @DataSet.column_expectation
    def expect_column_values_to_be_valid_json(self,col,suppress_exceptions=False):
        """
        docstring
        """
        def is_json(val):
            try:
                json.loads(str(val))
                return True
            except:
                return False

        not_null = self[col].notnull()
        not_null_values = self[col][not_null]
        outcome = not_null_values.map(is_json)

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = not_null_values[~outcome]

        return {'success' : outcome.all(),
                'result' : {'exception_list' : exceptions}}


    @DataSet.column_expectation
    def expect_column_stdev_to_be_between(self,col,M,N,suppress_exceptions=False):
        """
        docstring
        """
        outcome = False
        if self[col].std() >= M and self[col].std() <= N:
            outcome = True

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = self[col].std()

        return {'success':outcome,
                'result':{'true_stdev':exceptions}}


    @DataSet.column_expectation
    def expect_two_column_values_to_be_subsets(self,col1,col2,mostly=None,suppress_exceptions=False):
        """
        docstring
        """
        C1 = set(self[col1])
        C2 = set(self[col2])

        outcome = False
        if C1.issubset(C2) or C2.issubset(C1):
            outcome = True

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = C1.union(C2) - C1.intersection(C2)

        if mostly:
            subset_proportion = 1 - float(len(C1.intersection(C2)))/len(C1.union(C2))
            return {'success':(subset_proportion >= mostly),
                    'result':{'not_in_subset':exceptions}}
        else:
            return {'success':outcome,
                    'result':{'not_in_subset':exceptions}}


    @DataSet.column_expectation
    def expect_two_column_values_to_be_many_to_one(self,col1,col2,mostly=None,suppress_exceptions=False):
        """
        docstring
        """
        raise NotImplementedError("Expectation is not yet implemented")


    @DataSet.column_expectation
    def expect_column_values_to_match_regex_list(self,col,regex_list,mostly=None,suppress_exceptions=False):
        """
        NOT STABLE
        docstring
        define test function first
        """
        outcome = list()
        exceptions = dict()
        for r in regex_list:
            out = expect_column_values_to_match_regex(col,r,mostly,suppress_exceptions)
            outcome.append(out['success'])
            exceptions[r] = out['result']['exception_list']

        if suppress_exceptions:
            exceptions = None

        if mostly:
            if len(outcome) == 0:
                return {'success':True,
                        'result':{'exception_list':exceptions}}

            percent_true = float(sum(outcome))/len(outcome)
            return {'success':(percent_true >= mostly),
                    'result':{'exception_list':exceptions}}
        else:
            return {'success':outcome.all(),
                    'result':{'exception_list':exceptions}}
