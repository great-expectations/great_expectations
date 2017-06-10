import pandas as pd
import numpy as np
import re

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
            return (True, [])

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
            return (True, [])

        matches = not_null_values.map(lambda x: re.findall(regex, str(x)) != [])

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[matches==False])

        if mostly:
            #Prevent division-by-zero errors
            if len(not_null_values) == 0:
                return True, exceptions

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
            return (True, [])

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
            return (percent_matching >= mostly), exceptions
        else:
            return null.all(), exceptions

    @DataSet.column_expectation
    def expect_column_values_to_not_match_regex(self,col,regex,mostly=None,suppress_exceptions=False):
        """
        docstring
        """
        not_null = self[col].notnull()
        not_null_values = self[not_null][col]

        if len(not_null_values) == 0:
            # print 'Warning: All values are null'
            return (True, [])

        matches = not_null_values.map(lambda x: re.findall(regex, str(x)) != [])
        does_not_match = not_null_values.map(lambda x: re.findall(regex, str(x)) == [])

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[matches==True])

        if mostly:
            #Prevent division-by-zero errors
            if len(not_null_values) == 0:
                return True, exceptions

            percent_matching = float(does_not_match.sum())/len(not_null_values)
            return (percent_matching >= mostly), exceptions
        else:
            return does_not_match.all(), exceptions

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
                return True, exceptions

            percent_true = float(result.sum())/len(not_null_values)
            return (percent_true >= mostly), exceptions
        else:
            return result.all(),exceptions

    @DataSet.column_expectation
    def expect_column_values_to_be_of_type(self,col,dtype,mostly=None,suppress_exceptions=False):
        """
        NOT STABLE
        docstring
        From the test file, it's unclear how to implement this function
        """
        dtype_dict = {np.dtype("float64"):"double precision",np.dtype("O"):"text",np.dtype("bool"):"boolean",np.dtype("int64"):"integer"}
        not_null = self[col].notnull()
        not_null_values = self[not_null][col]
        result = not_null_values.map(lambda x: type(x) == dtype)

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = not_null_values[~result]

        if mostly:
            # prevent division by zero error
            if len(not_null_values) == 0:
                return True,exceptions

            percent_true = float(result.sum())/len(not_null_values)
            return (percent_true >= mostly),exceptions
        else:
            return result.all(),exceptions

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
                return True,exceptions

            percent_not_in_set = 1 - (float(result.sum())/len(not_null_values))
            return (percent_not_in_set >= mostly),exceptions
        else:
            return (~result).all(),exceptions

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

        return result.all(),exceptions

    @DataSet.column_expectation
    def expect_table_row_count_to_be_between(self):
        raise NotImplementedError("Expectation is not yet implemented")

    @DataSet.column_expectation
    def expect_table_row_count_to_equal(self):
        raise NotImplementedError("Expectation is not yet implemented")

    @DataSet.column_expectation
    def expect_column_value_lengths_to_be_between(self):
        raise NotImplementedError("Expectation is not yet implemented")

    @DataSet.column_expectation
    def expect_column_values_to_match_regex_list(self):
        raise NotImplementedError("Expectation is not yet implemented")

    @DataSet.column_expectation
    def expect_column_values_to_be_dateutil_parseable(self):
        raise NotImplementedError("Expectation is not yet implemented")

    @DataSet.column_expectation
    def expect_column_values_to_be_valid_json(self):
        raise NotImplementedError("Expectation is not yet implemented")

    @DataSet.column_expectation
    def expect_column_stdev_to_be_between(self):
        raise NotImplementedError("Expectation is not yet implemented")

    @DataSet.column_expectation
    def expect_two_column_values_to_be_subsets(self):
        raise NotImplementedError("Expectation is not yet implemented")

    @DataSet.column_expectation
    def expect_two_column_values_to_be_many_to_one(self):
        raise NotImplementedError("Expectation is not yet implemented")



