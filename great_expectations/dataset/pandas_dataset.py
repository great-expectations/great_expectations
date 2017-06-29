import pandas as pd
import numpy as np
import re
from dateutil.parser import parse
from datetime import datetime
import json

from .base import DataSet

class PandasDataSet(DataSet, pd.DataFrame):

    def __init__(self, *args, **kwargs):
        super(PandasDataSet, self).__init__(*args, **kwargs)

    ### Expectation methods ###

    @DataSet.column_expectation
    def expect_column_to_exist(self, column, suppress_exceptions=False):

        if suppress_exceptions:
            column in self
        else:
            return {
                "success" : column in self
            }


    @DataSet.column_expectation
    def expect_table_row_count_to_be_between(self, min_value, max_value,suppress_exceptions=False):

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
    def expect_column_values_to_be_unique(self, column, mostly=None, suppress_exceptions=False):

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
    def expect_column_values_to_be_null(self, column, mostly=None, suppress_exceptions=False):

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


#    @DataSet.column_expectation
#    def expect_column_values_to_be_of_type(self, column, type_, target_datasource, mostly=None, suppress_exceptions=False):
#
#        python_avro_types = {
#                "null":None, 
#                "boolean":bool, 
#                "int":int, 
#                "long":int, 
#                "float":float, 
#                "double":float, 
#                "bytes":bytes, 
#                "string":str
#                }
#
#        numpy_avro_types = {
#                "null":np.nan, 
#                "boolean":np.bool_, 
#                "int":np.int64, 
#                "long":np.longdouble, 
#                "float":np.float_, 
#                "double":np.longdouble, 
#                "bytes":np.bytes_, 
#                "string":np.string_
#                }
#
#        datasource = {"python":python_avro_types, "numpy":numpy_avro_types}
#
#        user_type = datasource[target_datasource][type_]
#
#        not_null = self[column].notnull()
#        not_null_values = self[not_null][column]
#        result = not_null_values.map(lambda x: type(x) == user_type)
#
#        if suppress_exceptions:
#            exceptions = None
#        else:
#            exceptions = not_null_values[~result]
#
#        if mostly:
#            # prevent division by zero error
#            if len(not_null_values) == 0:
#                return True,exceptions
#
#            percent_true = float(result.sum())/len(not_null_values)
#            return (percent_true >= mostly),exceptions
#        else:
#            return result.all(),exceptions


    @DataSet.column_expectation
    def expect_column_values_to_be_in_set(self, column, values_set, mostly=None, suppress_exceptions=False):

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


    @DataSet.column_expectation
    def expect_column_values_to_not_be_in_set(self, column, values_set, mostly=None, suppress_exceptions=False):

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


    @DataSet.column_expectation
    def expect_column_values_to_be_between(self, column, min_value, max_value, mostly=None, suppress_exceptions=False):

        not_null = self[column].notnull()
        not_null_values = self[not_null][column]

        def is_between(val):
            try:
                return val >= min_value and val <= max_value
            except:
                return False
            
        result = not_null_values.map(is_between)

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
    def expect_column_value_lengths_to_be_between(self, column, min_value, max_value, mostly=None, suppress_exceptions=False):

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
    def expect_column_values_to_match_regex(self, column, regex, mostly=None, suppress_exceptions=False):

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
    def expect_column_values_to_not_match_regex(self, column, regex, mostly=None, suppress_exceptions=False):

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
    def expect_column_values_to_match_regex_list(self, column, regex_list, mostly=None, suppress_exceptions=False):

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


    @DataSet.column_expectation
    def expect_column_values_to_match_strftime_format(self, column, format, mostly=None, suppress_exceptions=False):

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
    def expect_column_values_to_be_dateutil_parseable(self, column, mostly=None, suppress_exceptions=False):

        def is_parseable(val):
            try:
                parse(val)
                return True
            except:
                return False

        not_null = self[column].notnull()
        not_null_values = self[column][not_null]
        outcome = not_null_values.map(is_parseable)

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
    def expect_column_values_to_be_valid_json(self, column, suppress_exceptions=False):

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
    def expect_column_values_to_match_json_schema(self):
        raise NotImplementedError("Under development")


    @DataSet.column_expectation
    def expect_column_mean_to_be_between(self, column, min_value, max_value):

        dtype = self[column].dtype
        not_null = self[column].notnull()
        not_null_values = self[not_null][column]
        try:
            result = (not_null_values.mean() >= min_value) and (not_null_values.mean() <= max_value)
            return {
                'success' : result,
                'true_mean' : not_null_values.mean()
            }
        except:
            return {
                'success' : False,
                'true_mean' : None
            }


    @DataSet.column_expectation
    def expect_column_median_to_be_between(self):
        raise NotImplementedError("Under Development")


    @DataSet.column_expectation
    def expect_column_stdev_to_be_between(self, column, min_value, max_value, suppress_exceptions=False):

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
        raise NotImplementedError("Expectation is not yet implemented")



