import pandas as pd
import numpy as np
from scipy import stats
import re
from dateutil.parser import parse
from datetime import datetime
import json
from functools import wraps


from .base import DataSet
from .util import is_valid_partition_object, remove_empty_intervals

class MetaPandasDataSet(DataSet):

    def __init__(self, *args, **kwargs):
        super(MetaPandasDataSet, self).__init__(*args, **kwargs)

    @classmethod
    def column_map_expectation(cls, func):

        @cls.expectation
        @wraps(func)
        def inner_wrapper(self, column, mostly=None, output_format=None, *args, **kwargs):

            if output_format == None:
                output_format = self.default_expectation_args["output_format"]

            series = self[column]
            null_indexes = series.isnull()

            nonnull_values = series[null_indexes==False]
            nonnull_count = (null_indexes==False).sum()

            successful_indexes = func(self, nonnull_values, *args, **kwargs)
            success_count = successful_indexes.sum()

            exception_list = list(series[(successful_indexes==False)&(null_indexes==False)])
            exception_index_list = list(series[(successful_indexes==False)&(null_indexes==False)].index)

            if nonnull_count > 0:
                percent_success = float(success_count)/nonnull_count

                if mostly:
                    success = percent_success >= mostly

                else:
                    success = len(exception_list) == 0

            else:
                success = True
                percent_success = None

            # print nonnull_count, success_count, percent_success, success

            if output_format=="BOOLEAN_ONLY":
                return_obj = success

            elif output_format=="BASIC":
                return_obj = {
                    "success" : success,
                    "exception_list" : exception_list,
                    "exception_index_list": exception_index_list,
                }

            elif output_format=="SUMMARY":
                element_count = int(len(series))
                missing_count = int(null_indexes.sum())
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
                    "success" : success,
                    "exception_list" : exception_list,
                    "exception_index_list": exception_index_list,
                    "element_count" : element_count,
                    "missing_count" : missing_count,
                    "missing_percent" : missing_percent,
                    "exception_count" : exception_count,
                    "exception_percent": exception_percent,
                    "exception_percent_nonmissing": exception_percent_nonmissing,
                    "exception_counts": exception_counts,
                }

            else:
                print ("Warning: Unknown output_format %s. Defaulting to BASIC." % (output_format,))
                return_obj = {
                    "success" : success,
                    "exception_list" : exception_list,
                }

            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__
        return inner_wrapper


    @classmethod
    def column_aggregate_expectation(cls, func):

        @cls.expectation
        @wraps(func)
        def inner_wrapper(self, column, output_format=None, *args, **kwargs):

            series = self[column]
            null_indexes = series.isnull()

            nonnull_values = series[null_indexes==False]
            nonnull_count = (null_indexes==False).sum()

            success, true_value = func(self, nonnull_values, *args, **kwargs)
            success = bool(success)

            if output_format in ["BASIC", "SUMMARY"]:
                return_obj = {
                    "success" : success,
                    "true_value" : true_value,
                }

            elif output_format=="BOOLEAN_ONLY":
                return_obj = success

            else:
                print ("Warning: Unknown output_format %s. Defaulting to BASIC." % (output_format,))
                return_obj = {
                    "success" : success,
                    "true_value" : true_value,
                }

            return return_obj

        return inner_wrapper

class PandasDataSet(MetaPandasDataSet, pd.DataFrame):

    def __init__(self, *args, **kwargs):
        super(PandasDataSet, self).__init__(*args, **kwargs)

    ### Expectation methods ###

    @DataSet.old_column_expectation
    def expect_column_to_exist(self, column, suppress_exceptions=False):

        if suppress_exceptions:
            column in self
        else:
            return {
                "success" : column in self
            }


    @DataSet.old_expectation
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
            'true_value':exceptions
        }


    @DataSet.old_expectation
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
            'true_value':self.shape[0]
        }


    @DataSet.old_column_expectation
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


    @DataSet.old_column_expectation
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

    @DataSet.old_column_expectation
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


    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_of_type(self, series, type_, target_datasource="numpy"):

        python_avro_types = {
                "null":type(None),
                "boolean":bool,
                "int":int,
                "long":int,
                "float":float,
                "double":float,
                "bytes":bytes,
                "string":str
                }

        numpy_avro_types = {
                "null":np.nan,
                "boolean":np.bool_,
                "int":np.int64,
                "long":np.longdouble,
                "float":np.float_,
                "double":np.longdouble,
                "bytes":np.bytes_,
                "string":np.string_
                }

        datasource = {"python":python_avro_types, "numpy":numpy_avro_types}

        target_type = datasource[target_datasource][type_]
        result = series.map(lambda x: type(x) == target_type)

        return result

    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_in_type_list(self, series, type_list, target_datasource="numpy"):

        python_avro_types = {
                "null":type(None),
                "boolean":bool,
                "int":int,
                "long":int,
                "float":float,
                "double":float,
                "bytes":bytes,
                "string":str
                }

        numpy_avro_types = {
                "null":np.nan,
                "boolean":np.bool_,
                "int":np.int64,
                "long":np.longdouble,
                "float":np.float_,
                "double":np.longdouble,
                "bytes":np.bytes_,
                "string":np.string_
                }

        datasource = {"python":python_avro_types, "numpy":numpy_avro_types}

        target_type_list = [datasource[target_datasource][t] for t in type_]
        result = series.map(lambda x: type(x) in target_type_list)

        return result


    # @DataSet.old_column_expectation
    # def expect_column_values_to_be_of_type(self, column, type_, target_datasource, mostly=None, suppress_exceptions=False):

    #     python_avro_types = {
    #             "null":type(None),
    #             "boolean":bool,
    #             "int":int,
    #             "long":int,
    #             "float":float,
    #             "double":float,
    #             "bytes":bytes,
    #             "string":str
    #             }

    #     numpy_avro_types = {
    #             "null":np.nan,
    #             "boolean":np.bool_,
    #             "int":np.int64,
    #             "long":np.longdouble,
    #             "float":np.float_,
    #             "double":np.longdouble,
    #             "bytes":np.bytes_,
    #             "string":np.string_
    #             }

    #     datasource = {"python":python_avro_types, "numpy":numpy_avro_types}

    #     user_type = datasource[target_datasource][type_]

    #     not_null = self[column].notnull()
    #     not_null_values = self[not_null][column]
    #     result = not_null_values.map(lambda x: type(x) == user_type)

    #     if suppress_exceptions:
    #         exceptions = None
    #     else:
    #         exceptions = list(not_null_values[~result])

    #     if mostly:
    #         # prevent division by zero error
    #         if len(not_null_values) == 0:
    #             return {
    #                 'success':True,
    #                 'exception_list':exceptions
    #             }

    #         percent_true = float(result.sum())/len(not_null_values)
    #         return {
    #             'success':(percent_true >= mostly),
    #             'exception_list':exceptions
    #         }
    #     else:
    #         return {
    #             'success':result.all(),
    #             'exception_list':exceptions
    #         }


    @DataSet.old_column_expectation
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


    @DataSet.old_column_expectation
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

    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_between(self, series, min_value=None, max_value=None):

        if min_value != None and max_value != None:
            return series.map(
                lambda x: (min_value <= x) and (x <= max_value)
            )

        elif min_value == None and max_value != None:
            return series.map(
                lambda x: (x <= max_value)
            )

        elif min_value != None and max_value == None:
            return series.map(
                lambda x: (min_value <= x)
            )

        else:
            raise ValueError("min_value and max_value cannot both be None")

    # @DataSet.old_column_expectation
    # def expect_column_values_to_be_between(self, column, min_value, max_value, mostly=None, suppress_exceptions=False):

    #     not_null = self[column].notnull()
    #     not_null_values = self[not_null][column]

    #     def is_between(val):
    #         try:
    #             return val >= min_value and val <= max_value
    #         except:
    #             return False

    #     result = not_null_values.map(is_between)

    #     if suppress_exceptions:
    #         exceptions = None
    #     else:
    #         exceptions = list(not_null_values[result==False])

    #     if mostly:
    #         #Prevent division-by-zero errors
    #         if len(not_null_values) == 0:
    #             return {
    #                 'success':True,
    #                 'exception_list':exceptions
    #             }

    #         percent_true = float(result.sum())/len(not_null_values)
    #         return {
    #             'success':(percent_true >= mostly),
    #             'exception_list':exceptions
    #         }

    #     else:
    #         return {
    #             'success': bool(result.all()),
    #             'exception_list':exceptions
    #         }

    @DataSet.old_column_expectation
    def expect_column_value_lengths_to_be_between(self, column, min_value, max_value, mostly=None, suppress_exceptions=False):

        not_null = self[column].notnull()
        not_null_values = self[column][not_null]

        def length_is_between(val):

            if min_value != None and max_value != None:
                try:
                    return len(val) >= min_value and len(val) <= max_value
                except:
                    return False

            elif min_value == None and max_value != None:
                return len(val) <= max_value

            elif min_value != None and max_value == None:
                return len(val) >= min_value

            else:
                raise ValueError("Undefined interval: min_value and max_value are both None")

        outcome = not_null_values.map(length_is_between)

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

    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_match_regex(self, series, regex):
        return series.map(
            lambda x: re.findall(regex, str(x)) != []
        )


    # @DataSet.old_column_expectation
    # def expect_column_values_to_match_regex(self, column, regex, mostly=None, suppress_exceptions=False):

    #     not_null = self[column].notnull()
    #     not_null_values = self[not_null][column]

    #     if len(not_null_values) == 0:
    #         # print 'Warning: All values are null'
    #         return {
    #             'success':True,
    #             'exception_list':[]
    #         }

    #     matches = not_null_values.map(lambda x: re.findall(regex, str(x)) != [])

    #     if suppress_exceptions:
    #         exceptions = None
    #     else:
    #         exceptions = list(not_null_values[matches==False])

    #     if mostly:
    #         #Prevent division-by-zero errors
    #         if len(not_null_values) == 0:
    #             return {
    #                 'success': True,
    #                 'exception_list':exceptions
    #             }

    #         percent_matching = float(matches.sum())/len(not_null_values)
    #         return {
    #             "success" : percent_matching >= mostly,
    #             "exception_list" : exceptions
    #         }
    #     else:
    #         return {
    #             "success" : bool(matches.all()),
    #             "exception_list" : exceptions
    #         }


    @DataSet.old_column_expectation
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


    @DataSet.old_column_expectation
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


    @DataSet.old_column_expectation
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

    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_dateutil_parseable(self, series):
        def is_parseable(val):
            try:
                parse(val)
                return True
            except:
                return False

        return series.map(is_parseable)

    # @DataSet.old_column_expectation
    # def expect_column_values_to_be_dateutil_parseable(self, column, mostly=None, suppress_exceptions=False):

    #     def is_parseable(val):
    #         try:
    #             parse(val)
    #             return True
    #         except:
    #             return False

    #     not_null = self[column].notnull()
    #     not_null_values = self[column][not_null]
    #     outcome = not_null_values.map(is_parseable)

    #     if suppress_exceptions:
    #         exceptions = None
    #     else:
    #         exceptions = list(not_null_values[~outcome])

    #     if mostly:
    #         # prevent divide by zero error
    #         if len(not_null_values) == 0:
    #             return {
    #                 'success' : True,
    #                 'exception_list' : exceptions
    #             }

    #         percent_true = float(sum(outcome))/len(outcome)

    #         return {
    #             'success' : (percent_true >= mostly),
    #             'exception_list' : exceptions
    #         }

    #     else:
    #         return {
    #             'success' : bool(outcome.all()),
    #             'exception_list' : exceptions
    #         }


    @DataSet.old_column_expectation
    def expect_column_values_to_be_valid_json(self, column, mostly=None, suppress_exceptions=False):

        def is_json(val):
            try:
                json.loads(val)
                return True
            except:
                return False

        not_null = self[column].notnull()
        not_null_values = self[column][not_null]
        outcome = not_null_values.map(is_json)

        if suppress_exceptions:
            exceptions = None
        else:
            exceptions = list(not_null_values[~outcome])

        if mostly:
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

        return {
            'success' : outcome.all(),
            'exception_list' : exceptions
        }


    @DataSet.old_column_expectation
    def expect_column_values_to_match_json_schema(self):
        raise NotImplementedError("Under development")


    @DataSet.old_column_expectation
    def expect_column_mean_to_be_between(self, column, min_value, max_value):

        dtype = self[column].dtype
        not_null = self[column].notnull()
        not_null_values = self[not_null][column]
        try:
            result = (not_null_values.mean() >= min_value) and (not_null_values.mean() <= max_value)
            return {
                'success' : bool(result),
                'true_value' : not_null_values.mean()
            }
        except:
            return {
                'success' : False,
                'true_value' : None
            }


    @DataSet.old_column_expectation
    def expect_column_median_to_be_between(self):
        raise NotImplementedError("Under Development")


    @DataSet.old_column_expectation
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
            'true_value':exceptions
        }

    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_unique_value_count_to_be_between(self, series, min_value=None, max_value=None, output_format=None):
        unique_value_count = series.value_counts().shape[0]

        if min_value != None and max_value != None:
            return (
                (min_value <= unique_value_count) and
                (unique_value_count <= max_value)
            ), unique_value_count
            
        elif min_value == None and max_value != None:
            return (x <= max_value), unique_value_count

        elif min_value != None and max_value == None:
            return (min_value <= x), unique_value_count

        else:
            raise ValueError("min_value and max_value cannot both be None")

    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_proportion_of_unique_values_to_be_between(self, series, min_value=0, max_value=1, output_format=None):
        unique_value_count = series.value_counts().shape[0]
        total_value_count = series.notnull().sum()

        if denominator > 0:
            proportion_unique = (1. * unique_value_count) / total_value_count
        else:
            proportion_unique = None

        return (min_value <= proportion_unique <= max_value), proportion_unique

    @DataSet.old_column_expectation
    def expect_column_frequency_distribution_to_be(self, column, partition_object, p=0.05, suppress_exceptions=False):
        if not is_valid_partition_object(partition_object):
            return {
                "success": False,
                "error": "Invalid partition_object"
            }

        expected_series = pd.Series(partition_object['weights'], index=partition_object['partition'], name='expected') * len(self[column])
        observed_frequencies = self[column].value_counts()
        # Join along the indicies to ensure we have values
        test_df = pd.concat([expected_series, observed_frequencies], axis = 1).fillna(0)
        test_result = stats.chisquare(test_df[column], test_df['expected'])

        if suppress_exceptions:
            return {
                "success" : test_result.pvalue > p,
            }
        else:
            return {
                "success" : test_result.pvalue > p,
                "true_value" : test_result.pvalue,
            }

    @DataSet.old_column_expectation
    def expect_column_numerical_distribution_to_be(self, column, partition_object, bootsrap_samples=0, p=0.05, suppress_exceptions=False):
        if not is_valid_partition_object(partition_object):
            return {
                "success": False,
                "error": "Invalid partition_object"
            }
        not_null = self[column].notnull()
        not_null_values = self[not_null][column]

        estimated_cdf = lambda x: np.interp(x, partition_object['partition'], np.append(np.array([0]), np.cumsum(partition_object['weights'])))

        if (bootsrap_samples == 0):
            #bootsrap_samples = min(1000, int (len(not_null_values) / len(partition_object['weights'])))
            bootsrap_samples = 1000

        results = [ stats.kstest(
                        np.random.choice(not_null_values, size=len(partition_object['weights']), replace=True),
                        estimated_cdf).pvalue
                    for k in range(bootsrap_samples)
                  ]

        test_result = np.mean(results)

        if suppress_exceptions:
            return {
                "success" : test_result > p,
            }
        else:
            return {
                "success" : test_result > p,
                "true_value" : test_result
            }

    @DataSet.old_column_expectation
    def expect_column_kl_divergence_to_be(self, column, partition_object, threshold, suppress_exceptions=False):
        if not is_valid_partition_object(partition_object):
            # return {
            #     "success": False,
            #     "error": "Invalid partition_object"
            # }
            raise ValueError("Invalid partition_object")

        if not (isinstance(threshold, float) and (threshold >= 0)):
            raise ValueError("Threshold must be a float greater than zero.")

        not_null = self[column].notnull()
        not_null_values = self[not_null][column]

        # If the data expected to be discrete, build a series
        if (len(partition_object['weights']) == len(partition_object['partition'])):
            observed_frequencies = not_null_values.value_counts()
            pk = observed_frequencies / (1.* len(not_null_values))
        else:
            partition_object = remove_empty_intervals(partition_object)
            hist, bin_edges = np.histogram(not_null_values, partition_object['partition'], density=False)
            pk = hist / (1.* len(not_null_values))

        kl_divergence = stats.entropy(pk, partition_object['weights'])

        if suppress_exceptions:
            return {
                "success" : kl_divergence <= threshold,
            }
        else:
            return {
                "success" : kl_divergence <= threshold,
                "true_value" : kl_divergence
            }
