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
                    "summary_obj" : {
                        "element_count" : element_count,
                        "missing_count" : missing_count,
                        "missing_percent" : missing_percent,
                        "exception_count" : exception_count,
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

            result_obj = func(self, nonnull_values, *args, **kwargs)

            #!!! This would be the right place to validate result_obj
            #!!! It should contain:
            #!!!    success: bool
            #!!!    true_value: int or float
            #!!!    summary_obj: json-serializable dict

            if output_format == "BASIC":
                return_obj = {
                    "success" : bool(result_obj["success"]),
                    "true_value" : result_obj["true_value"],
                }

            elif output_format == "SUMMARY":
                return_obj = {
                    "success" : bool(result_obj["success"]),
                    "true_value" : result_obj["true_value"],
                    "summary_obj" : result_obj["summary_obj"]
                }

            elif output_format=="BOOLEAN_ONLY":
                return_obj = bool(result_obj["success"])

            else:
                print ("Warning: Unknown output_format %s. Defaulting to BASIC." % (output_format,))
                return_obj = {
                    "success" : bool(result_obj["success"]),
                    "true_value" : result_obj["true_value"],
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


    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_unique(self, series):
        dupes = set(series[series.duplicated()])
        return series.map(lambda x: x not in dupes)


    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_not_be_null(self, series):
        return series.notnull()


    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_null(self, series):
        return series.isnull()


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



    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_in_set(self, series, value_set=None):
        return series.map(lambda x: x in value_set)


    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_not_be_in_set(self, series, value_set=None):
        return series.map(lambda x: x not in value_set)


    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_between(self, series, min_value=None, max_value=None):

        def is_between(val):
            # TODO Might be worth explicitly defining comparisons between types (for example, between strings and ints).
            # Ensure types can be compared since some types in Python 3 cannot be logically compared.
            if type(val) == None:
                return False
            else:
                try:

                    if min_value != None and max_value != None:
                        return (min_value <= val) and (val <= max_value)

                    elif min_value == None and max_value != None:
                        return (val <= max_value)

                    elif min_value != None and max_value == None:
                        return (min_value <= val)

                    else:
                        raise ValueError("min_value and max_value cannot both be None")
                except:
                    return False

        return series.map(is_between)


    @MetaPandasDataSet.column_map_expectation
    def expect_column_value_lengths_to_be_between(self, series, min_value=None, max_value=None):
        #TODO should the mapping function raise the error or should the decorator?
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

        return series.map(length_is_between)


    @MetaPandasDataSet.column_map_expectation
    def expect_column_value_lengths_to_equal(self, series, value):
        return series.map(lambda x : len(x) == value)


    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_match_regex(self, series, regex):
        return series.map(
            lambda x: re.findall(regex, str(x)) != []
        )


    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_not_match_regex(self, column, regex):
        return series.map(lambda x: re.findall(regex, str(x)) == [])


    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_match_regex_list(self, series, regex_list):

        def match_in_list(val):
            if any(re.match(regex, str(val)) for regex in regex_list):
                return True
            else:
                return False

        return series.map(match_in_list)


    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_match_strftime_format(self, series, strftime_format):
        ## Below is a simple validation that the provided format can both format and parse a datetime object.
        ## %D is an example of a format that can format but not parse, e.g.
        try:
            datetime.strptime(datetime.strftime(datetime.now(), strftime_format), strftime_format)
        except ValueError as e:
            raise ValueError("Unable to use provided format. " + e.message)

        def is_parseable_by_format(val):
            try:
                # Note explicit cast of val to str type
                datetime.strptime(str(val), strftime_format)
                return True
            except ValueError as e:
                return False

        return series.map(is_parseable_by_format)

        #TODO Add the following to the decorator as a preliminary check.
        #if (not (column in self)):
        #    raise LookupError("The specified column does not exist.")


    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_dateutil_parseable(self, series):
        def is_parseable(val):
            try:
                parse(val)
                return True
            except:
                return False

        return series.map(is_parseable)


    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_json_parseable(self, series):
        def is_json(val):
            try:
                json.loads(val)
                return True
            except:
                return False

        return series.map(is_json)


    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_match_json_schema(self):
        raise NotImplementedError("Under development")


    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_mean_to_be_between(self, series, min_value, max_value):

        #!!! Does not raise an error if both min_value and max_value are None.
        column_mean = series.mean()

        return {
            "success" : (
                ((min_value <= column_mean) | (min_value == None)) and
                ((column_mean <= max_value) | (max_value == None))
            ),
            "true_value" : column_mean,
            "summary_obj" : {}
        }


    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_median_to_be_between(self, series, min_value, max_value):

        #!!! Does not raise an error if both min_value and max_value are None.
        column_median = series.median()

        return {
            "success" : (
                ((min_value <= column_median) | (min_value == None)) and
                ((column_median <= max_value) | (max_value == None))
            ),
            "true_value" : column_median,
            "summary_obj" : {}
        }


    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_stdev_to_be_between(self, series, min_value, max_value):

        #!!! Does not raise an error if both min_value and max_value are None.
        column_stdev = series.std()

        return {
            "success" : (
                ((min_value <= column_stdev) | (min_value == None)) and
                ((column_stdev <= max_value) | (max_value == None))
            ),
            "true_value" : column_stdev,
            "summary_obj" : {}
        }


    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_unique_value_count_to_be_between(self, series, min_value=None, max_value=None, output_format=None):
        unique_value_count = series.value_counts().shape[0]

        return {
            "success" : (
                ((min_value <= unique_value_count) | (min_value == None)) and
                ((unique_value_count <= max_value) | (max_value ==None))
            ),
            "true_value" : unique_value_count,
            "summary_obj" : {}
        }

    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_proportion_of_unique_values_to_be_between(self, series, min_value=0, max_value=1, output_format=None):
        unique_value_count = series.value_counts().shape[0]
        total_value_count = series.notnull().sum()

        if denominator > 0:
            proportion_unique = (1. * unique_value_count) / total_value_count
        else:
            proportion_unique = None

        return {
            "success" : (
                ((min_value <= proportion_unique) | (min_value == None)) and
                ((proportion_unique <= max_value) | (max_value ==None))
            ),
            "true_value" : proportion_unique,
            "summary_obj" : {}
        }

    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_chisquare_test_p_value_greater_than(self, series, partition_object=None, p=0.05):
        if not is_valid_partition_object(partition_object):
            raise ValueError("Invalid partition object.")

        expected_series = pd.Series(partition_object['weights'], index=partition_object['partition'], name='expected') * len(series)
        observed_frequencies = series.value_counts()
        # Join along the indicies to ensure we have values
        test_df = pd.concat([expected_series, observed_frequencies], axis = 1).fillna(0)
        test_result = stats.chisquare(test_df[series.name], test_df['expected'])[1]

        result_obj = {
                "success" : test_result > p,
                "true_value": test_result,
                "summary_obj": {}
            }

        return result_obj

    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_bootstrapped_ks_test_p_value_greater_than(self, series, partition_object=None, bootstrap_samples=0, p=0.05):
        if not is_valid_partition_object(partition_object):
            raise ValueError("Invalid partition object.")

        estimated_cdf = lambda x: np.interp(x, partition_object['partition'], np.append(np.array([0]), np.cumsum(partition_object['weights'])))

        if (bootsrap_samples == 0):
            #bootsrap_samples = min(1000, int (len(not_null_values) / len(partition_object['weights'])))
            bootsrap_samples = 1000

        results = [ stats.kstest(
                        np.random.choice(series, size=len(partition_object['weights']), replace=True),
                        estimated_cdf).pvalue
                    for k in range(bootsrap_samples)
                  ]

        test_result = np.mean(results)

        result_obj = {
                "success" : test_result > p,
                "true_value": test_result,
                "summary_obj": {
                    "bootsrap_samples": bootsrap_samples
                }
            }

        return result_obj


    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_kl_divergence_less_than(self, series, partition_object=None, threshold=None):
        if not is_valid_partition_object(partition_object):
            raise ValueError("Invalid partition object.")

        if not (isinstance(threshold, float) and (threshold >= 0)):
            raise ValueError("Threshold must be specified, greater than or equal to zero.")


        # If the data expected to be discrete, build a series
        if (len(partition_object['weights']) == len(partition_object['partition'])):
            observed_frequencies = series.value_counts()
            pk = observed_frequencies / (1.* len(series))
        else:
            partition_object = remove_empty_intervals(partition_object)
            hist, bin_edges = np.histogram(series, partition_object['partition'], density=False)
            pk = hist / (1.* len(series))

        kl_divergence = stats.entropy(pk, partition_object['weights'])

        result_obj = {
                "success" : kl_divergence <= threshold,
                "true_value" : kl_divergence,
                "summary_obj": {}
            }

        return result_obj
