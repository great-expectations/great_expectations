import inspect
import json
import re
from datetime import datetime
from functools import wraps
import jsonschema

import numpy as np
import pandas as pd
from dateutil.parser import parse
from scipy import stats

from .base import DataSet
from .util import DocInherit, is_valid_partition_object, remove_empty_intervals


class MetaPandasDataSet(DataSet):

    def __init__(self, *args, **kwargs):
        super(MetaPandasDataSet, self).__init__(*args, **kwargs)


    @classmethod
    def column_map_expectation(cls, func):
        """
        The column_map_expectation decorator handles boilerplate issues surrounding the common pattern of evaluating
        truthiness of some condition on a per row basis.

        NOTE: The MetaPandasDataSet implementation replaces the "column" parameter supplied by the user with a pandas Series
        object containing the actual column from the relevant pandas dataframe. This simplifies the implementing expectation
        logic while preserving the standard DataSet signature and expected behavior.

        Further, the column_map_expectation provides a unique set of output_format options and handles the optional "mostly" parameter.
        """

        @cls.expectation(inspect.getargspec(func)[0][1:])
        @wraps(func)
        def inner_wrapper(self, column, mostly=None, output_format=None, *args, **kwargs):

            if output_format is None:
                output_format = self.default_expectation_args["output_format"]

            series = self[column]
            boolean_mapped_null_values = series.isnull()

            element_count = int(len(series))
            nonnull_values = series[boolean_mapped_null_values==False]
            nonnull_count = (boolean_mapped_null_values==False).sum()

            boolean_mapped_success_values = func(self, nonnull_values, *args, **kwargs)
            success_count = boolean_mapped_success_values.sum()

            exception_list = list(series[(boolean_mapped_success_values==False)&(boolean_mapped_null_values==False)])
            exception_index_list = list(series[(boolean_mapped_success_values==False)&(boolean_mapped_null_values==False)].index)
            exception_count = len(exception_list)

            success, percent_success = self.calc_map_expectation_success(success_count, nonnull_count, exception_count, mostly)

            return_obj = self.format_column_map_output(
                output_format, success,
                element_count,
                nonnull_values, nonnull_count,
                boolean_mapped_success_values, success_count,
                exception_list, exception_index_list
            )

            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__
        return inner_wrapper


    @classmethod
    def column_aggregate_expectation(cls, func):
        """
        The column_aggregate_expectation decorator handles boilerplate issues surrounding computing aggregate measures
        from all nonnull values in a column.

        NOTE: The MetaPandasDataSet implementation replaces the "column" parameter supplied by the user with a pandas
        Series object containing the actual column from the relevant pandas dataframe. This simplifies the implementing
        expectation logic while preserving the standard DataSet signature and expected behavior.

        Further, the column_aggregate_expectation provides a unique set of output_format options.
        """
        @cls.expectation(inspect.getargspec(func)[0][1:])
        @wraps(func)
        def inner_wrapper(self, column, output_format = None, *args, **kwargs):

            if output_format is None:
                output_format = self.default_expectation_args["output_format"]

            series = self[column]
            null_indexes = series.isnull()

            nonnull_values = series[null_indexes == False]
            nonnull_count = (null_indexes == False).sum()
            null_count = nonnull_count - len(series)

            result_obj = func(self, nonnull_values, *args, **kwargs)

            #!!! This would be the right place to validate result_obj
            #!!! It should contain:
            #!!!    success: bool
            #!!!    true_value: int or float
            #!!!    summary_obj: json-serializable dict

            if output_format in ["BASIC", "COMPLETE"]:
                return_obj = {
                    "success" : bool(result_obj["success"]),
                    "true_value" : result_obj["true_value"],
                }

            elif (output_format == "SUMMARY"):
                if "summary_obj" in result_obj and result_obj["summary_obj"] is not None:
                    result_obj["summary_obj"].update({
                        "element_count": nonnull_count,
                        "missing_count": null_count,
                        "missing_percent": nonnull_count / null_count if null_count > 0 else 0
                    })
                else:
                    result_obj["summary_obj"] = {
                        "element_count": nonnull_count,
                        "missing_count": null_count,
                        "missing_percent": nonnull_count / null_count if null_count > 0 else 0
                    }
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
        self.add_default_expectations()

    def add_default_expectations(self):
        ## Default behavior for PandasDataSet is to explicitly include expectations that every column present
        ## upon initialization exists.
        for col in self.columns:
            self.append_expectation({
                "expectation_type": "expect_column_to_exist",
                "kwargs": {
                    "column": col
                }
            })

    ### Expectation methods ###
    @DocInherit
    @DataSet.expectation(['column'])
    def expect_column_to_exist(self, column, output_format=None, include_config=False, catch_exceptions=None):
        if column in self:
            return {
                "success" : True
            }
        else:
            return {
                "success": False
            }

    @DocInherit
    @DataSet.expectation(['min_value', 'max_value'])
    def expect_table_row_count_to_be_between(self, min_value=0, max_value=None, output_format=None, include_config=False, catch_exceptions=None):
        # Assert that min_value and max_value are integers
        if min_value is not None and not float(min_value).is_integer():
            raise ValueError("min_value and max_value must be integers")

        if max_value is not None and not float(max_value).is_integer():
            raise ValueError("min_value and max_value must be integers")

        if min_value <= self.shape[0] <= max_value:
            outcome = True
        else:
            outcome = False

        return {
            'success':outcome,
            'true_value': self.shape[0]
        }

    @DocInherit
    @DataSet.expectation(['value'])
    def expect_table_row_count_to_equal(self, value=None, output_format=None, include_config=False, catch_exceptions=None):
        if value is not None and not float(value).is_integer():
            raise ValueError("value must be an integer")

        if self.shape[0] == value:
            outcome = True
        else:
            outcome = False

        return {
            'success':outcome,
            'true_value':self.shape[0]
        }

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_unique(self, column, mostly=None, output_format=None, include_config=False, catch_exceptions=None):
        dupes = set(column[column.duplicated()])
        return column.map(lambda x: x not in dupes)

    @DocInherit
    @DataSet.expectation(['column', 'mostly', 'output_format'])
    def expect_column_values_to_not_be_null(self, column, mostly=None, output_format=None, include_config=False, catch_exceptions=None):
        if output_format is None:
            output_format = self.default_expectation_args["output_format"]

        series = self[column]
        boolean_mapped_null_values = series.isnull()

        element_count = int(len(series))
        nonnull_values = series[boolean_mapped_null_values==False]
        nonnull_count = (boolean_mapped_null_values==False).sum()

        boolean_mapped_success_values = boolean_mapped_null_values==False
        success_count = boolean_mapped_success_values.sum()

        exception_list = [None for i in list(series[(boolean_mapped_success_values==False)])]
        exception_index_list = list(series[(boolean_mapped_success_values==False)].index)
        exception_count = len(exception_list)

        # Pass element_count instead of nonnull_count, because that's the right denominator for this expectation
        success, percent_success = self.calc_map_expectation_success(success_count, element_count, exception_count, mostly)

        return_obj = self.format_column_map_output(
            output_format, success,
            element_count,
            nonnull_values, nonnull_count,
            boolean_mapped_success_values, success_count,
            exception_list, exception_index_list
        )

        return return_obj

    @DocInherit
    @DataSet.expectation(['column', 'mostly', 'output_format'])
    def expect_column_values_to_be_null(self, column, mostly=None, output_format=None, include_config=False, catch_exceptions=None):
        if output_format is None:
            output_format = self.default_expectation_args["output_format"]

        series = self[column]
        boolean_mapped_null_values = series.isnull()

        element_count = int(len(series))
        nonnull_values = series[boolean_mapped_null_values==False]
        nonnull_count = (boolean_mapped_null_values==False).sum()

        boolean_mapped_success_values = boolean_mapped_null_values
        success_count = boolean_mapped_success_values.sum()

        exception_list = list(series[(boolean_mapped_success_values==False)])
        exception_index_list = list(series[(boolean_mapped_success_values==False)].index)
        exception_count = len(exception_list)

        # Pass element_count instead of nonnull_count, because that's the right denominator for this expectation
        success, percent_success = self.calc_map_expectation_success(success_count, element_count, exception_count, mostly)

        return_obj = self.format_column_map_output(
            output_format, success,
            element_count,
            nonnull_values, nonnull_count,
            boolean_mapped_success_values, success_count,
            exception_list, exception_index_list
        )

        return return_obj

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_of_type(self, column, type_, target_datasource, mostly=None, output_format=None, include_config=False, catch_exceptions=None):
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
        result = column.map(lambda x: type(x) == target_type)

        return result

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_in_type_list(self, column, type_list, target_datasource="numpy"):

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

        target_type_list = [datasource[target_datasource][t] for t in type_list]
        result = column.map(lambda x: type(x) in target_type_list)

        return result

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_in_set(self, column, values_set, mostly=None, output_format=None, include_config=False, catch_exceptions=None):
        return column.map(lambda x: x in values_set)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_not_be_in_set(self, column, values_set, mostly=None, output_format=None, include_config=False, catch_exceptions=None):
        return column.map(lambda x: x not in values_set)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_between(self, column, min_value=None, max_value=None, mostly=None, output_format=None, include_config=False, catch_exceptions=None):
        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

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
                        return False

                except:
                    return False

        return column.map(is_between)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_value_lengths_to_be_between(self, column, min_value=None, max_value=None, mostly=None, output_format=None, include_config=False, catch_exceptions=None):

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        # Assert that min_value and max_value are integers
        if min_value is not None and not float(min_value).is_integer():
            raise ValueError("min_value and max_value must be integers")

        if max_value is not None and not float(max_value).is_integer():
            raise ValueError("min_value and max_value must be integers")

        def length_is_between(val):

            try:
                if min_value != None and max_value != None:
                    return len(val) >= min_value and len(val) <= max_value

                elif min_value == None and max_value != None:
                    return len(val) <= max_value

                elif min_value != None and max_value == None:
                    return len(val) >= min_value

                else:
                    return False
            except:
                return False

        return column.map(length_is_between)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_value_lengths_to_equal(self, column, value):
        return column.map(lambda x : len(x) == value)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_match_regex(self, column, regex, mostly=None, output_format=None, include_config=False, catch_exceptions=None):
        return column.map(
            lambda x: re.findall(regex, str(x)) != []
        )

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_not_match_regex(self, column, regex, mostly=None, output_format=None, include_config=False, catch_exceptions=None):
        return column.map(lambda x: re.findall(regex, str(x)) == [])

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_match_regex_list(self, column, regex_list, required_match="any", mostly=None, output_format=None, include_config=False, catch_exceptions=None):

        def match_in_list(val):
            if any(re.match(regex, str(val)) for regex in regex_list):
                return True
            else:
                return False

        return column.map(match_in_list)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_match_strftime_format(self, column, strftime_format, mostly=None, output_format=None, include_config=False, catch_exceptions=None):
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

        return column.map(is_parseable_by_format)

        #TODO Add the following to the decorator as a preliminary check.
        #if (not (column in self)):
        #    raise LookupError("The specified column does not exist.")

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_dateutil_parseable(self, column, mostly=None, output_format=None, include_config=False, catch_exceptions=None):
        def is_parseable(val):
            try:
                parse(val)
                return True
            except:
                return False

        return column.map(is_parseable)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_json_parseable(self, column, output_format=None, include_config=False, catch_exceptions=None):
        def is_json(val):
            try:
                json.loads(val)
                return True
            except:
                return False

        return column.map(is_json)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_match_json_schema(self, column, json_schema, output_format=None, include_config=False, catch_exceptions=None):
        def matches_json_schema(val):
            try:
                val_json = json.loads(val)
                jsonschema.validate(val_json, json_schema)
                #jsonschema.validate raises an error if validation fails.
                #So if we make it this far, we know that the validation succeeded.
                return True
            except:
                return False

        return column.map(matches_json_schema)


    # @DocInherit
    # @MetaPandasDataSet.column_map_expectation
    # def expect_column_values_to_match_json_schema(self, column, json_schema, output_format=None, include_config=False, catch_exceptions=None):
    #     raise NotImplementedError("Under development")

    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_mean_to_be_between(self, column, min_value=None, max_value=None, output_format=None, include_config=False, catch_exceptions=None):

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        column_mean = column.mean()

        return {
            "success": (
                ((min_value is None) or (min_value <= column_mean)) and
                ((max_value is None) or (column_mean <= max_value))
            ),
            "true_value": column_mean,
            "summary_obj": {}
        }

    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_median_to_be_between(self, column, min_value=None, max_value=None, output_format=None, include_config=False, catch_exceptions=None):

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        column_median = column.median()

        return {
            "success": (
                ((min_value or None) or (min_value <= column_median)) and
                ((max_value or None) or (column_median <= max_value))
            ),
            "true_value": column_median,
            "summary_obj": {}
        }

    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_stdev_to_be_between(self, column, min_value=None, max_value=None, output_format=None, include_config=False, catch_exceptions=None):

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        column_stdev = column.std()

        return {
            "success": (
                ((min_value is None) or (min_value <= column_stdev)) and
                ((max_value is None) or (column_stdev <= max_value))
            ),
            "true_value": column_stdev,
            "summary_obj": {}
        }

    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_unique_value_count_to_be_between(self, column, min_value=None, max_value=None, output_format=None, include_config=False, catch_exceptions=None):

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        unique_value_count = column.value_counts().shape[0]

        return {
            "success" : (
                ((min_value is None) or (min_value <= unique_value_count)) and
                ((max_value is None) or (unique_value_count <= max_value))
            ),
            "true_value": unique_value_count,
            "summary_obj": {}
        }

    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_most_common_value_to_be(self, column, value, ties_okay=None, output_format=None, include_config=False, catch_exceptions=None):

        mode_list = list(column.mode().values)

        if ties_okay:
            success = value in mode_list
        else:
            if len(mode_list) > 1:
                success = False
            else:
                success = value == mode_list[0]

        return {
            "success" : success,
            "true_value": mode_list,
            "summary_obj": {},
        }

    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_most_common_value_to_be_in_set(self, column, value_set, ties_okay=None, output_format=None, include_config=False, catch_exceptions=None):

        mode_list = list(column.mode().values)
        intersection_count = len(set(value_set).intersection(mode_list))

        if ties_okay:
            success = intersection_count>0
        else:
            if len(mode_list) > 1:
                success = False
            else:
                success = intersection_count==1

        return {
            "success" : success,
            "true_value": mode_list,
            "summary_obj": {},
        }

    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_proportion_of_unique_values_to_be_between(self, column, min_value=0, max_value=1, output_format=None, include_config=False, catch_exceptions=None):
        unique_value_count = column.value_counts().shape[0]
        total_value_count = int(len(column))#.notnull().sum()

        if total_value_count > 0:
            proportion_unique = float(unique_value_count) / total_value_count
        else:
            proportion_unique = None

        return {
            "success": (
                ((min_value is None) or (min_value <= proportion_unique)) and
                ((max_value is None) or (proportion_unique <= max_value))
            ),
            "true_value": proportion_unique,
            "summary_obj": {}
        }

    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_chisquare_test_p_value_greater_than(self, column, partition_object=None, p=0.05, output_format=None, include_config=False, catch_exceptions=None):
        if not is_valid_partition_object(partition_object):
            raise ValueError("Invalid partition object.")

        expected_column = pd.Series(partition_object['weights'], index=partition_object['partition'], name='expected') * len(column)
        observed_frequencies = column.value_counts()
        # Join along the indicies to ensure we have values
        test_df = pd.concat([expected_column, observed_frequencies], axis = 1).fillna(0)
        test_result = stats.chisquare(test_df[column.name], test_df['expected'])[1]

        result_obj = {
                "success": test_result > p,
                "true_value": test_result,
                "summary_obj": {}
            }

        return result_obj

    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_bootstrapped_ks_test_p_value_greater_than(self, column, partition_object=None, p=0.05, bootstrap_samples=0, output_format=None, include_config=False, catch_exceptions=None):
        if not is_valid_partition_object(partition_object):
            raise ValueError("Invalid partition object.")

        estimated_cdf = lambda x: np.interp(x, partition_object['partition'], np.append(np.array([0]), np.cumsum(partition_object['weights'])))

        if (bootstrap_samples == 0):
            #bootstrap_samples = min(1000, int (len(not_null_values) / len(partition_object['weights'])))
            bootstrap_samples = 1000

        results = [ stats.kstest(
                        np.random.choice(column, size=len(partition_object['weights']), replace=True),
                        estimated_cdf)[1]
                    for k in range(bootstrap_samples)
                  ]

        test_result = np.mean(results)

        result_obj = {
                "success" : test_result > p,
                "true_value": test_result,
                "summary_obj": {
                    "bootstrap_samples": bootstrap_samples
                }
            }

        return result_obj

    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_kl_divergence_less_than(self, column, partition_object=None, threshold=None, output_format=None, include_config=False, catch_exceptions=None):
        if not is_valid_partition_object(partition_object):
            raise ValueError("Invalid partition object.")

        if not (isinstance(threshold, float) and (threshold >= 0)):
            raise ValueError("Threshold must be specified, greater than or equal to zero.")

        # If the data expected to be discrete, build a column
        if (len(partition_object['weights']) == len(partition_object['partition'])):
            observed_frequencies = column.value_counts()
            pk = observed_frequencies / (1.* len(column))
        else:
            partition_object = remove_empty_intervals(partition_object)
            hist, bin_edges = np.histogram(column, partition_object['partition'], density=False)
            pk = hist / (1.* len(column))

        kl_divergence = stats.entropy(pk, partition_object['weights'])

        result_obj = {
                "success": kl_divergence <= threshold,
                "true_value": kl_divergence,
                "summary_obj": {}
            }

        return result_obj
