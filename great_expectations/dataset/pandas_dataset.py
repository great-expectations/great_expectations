from __future__ import division

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
from six import string_types

from .base import DataSet
from .util import DocInherit, recursively_convert_to_json_serializable, \
        is_valid_partition_object, is_valid_categorical_partition_object, is_valid_continuous_partition_object

class MetaPandasDataSet(DataSet):
    """
    MetaPandasDataSet is a thin layer between DataSet and PandasDataSet. This two-layer inheritance is required to make @classmethod decorators work.

    Practically speaking, that means that MetaPandasDataSet implements
    expectation decorators, like `column_map_expectation` and `column_aggregate_expectation`,
    and PandasDataset implements the expectation methods themselves.
    """

    def __init__(self, *args, **kwargs):
        super(MetaPandasDataSet, self).__init__(*args, **kwargs)


    @classmethod
    def column_map_expectation(cls, func):
        """Constructs an expectation using column-map semantics.


        The MetaPandasDataSet implementation replaces the "column" parameter supplied by the user with a pandas Series
        object containing the actual column from the relevant pandas dataframe. This simplifies the implementing expectation
        logic while preserving the standard DataSet signature and expected behavior.

        See :func:`column_map_expectation <great_expectations.dataset.base.DataSet.column_map_expectation>` \
        for full documentation of this function.
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
            nonnull_count = int((boolean_mapped_null_values==False).sum())

            boolean_mapped_success_values = func(self, nonnull_values, *args, **kwargs)
            success_count = boolean_mapped_success_values.sum()

            exception_list = list(series[(boolean_mapped_success_values==False)&(boolean_mapped_null_values==False)])
            exception_index_list = list(series[(boolean_mapped_success_values==False)&(boolean_mapped_null_values==False)].index)
            exception_count = len(exception_list)

            success, percent_success = self._calc_map_expectation_success(success_count, nonnull_count, mostly)

            return_obj = self._format_column_map_output(
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
        """Constructs an expectation using column-aggregate semantics.

        The MetaPandasDataSet implementation replaces the "column" parameter supplied by the user with a pandas
        Series object containing the actual column from the relevant pandas dataframe. This simplifies the implementing
        expectation logic while preserving the standard DataSet signature and expected behavior.

        See :func:`column_aggregate_expectation <great_expectations.dataset.base.DataSet.column_aggregate_expectation>` \
        for full documentation of this function.
        """
        @cls.expectation(inspect.getargspec(func)[0][1:])
        @wraps(func)
        def inner_wrapper(self, column, output_format = None, *args, **kwargs):

            if output_format is None:
                output_format = self.default_expectation_args["output_format"]

            series = self[column]
            null_indexes = series.isnull()

            element_count = int(len(series))
            nonnull_values = series[null_indexes == False]
            nonnull_count = int((null_indexes == False).sum())
            null_count = element_count - nonnull_count

            result_obj = func(self, nonnull_values, *args, **kwargs)

            #!!! This would be the right place to validate result_obj
            #!!! It should contain:
            #!!!    success: bool
            #!!!    true_value: int or float
            #!!!    summary_obj: json-serializable dict

            # if not output_format in ["BASIC", "COMPLETE", "SUMMARY", "BOOLEAN_ONLY"]:
            #     print ("Warning: Unknown output_format %s. Defaulting to %s." % (output_format, self.default_expectation_args["output_format"]))


            if output_format in ["BASIC", "COMPLETE"]:
                return_obj = {
                    "success" : bool(result_obj["success"]),
                    "true_value" : result_obj["true_value"],
                }

            elif (output_format == "SUMMARY"):
                new_summary_obj = {
                    "element_count": element_count,
                    "missing_count": null_count,
                    "missing_percent": null_count*1.0 / element_count if element_count > 0 else None
                }

                if "summary_obj" in result_obj and result_obj["summary_obj"] is not None:
                    result_obj["summary_obj"].update(new_summary_obj)
                else:
                    result_obj["summary_obj"] = new_summary_obj

                return_obj = {
                    "success" : bool(result_obj["success"]),
                    "true_value" : result_obj["true_value"],
                    "summary_obj" : result_obj["summary_obj"]
                }

            elif output_format=="BOOLEAN_ONLY":
                return_obj = bool(result_obj["success"])

            else:
                raise ValueError("Unknown output_format %s." % (output_format,))

            return return_obj

        return inner_wrapper


class PandasDataSet(MetaPandasDataSet, pd.DataFrame):
    """
    PandasDataset instantiates the great_expectations Expectations API as a subclass of a pandas.DataFrame.

    For the full API reference, please see :func:`DataSet <great_expectations.dataset.base.DataSet>`
    """

    def __init__(self, *args, **kwargs):
        super(PandasDataSet, self).__init__(*args, **kwargs)
        self.add_default_expectations()

    def add_default_expectations(self):
        """
        The default behavior for PandasDataSet is to explicitly include expectations that every column present upon initialization exists.

        FIXME: This should probably live in the grandparent class, DataSet, instead.
        """

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
    def expect_column_to_exist(self, column,
                               output_format=None, include_config=False, catch_exceptions=None, meta=None):
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
    def expect_table_row_count_to_be_between(self,
        min_value=0,
        max_value=None,
        output_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        # Assert that min_value and max_value are integers
        try:
            if min_value is not None:
                float(min_value).is_integer()

            if max_value is not None:
                float(max_value).is_integer()

        except ValueError:
            raise ValueError("min_value and max_value must be integers")

        row_count = self.shape[0]

        if min_value != None and max_value != None:
            outcome = row_count >= min_value and row_count <= max_value

        elif min_value == None and max_value != None:
            outcome = row_count <= max_value

        elif min_value != None and max_value == None:
            outcome = row_count >= min_value

        return {
            'success': outcome,
            'true_value': row_count
        }

    @DocInherit
    @DataSet.expectation(['value'])
    def expect_table_row_count_to_equal(self,
        value,
        output_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        try:
            if value is not None:
                float(value).is_integer()

        except ValueError:
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
    def expect_column_values_to_be_unique(self, column,
                                          mostly=None,
                                          output_format=None, include_config=False, catch_exceptions=None, meta=None):
        dupes = set(column[column.duplicated()])
        return column.map(lambda x: x not in dupes)

    @DocInherit
    @DataSet.expectation(['column', 'mostly', 'output_format'])
    def expect_column_values_to_not_be_null(self, column,
                                            mostly=None,
                                            output_format=None, include_config=False, catch_exceptions=None, meta=None):
        if output_format is None:
            output_format = self.default_expectation_args["output_format"]

        series = self[column]
        boolean_mapped_null_values = series.isnull()

        element_count = int(len(series))
        nonnull_values = series[boolean_mapped_null_values==False]
        nonnull_count = int((boolean_mapped_null_values==False).sum())

        boolean_mapped_success_values = boolean_mapped_null_values==False
        success_count = boolean_mapped_success_values.sum()

        exception_list = [None for i in list(series[(boolean_mapped_success_values==False)])]
        exception_index_list = list(series[(boolean_mapped_success_values==False)].index)
        exception_count = len(exception_list)

        # Pass element_count instead of nonnull_count, because that's the right denominator for this expectation
        success, percent_success = self._calc_map_expectation_success(success_count, element_count, mostly)

        return_obj = self._format_column_map_output(
            output_format, success,
            element_count,
            nonnull_values, nonnull_count,
            boolean_mapped_success_values, success_count,
            exception_list, exception_index_list
        )

        return return_obj

    @DocInherit
    @DataSet.expectation(['column', 'mostly', 'output_format'])
    def expect_column_values_to_be_null(self, column,
                                        mostly=None,
                                        output_format=None, include_config=False, catch_exceptions=None, meta=None):
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
        success, percent_success = self._calc_map_expectation_success(success_count, element_count, mostly)

        return_obj = self._format_column_map_output(
            output_format, success,
            element_count,
            nonnull_values, nonnull_count,
            boolean_mapped_success_values, success_count,
            exception_list, exception_index_list
        )

        return return_obj

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_of_type(self, column, type_, target_datasource="numpy",
                                           mostly=None,
                                           output_format=None, include_config=False, catch_exceptions=None, meta=None):
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
    def expect_column_values_to_be_in_type_list(self, column, type_list, target_datasource="numpy",
                                                mostly=None,
                                                output_format=None, include_config=False, catch_exceptions=None, meta=None):

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
    def expect_column_values_to_be_in_set(self, column, values_set,
                                          mostly=None,
                                          output_format=None, include_config=False, catch_exceptions=None, meta=None):
        return column.map(lambda x: x in values_set)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_not_be_in_set(self, column, values_set,
                                              mostly=None,
                                              output_format=None, include_config=False, catch_exceptions=None, meta=None):
        return column.map(lambda x: x not in values_set)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_between(self,
        column,
        min_value=None, max_value=None,
        parse_strings_as_datetimes=None,
        allow_cross_type_comparisons=None,
        mostly=None,
        output_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        if parse_strings_as_datetimes:
            if min_value:
                min_value = parse(min_value)

            if max_value:
                max_value = parse(max_value)

            temp_column = column.map(parse)

        else:
            temp_column = column

        if min_value > max_value:
            raise ValueError("min_value is greater than max_value")

        def is_between(val):
            # TODO Might be worth explicitly defining comparisons between types (for example, between strings and ints).
            # Ensure types can be compared since some types in Python 3 cannot be logically compared.
            # print type(val), type(min_value), type(max_value), val, min_value, max_value

            if type(val) == None:
                return False
            else:
                if min_value != None and max_value != None:
                    if allow_cross_type_comparisons:
                        try:
                            return (min_value <= val) and (val <= max_value)
                        except TypeError:
                            return False

                    else:
                        if (isinstance(val, string_types) != isinstance(min_value, string_types)) or (isinstance(val, string_types) != isinstance(max_value, string_types)):
                            raise TypeError("Column values, min_value, and max_value must either be None or of the same type.")

                        return (min_value <= val) and (val <= max_value)

                elif min_value == None and max_value != None:
                    if allow_cross_type_comparisons:
                        try:
                            return val <= max_value
                        except TypeError:
                            return False

                    else:
                        if isinstance(val, string_types) != isinstance(max_value, string_types):
                            raise TypeError("Column values, min_value, and max_value must either be None or of the same type.")

                        return val <= max_value

                elif min_value != None and max_value == None:
                    if allow_cross_type_comparisons:
                        try:
                            return min_value <= val
                        except TypeError:
                            return False

                    else:
                        if isinstance(val, string_types) != isinstance(min_value, string_types):
                            raise TypeError("Column values, min_value, and max_value must either be None or of the same type.")

                        return min_value <= val

                else:
                    return False


        return temp_column.map(is_between)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_increasing(self, column, strictly=None, parse_strings_as_datetimes=None,
                                              mostly=None,
                                              output_format=None, include_config=False, catch_exceptions=None, meta=None):
        if parse_strings_as_datetimes:
            temp_column = column.map(parse)

            col_diff = temp_column.diff()

            #The first element is null, so it gets a bye and is always treated as True
            col_diff[0] = pd.Timedelta(1)

            if strictly:
                return col_diff > pd.Timedelta(0)
            else:
                return col_diff >= pd.Timedelta(0)

        else:
            col_diff = column.diff()
            #The first element is null, so it gets a bye and is always treated as True
            col_diff[col_diff.isnull()] = 1

            if strictly:
                return col_diff > 0
            else:
                return col_diff >= 0

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_decreasing(self, column, strictly=None, parse_strings_as_datetimes=None,
                                              mostly=None,
                                              output_format=None, include_config=False, catch_exceptions=None, meta=None):
        if parse_strings_as_datetimes:
            temp_column = column.map(parse)

            col_diff = temp_column.diff()

            #The first element is null, so it gets a bye and is always treated as True
            col_diff[0] = pd.Timedelta(-1)

            if strictly:
                return col_diff < pd.Timedelta(0)
            else:
                return col_diff <= pd.Timedelta(0)

        else:
            col_diff = column.diff()
            #The first element is null, so it gets a bye and is always treated as True
            col_diff[col_diff.isnull()] = -1

            if strictly:
                return col_diff < 0
            else:
                return col_diff <= 0

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_value_lengths_to_be_between(self, column, min_value=None, max_value=None,
                                                  mostly=None,
                                                  output_format=None, include_config=False, catch_exceptions=None, meta=None):

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        # Assert that min_value and max_value are integers
        try:
            if min_value is not None and not float(min_value).is_integer():
                raise ValueError("min_value and max_value must be integers")

            if max_value is not None and not float(max_value).is_integer():
                raise ValueError("min_value and max_value must be integers")
        
        except ValueError:
            raise ValueError("min_value and max_value must be integers")


        def length_is_between(val):
            if min_value != None and max_value != None:
                return len(val) >= min_value and len(val) <= max_value

            elif min_value == None and max_value != None:
                return len(val) <= max_value

            elif min_value != None and max_value == None:
                return len(val) >= min_value

            else:
                return False

        return column.map(length_is_between)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_value_lengths_to_equal(self, column, value,
                                             mostly=None,
                                             output_format=None, include_config=False, catch_exceptions=None, meta=None):
        return column.map(lambda x : len(x) == value)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_match_regex(self, column, regex,
                                            mostly=None,
                                            output_format=None, include_config=False, catch_exceptions=None, meta=None):
        return column.map(
            lambda x: re.findall(regex, str(x)) != []
        )

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_not_match_regex(self, column, regex,
                                                mostly=None,
                                                output_format=None, include_config=False, catch_exceptions=None, meta=None):
        return column.map(lambda x: re.findall(regex, str(x)) == [])

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_match_regex_list(self, column, regex_list, match_on="any",
                                                 mostly=None,
                                                 output_format=None, include_config=False, catch_exceptions=None, meta=None):

        if match_on=="any":

            def match_in_list(val):
                if any(re.match(regex, str(val)) for regex in regex_list):
                    return True
                else:
                    return False

        elif match_on=="all":

            def match_in_list(val):
                if all(re.match(regex, str(val)) for regex in regex_list):
                    return True
                else:
                    return False

        return column.map(match_in_list)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_match_strftime_format(self, column, strftime_format,
                                                      mostly=None,
                                                      output_format=None, include_config=False, catch_exceptions=None,
                                                      meta=None):
        ## Below is a simple validation that the provided format can both format and parse a datetime object.
        ## %D is an example of a format that can format but not parse, e.g.
        try:
            datetime.strptime(datetime.strftime(datetime.now(), strftime_format), strftime_format)
        except ValueError as e:
            raise ValueError("Unable to use provided strftime_format. " + e.message)

        def is_parseable_by_format(val):
            try:
                datetime.strptime(val, strftime_format)
                return True
            except TypeError as e:
                raise TypeError("Values passed to expect_column_values_to_match_strftime_format must be of type string.\nIf you want to validate a column of dates or timestamps, please call the expectation before converting from string format.")

            except ValueError as e:
                return False


        return column.map(is_parseable_by_format)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_dateutil_parseable(self, column,
                                                      mostly=None,
                                                      output_format=None, include_config=False, catch_exceptions=None, meta=None):
        def is_parseable(val):
            try:
                if type(val) != str:
                    raise TypeError("Values passed to expect_column_values_to_be_dateutil_parseable must be of type string.\nIf you want to validate a column of dates or timestamps, please call the expectation before converting from string format.")

                parse(val)
                return True

            except (ValueError, OverflowError):
                return False

        return column.map(is_parseable)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_be_json_parseable(self, column,
                                                  mostly=None,
                                                  output_format=None, include_config=False, catch_exceptions=None, meta=None):
        def is_json(val):
            try:
                json.loads(val)
                return True
            except:
                return False

        return column.map(is_json)

    @DocInherit
    @MetaPandasDataSet.column_map_expectation
    def expect_column_values_to_match_json_schema(self, column, json_schema,
                                                  mostly=None,
                                                  output_format=None, include_config=False, catch_exceptions=None, meta=None):
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


    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_mean_to_be_between(self, column, min_value=None, max_value=None,
                                         output_format=None, include_config=False, catch_exceptions=None, meta=None):

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
    def expect_column_median_to_be_between(self, column, min_value=None, max_value=None,
                                           output_format=None, include_config=False, catch_exceptions=None, meta=None):

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
    def expect_column_stdev_to_be_between(self, column, min_value=None, max_value=None,
                                          output_format=None, include_config=False, catch_exceptions=None, meta=None):

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
    def expect_column_unique_value_count_to_be_between(self, column, min_value=None, max_value=None,
                                                       output_format=None, include_config=False, catch_exceptions=None, meta=None):

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
    def expect_column_proportion_of_unique_values_to_be_between(self, column, min_value=0, max_value=1,
                                                                output_format=None, include_config=False, catch_exceptions=None, meta=None):
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
    def expect_column_most_common_value_to_be_in_set(self, column, value_set, ties_okay=None,
                                                     output_format=None, include_config=False, catch_exceptions=None, meta=None):

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
    def expect_column_sum_to_be_between(self,
        column,
        min_value=None,
        max_value=None,
        output_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        col_sum = column.sum()

        if min_value != None and max_value != None:
            success = (min_value <= col_sum) and (col_sum <= max_value)

        elif min_value == None and max_value != None:
            success = (col_sum <= max_value)

        elif min_value != None and max_value == None:
            success = (min_value <= col_sum)

        return {
            "success" : success,
            "true_value" : col_sum,
            "summary_obj" : {}
        }

    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_min_to_be_between(self,
        column,
        min_value=None,
        max_value=None,
        parse_strings_as_datetimes=None,
        output_strftime_format=None,
        output_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        if parse_strings_as_datetimes:
            if min_value:
                min_value = parse(min_value)

            if max_value:
                max_value = parse(max_value)

            temp_column = column.map(parse)

        else:
            temp_column = column

        col_min = temp_column.min()

        if min_value != None and max_value != None:
            success = (min_value <= col_min) and (col_min <= max_value)

        elif min_value == None and max_value != None:
            success = (col_min <= max_value)

        elif min_value != None and max_value == None:
            success = (min_value <= col_min)

        if parse_strings_as_datetimes:
            if output_strftime_format:
                col_min = datetime.strftime(col_min, output_strftime_format)
            else:
                col_min = str(col_min)

        return {
            "success" : success,
            "true_value" : col_min,
            "summary_obj" : {}
        }

    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_max_to_be_between(self,
        column,
        min_value=None,
        max_value=None,
        parse_strings_as_datetimes=None,
        output_strftime_format=None,
        output_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")


        if parse_strings_as_datetimes:
            if min_value:
                min_value = parse(min_value)

            if max_value:
                max_value = parse(max_value)

            temp_column = column.map(parse)

        else:
            temp_column = column

        col_max = temp_column.max()

        if min_value != None and max_value != None:
            success = (min_value <= col_max) and (col_max <= max_value)

        elif min_value == None and max_value != None:
            success = (col_max <= max_value)

        elif min_value != None and max_value == None:
            success = (min_value <= col_max)

        if parse_strings_as_datetimes:
            if output_strftime_format:
                col_max = datetime.strftime(col_max, output_strftime_format)
            else:
                col_max = str(col_max)

        return {
            "success" : success,
            "true_value" : col_max,
            "summary_obj" : {}
        }

    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_chisquare_test_p_value_to_be_greater_than(self, column, partition_object=None, p=0.05, tail_weight_holdout=0,
                                                                output_format=None, include_config=False, catch_exceptions=None, meta=None):
        if not is_valid_categorical_partition_object(partition_object):
            raise ValueError("Invalid partition object.")

        observed_frequencies = column.value_counts()
        # Convert to Series object to allow joining on index values
        expected_column = pd.Series(partition_object['weights'], index=partition_object['values'], name='expected') * len(column)
        # Join along the indices to allow proper comparison of both types of possible missing values
        test_df = pd.concat([expected_column, observed_frequencies], axis = 1)

        na_counts = test_df.isnull().sum()

        ## Handle NaN: if we expected something that's not there, it's just not there.
        test_df[column.name] = test_df[column.name].fillna(0)
        ## Handle NaN: if something's there that was not expected, substitute the relevant value for tail_weight_holdout
        if na_counts['expected'] > 0:
            # Scale existing expected values
            test_df['expected'] = test_df['expected'] * (1 - tail_weight_holdout)
            # Fill NAs with holdout.
            test_df['expected'] = test_df['expected'].fillna(len(column) * (tail_weight_holdout / na_counts['expected']))

        test_result = stats.chisquare(test_df[column.name], test_df['expected'])[1]

        result_obj = {
                "success": test_result > p,
                "true_value": test_result,
                "summary_obj": {
                    "observed_partition": {
                        "values": test_df.index.tolist(),
                        "weights": test_df[column.name].tolist()
                    },
                    "expected_partition": {
                        "values": test_df.index.tolist(),
                        "weights": test_df['expected'].tolist()
                    }
                }
            }

        return result_obj

    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_bootstrapped_ks_test_p_value_to_be_greater_than(self, column, partition_object=None, p=0.05, bootstrap_samples=None, bootstrap_sample_size=None,
                                                                      output_format=None, include_config=False, catch_exceptions=None, meta=None):
        if not is_valid_continuous_partition_object(partition_object):
            raise ValueError("Invalid continuous partition object.")

        if (partition_object['bins'][0] == -np.inf) or (partition_object['bins'][-1] == np.inf):
            raise ValueError("Partition endpoints must be finite.")

        test_cdf = np.append(np.array([0]), np.cumsum(partition_object['weights']))

        def estimated_cdf(x):
            return np.interp(x, partition_object['bins'], test_cdf)

        if bootstrap_samples is None:
            bootstrap_samples = 1000

        if bootstrap_sample_size is None:
            # Sampling too many elements (or not bootstrapping) will make the test too sensitive to the fact that we've
            # compressed via a partition.

            # Sampling too few elements will make the test insensitive to significant differences, especially
            # for nonoverlapping ranges.
            bootstrap_sample_size = len(partition_object['weights']) * 2

        results = [stats.kstest(
                        np.random.choice(column, size=bootstrap_sample_size, replace=True),
                        estimated_cdf)[1]
                   for k in range(bootstrap_samples)]

        test_result = (1 + sum(x >= p for x in results)) / (bootstrap_samples + 1)

        hist, bin_edges = np.histogram(column, partition_object['bins'])
        below_partition = len(np.where(column < partition_object['bins'][0])[0])
        above_partition = len(np.where(column > partition_object['bins'][-1])[0])

        # Expand observed partition to report, if necessary
        if below_partition > 0 and above_partition > 0:
            observed_bins = [np.min(column)] + partition_object['bins'] + [np.max(column)]
            observed_weights = np.concatenate(([below_partition], hist, [above_partition])) / len(column)
        elif below_partition > 0:
            observed_bins = [np.min(column)] + partition_object['bins']
            observed_weights = np.concatenate(([below_partition], hist)) / len(column)
        elif above_partition > 0:
            observed_bins = partition_object['bins'] + [np.max(column)]
            observed_weights = np.concatenate((hist, [above_partition])) / len(column)
        else:
            observed_bins = partition_object['bins']
            observed_weights = hist / len(column)

        observed_cdf_values = np.cumsum(observed_weights)

        result_obj = {
                "success" : test_result > p,
                "true_value": test_result,
                "summary_obj": {
                    "bootstrap_samples": bootstrap_samples,
                    "bootstrap_sample_size": bootstrap_sample_size,
                    "observed_partition": {
                        "bins": observed_bins,
                        "weights": observed_weights.tolist()
                    },
                    "expected_partition": {
                        "bins": partition_object['bins'],
                        "weights": partition_object['weights']
                    },
                    "observed_cdf": {
                        "x": observed_bins,
                        "cdf_values": [0] + observed_cdf_values.tolist()
                    },
                    "expected_cdf": {
                        "x": partition_object['bins'],
                        "cdf_values": test_cdf.tolist()
                    }
                }
            }

        return result_obj

    @DocInherit
    @MetaPandasDataSet.column_aggregate_expectation
    def expect_column_kl_divergence_to_be_less_than(self, column, partition_object=None, threshold=None,
                                                    tail_weight_holdout=0, internal_weight_holdout=0,
                                                    output_format=None, include_config=False, catch_exceptions=None, meta=None):
        if not is_valid_partition_object(partition_object):
            raise ValueError("Invalid partition object.")

        if (not isinstance(threshold, (int, float))) or (threshold < 0):
            raise ValueError("Threshold must be specified, greater than or equal to zero.")

        if (not isinstance(tail_weight_holdout, (int, float))) or (tail_weight_holdout < 0) or (tail_weight_holdout > 1):
            raise ValueError("tail_weight_holdout must be between zero and one.")

        if (not isinstance(internal_weight_holdout, (int, float))) or (internal_weight_holdout < 0) or (internal_weight_holdout > 1):
            raise ValueError("internal_weight_holdout must be between zero and one.")

        if is_valid_categorical_partition_object(partition_object):
            if internal_weight_holdout > 0:
                raise ValueError("Internal weight holdout cannot be used for discrete data.")

            # Data are expected to be discrete, use value_counts
            observed_weights = column.value_counts() / len(column)
            expected_weights = pd.Series(partition_object['weights'], index=partition_object['values'], name='expected')
            test_df = pd.concat([expected_weights, observed_weights], axis=1)

            na_counts = test_df.isnull().sum()

            ## Handle NaN: if we expected something that's not there, it's just not there.
            pk = test_df[column.name].fillna(0)
            ## Handle NaN: if something's there that was not expected, substitute the relevant value for tail_weight_holdout
            if na_counts['expected'] > 0:
                # Scale existing expected values
                test_df['expected'] = test_df['expected'] * (1 - tail_weight_holdout)
                # Fill NAs with holdout.
                qk = test_df['expected'].fillna(tail_weight_holdout / na_counts['expected'])
            else:
                qk = test_df['expected']

            kl_divergence = stats.entropy(pk, qk)

            result_obj = {
                "success": kl_divergence <= threshold,
                "true_value": kl_divergence,
                "summary_obj": {
                    "observed_partition": {
                        "values": test_df.index.tolist(),
                        "weights": pk.tolist()
                    },
                    "expected_partition": {
                        "values": test_df.index.tolist(),
                        "weights": qk.tolist()
                    }
                }
            }

        else:
            # Data are expected to be continuous; discretize first

            # Build the histogram first using expected bins so that the largest bin is >=
            hist, bin_edges = np.histogram(column, partition_object['bins'], density=False)

            # Add in the frequencies observed above or below the provided partition
            below_partition = len(np.where(column < partition_object['bins'][0])[0])
            above_partition = len(np.where(column > partition_object['bins'][-1])[0])

            observed_weights = np.concatenate(([below_partition], hist, [above_partition])) / len(column)

            expected_weights = np.array(partition_object['weights']) * (1 - tail_weight_holdout - internal_weight_holdout)

            # Assign internal weight holdout values if applicable
            if internal_weight_holdout > 0:
                zero_count = len(expected_weights) - np.count_nonzero(expected_weights)
                if zero_count > 0:
                    for index, value in enumerate(expected_weights):
                        if value == 0:
                            expected_weights[index] = internal_weight_holdout / zero_count

            # Assign tail weight holdout if applicable
            # We need to check cases to only add tail weight holdout if it makes sense based on the provided partition.
            if (partition_object['bins'][0] == -np.inf) and (partition_object['bins'][-1]) == np.inf:
                if tail_weight_holdout > 0:
                    raise ValueError("tail_weight_holdout cannot be used for partitions with infinite endpoints.")
                expected_bins = partition_object['bins']
                # Remove the below_partition and above_partition weights we just added (they will necessarily have been zero)
                observed_weights = observed_weights[1:-1]
                # No change to expected weights in this case
            elif (partition_object['bins'][0] == -np.inf):
                expected_bins = partition_object['bins'] + [np.inf]
                # Remove the below_partition weight we just added (it will necessarily have been zero)
                observed_weights = observed_weights[1:]
                expected_weights = np.concatenate((expected_weights, [tail_weight_holdout]))
            elif (partition_object['bins'][-1] == np.inf):
                expected_bins = [-np.inf] + partition_object['bins']
                # Remove the above_partition weight we just added (it will necessarily have been zero)
                observed_weights = observed_weights[:-1]
                expected_weights = np.concatenate(([tail_weight_holdout], expected_weights))
            else:
                expected_bins = [-np.inf] + partition_object['bins'] + [np.inf]
                # No change to observed_weights in this case
                expected_weights = np.concatenate(([tail_weight_holdout / 2], expected_weights, [tail_weight_holdout / 2]))

            kl_divergence = stats.entropy(observed_weights, expected_weights)

            result_obj = {
                    "success": kl_divergence <= threshold,
                    "true_value": kl_divergence,
                    "summary_obj": {
                        "observed_partition": {
                            # return expected_bins, since we used those bins to compute the observed_weights
                            "bins": expected_bins,
                            "weights": observed_weights.tolist()
                        },
                        "expected_partition": {
                            "bins": expected_bins,
                            "weights": expected_weights.tolist()
                        }
                    }
                }

        return result_obj
