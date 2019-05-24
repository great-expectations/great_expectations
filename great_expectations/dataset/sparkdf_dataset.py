from __future__ import division

import inspect
import re
import logging
from six import PY3, string_types
from functools import wraps
from datetime import datetime
from dateutil.parser import parse

from .dataset import Dataset
from great_expectations.data_asset.util import DocInherit, parse_result_format
from great_expectations.dataset.util import (
    is_valid_partition_object,
    is_valid_categorical_partition_object,
    is_valid_continuous_partition_object,
)

import pandas as pd
import numpy as np
from scipy import stats

logger = logging.getLogger(__name__)

try:
    from pyspark.sql.functions import udf, col, stddev as stddev_
    import pyspark.sql.types as sparktypes
except ImportError:
    logger.error("Unable to load spark context; install optional spark dependency for support.")
    raise

class MetaSparkDFDataset(Dataset):
    """MetaSparkDFDataset is a thin layer between Dataset and SparkDFDataset.
    This two-layer inheritance is required to make @classmethod decorators work.
    Practically speaking, that means that MetaSparkDFDataset implements \
    expectation decorators, like `column_map_expectation` and `column_aggregate_expectation`, \
    and SparkDFDataset implements the expectation methods themselves.
    """

    def __init__(self, *args, **kwargs):
        super(MetaSparkDFDataset, self).__init__(*args, **kwargs)

    @classmethod
    def column_map_expectation(cls, func):
        """Constructs an expectation using column-map semantics.


        The MetaSparkDFDataset implementation replaces the "column" parameter supplied by the user with a Spark Dataframe
        with the actual column data. The current approach for functions implementing expectation logic is to append
        a column named "__success" to this dataframe and return to this decorator.

        See :func:`column_map_expectation <great_expectations.Dataset.base.Dataset.column_map_expectation>` \
        for full documentation of this function.
        """
        if PY3:
            argspec = inspect.getfullargspec(func)[0][1:]
        else:
            argspec = inspect.getargspec(func)[0][1:]

        @cls.expectation(argspec)
        @wraps(func)
        def inner_wrapper(self, column, mostly=None, result_format=None, *args, **kwargs):
            """
            This whole decorator is pending a re-write. Currently there is are huge performance issues
            when the # of unexpected elements gets large (10s of millions). Additionally, there is likely
            easy optimization opportunities by coupling result_format with how many different transformations
            are done on the dataset, as is done in sqlalchemy_dataset.
            """

            if result_format is None:
                result_format = self.default_expectation_args["result_format"]

            result_format = parse_result_format(result_format)

            # this is a little dangerous: expectations that specify "COMPLETE" result format and have a very
            # large number of unexpected results could hang for a long time. we should either call this out in docs
            # or put a limit on it
            if result_format['result_format'] == 'COMPLETE':
                unexpected_count_limit = None
            else:
                unexpected_count_limit = result_format['partial_unexpected_count']

            col_df = self.spark_df.select(column) # pyspark.sql.DataFrame

            # a couple of tests indicate that caching here helps performance
            col_df.cache()
            element_count = self.get_row_count()

            # FIXME temporary fix for missing/ignored value
            if func.__name__ not in ['expect_column_values_to_not_be_null', 'expect_column_values_to_be_null']:
                col_df = col_df.filter('{column} is not null'.format(column=column))
                # these nonnull_counts are cached by SparkDFDataset
                nonnull_count = self.get_column_nonnull_count(column)
            else:
                nonnull_count = element_count

            # success_df will have columns [column, '__success']
            # this feels a little hacky, so might want to change
            success_df = func(self, col_df, *args, **kwargs)
            success_count = success_df.filter('__success = True').count()

            unexpected_count = nonnull_count - success_count
            if unexpected_count == 0:
                # save some computation time if no unexpected items
                maybe_limited_unexpected_list = []
            else:
                # here's an example of a place where we could do optimizations if we knew result format: see
                # comment block below
                unexpected_df = success_df.filter('__success = False')
                if unexpected_count_limit:
                    unexpected_df = unexpected_df.limit(unexpected_count_limit)
                maybe_limited_unexpected_list = [
                    row[column]
                    for row
                    in unexpected_df.collect()
                ]

            success, percent_success = self._calc_map_expectation_success(
                success_count, nonnull_count, mostly)

            # Currently the abstraction of "result_format" that _format_column_map_output provides
            # limits some possible optimizations within the column-map decorator. It seems that either
            # this logic should be completely rolled into the processing done in the column_map decorator, or that the decorator
            # should do a minimal amount of computation agnostic of result_format, and then delegate the rest to this method.
            # In the first approach, it could make sense to put all of this decorator logic in Dataset, and then implement
            # properties that require dataset-type-dependent implementations (as is done with SparkDFDataset.row_count currently).
            # Then a new dataset type could just implement these properties/hooks and Dataset could deal with caching these and
            # with the optimizations based on result_format. A side benefit would be implementing an interface for the user
            # to get basic info about a dataset in a standardized way, e.g. my_dataset.row_count, my_dataset.columns (only for
            # tablular datasets maybe). However, unclear if this is worth it or if it would conflict with optimizations being done
            # in other dataset implementations.
            return_obj = self._format_map_output(
                result_format,
                success,
                element_count,
                nonnull_count,
                unexpected_count,
                maybe_limited_unexpected_list,
                unexpected_index_list=None,
            )

            # FIXME Temp fix for result format
            if func.__name__ in ['expect_column_values_to_not_be_null', 'expect_column_values_to_be_null']:
                del return_obj['result']['unexpected_percent_nonmissing']
                try:
                    del return_obj['result']['partial_unexpected_counts']
                except KeyError:
                    pass

            col_df.unpersist()

            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__

        return inner_wrapper


class SparkDFDataset(MetaSparkDFDataset):
    """
    This class holds an attribute `spark_df` which is a spark.sql.DataFrame.
    """

    def __init__(self, spark_df, *args, **kwargs):
        super(SparkDFDataset, self).__init__(*args, **kwargs)
        self.discard_subset_failing_expectations = kwargs.get("discard_subset_failing_expectations", False)
        # Creation of the Spark Dataframe is done outside this class
        self.spark_df = spark_df

    def get_row_count(self):
        return self.spark_df.count()

    def get_table_columns(self):
        return self.spark_df.columns

    def get_column_nonnull_count(self, column):
        return self.spark_df.filter('{column} is not null'.format(column=column)).count()

    def get_column_mean(self, column):
        # TODO need to apply this logic to other such methods?
        types = dict(self.spark_df.dtypes)
        if types[column] not in ('int', 'float', 'double'):
            raise TypeError('Expected numeric column type for function mean()')
        result = self.spark_df.select(column).groupBy().mean().collect()[0]
        return result[0] if len(result) > 0 else None

    def get_column_sum(self, column):
        return self.spark_df.select(column).groupBy().sum().collect()[0][0]

    def get_column_max(self, column, parse_strings_as_datetimes=False):
        temp_column = self.spark_df.select(column).where(col(column).isNotNull())
        if parse_strings_as_datetimes:
            temp_column = self._apply_dateutil_parse(temp_column)
        result = temp_column.agg({column: 'max'}).collect()
        if not result or not result[0]:
            return None
        return result[0][0]

    def get_column_min(self, column, parse_strings_as_datetimes=False):
        temp_column = self.spark_df.select(column).where(col(column).isNotNull())
        if parse_strings_as_datetimes:
            temp_column = self._apply_dateutil_parse(temp_column)
        result = temp_column.agg({column: 'min'}).collect()
        if not result or not result[0]:
            return None
        return result[0][0]

    def get_column_value_counts(self, column):
        value_counts = self.spark_df.select(column)\
            .filter('{} is not null'.format(column))\
            .groupBy(column)\
            .count()\
            .orderBy(column)\
            .collect()
        # assuming this won't get too big
        return pd.Series(
            [row['count'] for row in value_counts],
            index=[row[column] for row in value_counts],
            # don't know about including name here
            name=column,
        )

    def get_column_unique_count(self, column):
        return self.get_column_value_counts(column).shape[0]

    def get_column_modes(self, column):
        """leverages computation done in _get_column_value_counts"""
        s = self.get_column_value_counts(column)
        return list(s[s == s.max()].index)

    def get_column_median(self, column):
        # TODO this doesn't actually work e.g. median([1, 2, 3, 4]) -> 2.0
        raise NotImplementedError
        result = self.spark_df.approxQuantile(column, [0.5], 0)
        return result[0] if len(result) > 0 else None

    def get_column_stdev(self, column):
        return self.spark_df.select(stddev_(col(column))).collect()[0][0]

    def get_column_hist(self, column, bins):
        """return a list of counts corresponding to bins"""
        hist = []
        for i in range(0, len(bins) - 1):
            # all bins except last are half-open
            if i == len(bins) - 2:
                max_strictly = False
            else:
                max_strictly = True
            hist.append(
                self.get_column_count_in_range(column, min_val=bins[i], max_val=bins[i + 1], max_strictly=max_strictly)
            )
        return hist

    def get_column_count_in_range(self, column, min_val=None, max_val=None, min_strictly=False, max_strictly=True):
        # TODO this logic could probably go in the non-underscore version if we want to cache
        if min_val is None and max_val is None:
            raise ValueError('Must specify either min or max value')
        if min_val is not None and max_val is not None and min_val > max_val:
            raise ValueError('Min value must be <= to max value')

        result = self.spark_df.select(column)
        if min_val is not None:
            if min_strictly:
                result = result.filter(col(column) > min_val)
            else:
                result = result.filter(col(column) >= min_val)
        if max_val is not None:
            if max_strictly:
                result = result.filter(col(column) < max_val)
            else:
                result = result.filter(col(column) <= max_val)
        return result.count()

    # Utils
    @staticmethod
    def _apply_dateutil_parse(column):
        assert len(column.columns) == 1, "Expected DataFrame with 1 column"
        col_name = column.columns[0]
        _udf = udf(parse, sparktypes.TimestampType())
        return column.withColumn(col_name, _udf(col_name))

    # Expectations
    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_be_in_set(
            self,
            column,  # pyspark.sql.DataFrame
            value_set,  # List[Any]
            mostly=None,
            parse_strings_as_datetimes=None,
            result_format=None,
            include_config=False,
            catch_exceptions=None,
            meta=None,
    ):
        if parse_strings_as_datetimes:
            column = self._apply_dateutil_parse(column)
            value_set = [parse(value) if isinstance(value, string_types) else value for value in value_set]
        success_udf = udf(lambda x: x in value_set)
        return column.withColumn('__success', success_udf(column[0]))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_not_be_in_set(
            self,
            column,  # pyspark.sql.DataFrame
            value_set,  # List[Any]
            mostly=None,
            result_format=None,
            include_config=False,
            catch_exceptions=None,
            meta=None,
    ):
        success_udf = udf(lambda x: x not in value_set)
        return column.withColumn('__success', success_udf(column[0]))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_be_unique(
        self,
        column,
        mostly=None,
        result_format=None,
        include_config=False,
        catch_exceptions=None,
        meta=None,
    ):
        # TODO is there a more efficient way to do this?
        dups = set([row[0] for row in column.groupBy(column[0]).count().filter('count > 1').collect()])
        success_udf = udf(lambda x: x not in dups)
        return column.withColumn('__success', success_udf(column[0]))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_value_lengths_to_equal(
        self,
        column,
        value, # int
        mostly=None,
        result_format=None,
        include_config=False,
        catch_exceptions=None,
        meta=None,
    ):
        success_udf = udf(lambda x: len(x) == value)
        return column.withColumn('__success', success_udf(column[0]))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_match_strftime_format(
        self,
        column,
        strftime_format, # str
        mostly=None,
        result_format=None,
        include_config=False,
        catch_exceptions=None,
        meta=None,
    ):
        # Below is a simple validation that the provided format can both format and parse a datetime object.
        # %D is an example of a format that can format but not parse, e.g.
        try:
            datetime.strptime(datetime.strftime(
                datetime.now(), strftime_format), strftime_format)
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

        success_udf = udf(is_parseable_by_format)
        return column.withColumn('__success', success_udf(column[0]))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_not_be_null(
        self,
        column,
        mostly=None,
        result_format=None,
        include_config=False,
        catch_exceptions=None,
        meta=None,
    ):
        success_udf = udf(lambda x: x is not None)
        return column.withColumn('__success', success_udf(column[0]))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_be_null(
        self,
        column,
        mostly=None,
        result_format=None,
        include_config=False,
        catch_exceptions=None,
        meta=None,
    ):
        success_udf = udf(lambda x: x is None)
        return column.withColumn('__success', success_udf(column[0]))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_match_regex(
        self,
        column,
        regex,
        mostly=None,
        result_format=None,
        include_config=False,
        catch_exceptions=None,
        meta=None,
    ):
        # not sure know about casting to string here
        success_udf = udf(lambda x: re.findall(regex, str(x)) != [])
        return column.withColumn('__success', success_udf(column[0]))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_not_match_regex(
        self,
        column,
        regex,
        mostly=None,
        result_format=None,
        include_config=False,
        catch_exceptions=None,
        meta=None,
    ):
        # not sure know about casting to string here
        success_udf = udf(lambda x: re.findall(regex, str(x)) == [])
        return column.withColumn('__success', success_udf(column[0]))
