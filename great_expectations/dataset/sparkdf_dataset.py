from __future__ import division

from six import PY3
import inspect
import re
from functools import wraps
from datetime import datetime
# TODO change this import to be python2 compatible
from itertools import zip_longest
from collections import UserDict, Counter

from .base import Dataset
from .util import DocInherit, parse_result_format

from pyspark.sql.functions import udf


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
            # this value is cached by SparkDFDataset
            element_count = self.row_count

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

            if success_count == nonnull_count:
                # save some computation time if no unexpected items
                maybe_limited_unexpected_list = []
                unexpected_count = 0
            else:
                # here's an example of a place where we could do optimizations if we knew result format: see
                # comment block below
                unexpected_df = success_df.filter('__success = False')
                if unexpected_count_limit:
                    unexpected_df = unexpected_df.limit(unexpected_count_limit)
                unexpected_count = unexpected_df.count()
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
            return_obj = self._format_column_map_output(
                result_format,
                success,
                element_count,
                nonnull_count,
                maybe_limited_unexpected_list,
                unexpected_count=unexpected_count,
                # spark dataframes are not indexed
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
        # TODO: consider renaming to self._spark_df; see below comments
        self.spark_df = spark_df

        # some data structures to keep track of commonly used values. this approach works especially well
        # with Spark's lazy execution model, but it could be moved up to the Dataset module if other datasets could benefit.
        # NOTE: this approach makes the strong assumption that the user will not modify self.spark_df over the
        # lifetime of this dataset instance
        self._row_count = None
        self._column_nonnull_counts = {}

    @property
    def row_count(self):
        """Compute dataframe length once and store"""
        if not self._row_count:
            self._row_count = self.spark_df.count()
        return self._row_count

    def get_column_nonnull_count(self, column):
        """Compute nonnull counts for each column once and store"""
        # TODO: is there a way to make this a property like row_count?
        if not column in self._column_nonnull_counts:
            nonnull_count = self.spark_df.filter('{column} is not null'.format(column=column)).count()
            self._column_nonnull_counts[column] = nonnull_count
        return self._column_nonnull_counts[column]

    @DocInherit
    @Dataset.expectation(["column"])
    def expect_column_to_exist(
        self, column, column_index=None, result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        if column in self.spark_df.columns:
            return {
                # FIXME: list.index does not check for duplicate values.
                "success": (column_index is None) or (self.spark_df.columns.index(column) == column_index)
            }
        else:
            return {"success": False}

    def expect_table_columns_to_match_ordered_list(
        self,
        column_list, # List
        result_format=None,
        include_config=False,
        catch_exceptions=None,
        meta=None,
    ):
        """
        Checks if observed columns are in the expected order. The expectations will fail if columns are out of expected
        order, columns are missing, or additional columns are present. On failure, details are provided on the location
        of the unexpected column(s).
        """
        if self.spark_df.columns == column_list:
            return {
                "success": True
            }
        else:
            # In the case of differing column lengths between the defined expectation and the observed column set, the
            # max is determined to generate the column_index.
            number_of_columns = max(len(column_list), len(self.spark_df.columns))
            column_index = range(number_of_columns)

            # Create a list of the mismatched details
            compared_lists = list(zip_longest(column_index, list(column_list), list(self.spark_df.columns)))
            mismatched = [{"Expected Column Position": i,
                           "Expected": k,
                           "Found": v} for i, k, v in compared_lists if k != v]
            return {
                "success": False,
                "details": {"mismatched": mismatched}
            }

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_be_in_set(
            self,
            column,  # pyspark.sql.DataFrame
            value_set,  # List[Any]
            mostly=None,
            result_format=None,
            include_config=False,
            catch_exceptions=None,
            meta=None,
    ):
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
    @Dataset.expectation(['min_value', 'max_value'])
    def expect_table_row_count_to_be_between(
            self,
            min_value=None, # int
            max_value=None, # int
            result_format=None,
            include_config=False,
            catch_exceptions=None,
            meta=None,
    ):
        # This and several other expectations have almost identical implementations in all 3 datasets;
        # is this worth refactoring?
        try:
            if min_value is not None:
                if not float(min_value).is_integer():
                    raise ValueError("min_value must be integer")
            if max_value is not None:
                if not float(max_value).is_integer():
                    raise ValueError("max_value must be integer")
        except ValueError:
            raise ValueError("min_value and max_value must be integers")

        # check that min_value or max_value is set
        if min_value is None and max_value is None:
            raise Exception('Must specify either or both of min_value and max_value')

        row_count = self.row_count

        if min_value is not None and max_value is not None:
            outcome = row_count >= min_value and row_count <= max_value

        elif min_value is None and max_value is not None:
            outcome = row_count <= max_value

        elif min_value is not None and max_value is None:
            outcome = row_count >= min_value

        return {
            'success': outcome,
            'result': {
                'observed_value': row_count
            }
        }

    @DocInherit
    @Dataset.expectation(['value'])
    def expect_table_row_count_to_equal(
        self,
        value, # int
        result_format=None,
        include_config=False,
        catch_exceptions=None,
        meta=None,
    ):
        if not float(value).is_integer():
            raise ValueError("Value must be an integer")

        row_count = self.row_count

        return {
            'success': row_count == value,
            'result': {
                'observed_value': row_count
            }
        }

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
        # TODO tests for this expectations
        # not sure know about casting to string here
        success_udf = udf(lambda x: re.findall(regex, str(x)) != [])
        return column.withColumn('__success', success_udf(column[0]))
