from __future__ import division

from six import PY3
import inspect
from functools import wraps
# TODO change this import to be python2 compatible
from itertools import zip_longest

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


        The MetaPandasDataset implementation replaces the "column" parameter supplied by the user with a pandas Series
        object containing the actual column from the relevant pandas dataframe. This simplifies the implementing expectation
        logic while preserving the standard Dataset signature and expected behavior.

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

            if result_format is None:
                result_format = self.default_expectation_args["result_format"]

            result_format = parse_result_format(result_format)

            col_df = self.spark_df.select(column) # pyspark.sql.DataFrame

            # a couple of tests indicate that caching here helps performance
            col_df.cache()
            element_count = col_df.count()

            nonnull_values = col_df.filter('{column} is not null'.format(column=column))
            nonnull_count = nonnull_values.count()

            success_df = func(self, nonnull_values, *args, **kwargs)
            success_count = success_df.filter('__success = True').count()

            if success_count == nonnull_count:
                # save some computation time if no unexpected items
                unexpected_list = []
            else:
                unexpected_df = success_df.filter('__success = False')
                unexpected_list = [row[column] for row in unexpected_df.collect()]

            success, percent_success = self._calc_map_expectation_success(
                success_count, nonnull_count, mostly)

            return_obj = self._format_column_map_output(
                result_format,
                success,
                element_count,
                nonnull_count,
                unexpected_list,
                # I don't think indices are relevant for a spark dataframe
                unexpected_index_list=None,
            )

            # FIXME Temp fix for result format
            if func.__name__ in ['expect_column_values_to_not_be_null', 'expect_column_values_to_be_null']:
                del return_obj['result']['unexpected_percent_nonmissing']
                try:
                    del return_obj['result']['partial_unexpected_counts']
                except KeyError:
                    pass

            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__

        return inner_wrapper


class SparkDFDataset(MetaSparkDFDataset):
    """
    For now this class holds an attribute `df` which is a spark.sql.DataFrame, rather than subclassing as is
    done in PandasDataset.
    """

    def __init__(self, spark_df, *args, **kwargs):
        super(SparkDFDataset, self).__init__(*args, **kwargs)
        self.discard_subset_failing_expectations = kwargs.get("discard_subset_failing_expectations", False)
        # Creation of the Spark Dataframe is done outside this class
        self.spark_df = spark_df

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
        self, column_list, result_format=None, include_config=False, catch_exceptions=None, meta=None
    ):
        """
        Checks if observed columns are in the expected order. The expectations will fail if columns are out of expected
        order, columns are missing, or additional columns are present. On failure, details are provided on the location
        of the unexpected column(s).
        """
        if self.spark_df.columns == list(column_list):
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
        """
        Assumes that `column` is a pyspark.sql.DataFrame with only 1 column.

        For now, this function returns the `column` dataframe with a column appended for success/failure of the condition
        """
        success_udf = udf(lambda x: x in value_set)
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
        # Assert that min_value and max_value are integers
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

        row_count = self.spark_df.count()

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

        row_count = self.spark_df.count()

        return {
            'success': row_count == value,
            'result': {
                'observed_value': row_count
            }
        }
