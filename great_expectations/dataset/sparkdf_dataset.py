import copy
import inspect
import logging
from datetime import datetime
from functools import reduce, wraps
from typing import List
from collections import OrderedDict

import numpy as np
import pandas as pd
from dateutil.parser import parse
from great_expectations.data_asset import DataAsset
from great_expectations.data_asset.util import DocInherit, parse_result_format

from .dataset import Dataset
from .pandas_dataset import PandasDataset

logger = logging.getLogger(__name__)

try:
    from pyspark.sql.functions import (
        udf,
        col,
        lit,
        desc,
        stddev_samp,
        length as length_,
        when,
        year,
        count,
        countDistinct,
        monotonically_increasing_id,
    )
    import pyspark.sql.types as sparktypes
    from pyspark.ml.feature import Bucketizer
    from pyspark.sql import Window
except ImportError as e:
    logger.debug(str(e))
    logger.debug(
        "Unable to load spark context; install optional spark dependency for support."
    )


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
        argspec = inspect.getfullargspec(func)[0][1:]

        @cls.expectation(argspec)
        @wraps(func)
        def inner_wrapper(
            self, column, mostly=None, result_format=None, *args, **kwargs
        ):
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
            if result_format["result_format"] == "COMPLETE":
                unexpected_count_limit = None
            else:
                unexpected_count_limit = result_format["partial_unexpected_count"]

            col_df = self.spark_df.select(column)  # pyspark.sql.DataFrame

            # a couple of tests indicate that caching here helps performance
            col_df.cache()
            element_count = self.get_row_count()

            # FIXME temporary fix for missing/ignored value
            if func.__name__ not in [
                "expect_column_values_to_not_be_null",
                "expect_column_values_to_be_null",
            ]:
                col_df = col_df.filter(col_df[0].isNotNull())
                # these nonnull_counts are cached by SparkDFDataset
                nonnull_count = self.get_column_nonnull_count(column)
            else:
                nonnull_count = element_count

            # success_df will have columns [column, '__success']
            # this feels a little hacky, so might want to change
            success_df = func(self, col_df, *args, **kwargs)
            success_count = success_df.filter("__success = True").count()

            unexpected_count = nonnull_count - success_count
            if unexpected_count == 0:
                # save some computation time if no unexpected items
                maybe_limited_unexpected_list = []
            else:
                # here's an example of a place where we could do optimizations if we knew result format: see
                # comment block below
                unexpected_df = success_df.filter("__success = False")
                if unexpected_count_limit:
                    unexpected_df = unexpected_df.limit(unexpected_count_limit)
                maybe_limited_unexpected_list = [
                    row[column] for row in unexpected_df.collect()
                ]

                if "output_strftime_format" in kwargs:
                    output_strftime_format = kwargs["output_strftime_format"]
                    parsed_maybe_limited_unexpected_list = []
                    for val in maybe_limited_unexpected_list:
                        if val is None:
                            parsed_maybe_limited_unexpected_list.append(val)
                        else:
                            if isinstance(val, str):
                                val = parse(val)
                            parsed_maybe_limited_unexpected_list.append(
                                datetime.strftime(val, output_strftime_format)
                            )
                    maybe_limited_unexpected_list = parsed_maybe_limited_unexpected_list

            success, percent_success = self._calc_map_expectation_success(
                success_count, nonnull_count, mostly
            )

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
            if func.__name__ in [
                "expect_column_values_to_not_be_null",
                "expect_column_values_to_be_null",
            ]:
                del return_obj["result"]["unexpected_percent_nonmissing"]
                del return_obj["result"]["missing_count"]
                del return_obj["result"]["missing_percent"]
                try:
                    del return_obj["result"]["partial_unexpected_counts"]
                except KeyError:
                    pass

            col_df.unpersist()

            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__

        return inner_wrapper

    @classmethod
    def column_pair_map_expectation(cls, func):
        """
        The column_pair_map_expectation decorator handles boilerplate issues surrounding the common pattern of evaluating
        truthiness of some condition on a per row basis across a pair of columns.
        """
        argspec = inspect.getfullargspec(func)[0][1:]

        @cls.expectation(argspec)
        @wraps(func)
        def inner_wrapper(
            self,
            column_A,
            column_B,
            mostly=None,
            ignore_row_if="both_values_are_missing",
            result_format=None,
            *args,
            **kwargs
        ):
            if result_format is None:
                result_format = self.default_expectation_args["result_format"]

            result_format = parse_result_format(result_format)

            # this is a little dangerous: expectations that specify "COMPLETE" result format and have a very
            # large number of unexpected results could hang for a long time. we should either call this out in docs
            # or put a limit on it
            if result_format["result_format"] == "COMPLETE":
                unexpected_count_limit = None
            else:
                unexpected_count_limit = result_format["partial_unexpected_count"]

            cols_df = self.spark_df.select(column_A, column_B).withColumn(
                "__row", monotonically_increasing_id()
            )  # pyspark.sql.DataFrame

            # a couple of tests indicate that caching here helps performance
            cols_df.cache()
            element_count = self.get_row_count()

            if ignore_row_if == "both_values_are_missing":
                boolean_mapped_null_values = cols_df.selectExpr(
                    "`__row`",
                    "`{0}` AS `A_{0}`".format(column_A),
                    "`{0}` AS `B_{0}`".format(column_B),
                    "ISNULL(`{0}`) AND ISNULL(`{1}`) AS `__null_val`".format(
                        column_A, column_B
                    ),
                )
            elif ignore_row_if == "either_value_is_missing":
                boolean_mapped_null_values = cols_df.selectExpr(
                    "`__row`",
                    "`{0}` AS `A_{0}`".format(column_A),
                    "`{0}` AS `B_{0}`".format(column_B),
                    "ISNULL(`{0}`) OR ISNULL(`{1}`) AS `__null_val`".format(
                        column_A, column_B
                    ),
                )
            elif ignore_row_if == "never":
                boolean_mapped_null_values = cols_df.selectExpr(
                    "`__row`",
                    "`{0}` AS `A_{0}`".format(column_A),
                    "`{0}` AS `B_{0}`".format(column_B),
                    lit(False).alias("__null_val"),
                )
            else:
                raise ValueError("Unknown value of ignore_row_if: %s", (ignore_row_if,))

            # since pyspark guaranteed each columns selected has the same number of rows, no need to do assert as in pandas
            # assert series_A.count() == (
            #     series_B.count()), "Series A and B must be the same length"

            nonnull_df = boolean_mapped_null_values.filter("__null_val = False")
            nonnull_count = nonnull_df.count()

            col_A_df = nonnull_df.select("__row", "`A_{0}`".format(column_A))
            col_B_df = nonnull_df.select("__row", "`B_{0}`".format(column_B))

            success_df = func(self, col_A_df, col_B_df, *args, **kwargs)
            success_count = success_df.filter("__success = True").count()

            unexpected_count = nonnull_count - success_count
            if unexpected_count == 0:
                # save some computation time if no unexpected items
                maybe_limited_unexpected_list = []
            else:
                # here's an example of a place where we could do optimizations if we knew result format: see
                # comment block below
                unexpected_df = success_df.filter("__success = False")
                if unexpected_count_limit:
                    unexpected_df = unexpected_df.limit(unexpected_count_limit)
                maybe_limited_unexpected_list = [
                    (row["A_{0}".format(column_A)], row["B_{0}".format(column_B)])
                    for row in unexpected_df.collect()
                ]

                if "output_strftime_format" in kwargs:
                    output_strftime_format = kwargs["output_strftime_format"]
                    parsed_maybe_limited_unexpected_list = []
                    for val in maybe_limited_unexpected_list:
                        if val is None or (val[0] is None or val[1] is None):
                            parsed_maybe_limited_unexpected_list.append(val)
                        else:
                            if isinstance(val[0], str) and isinstance(val[1], str):
                                val = (parse(val[0]), parse(val[1]))
                            parsed_maybe_limited_unexpected_list.append(
                                (
                                    datetime.strftime(val[0], output_strftime_format),
                                    datetime.strftime(val[1], output_strftime_format),
                                )
                            )
                    maybe_limited_unexpected_list = parsed_maybe_limited_unexpected_list

            success, percent_success = self._calc_map_expectation_success(
                success_count, nonnull_count, mostly
            )

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

            # # FIXME Temp fix for result format
            # if func.__name__ in ['expect_column_values_to_not_be_null', 'expect_column_values_to_be_null']:
            #     del return_obj['result']['unexpected_percent_nonmissing']
            #     del return_obj['result']['missing_count']
            #     del return_obj['result']['missing_percent']
            #     try:
            #         del return_obj['result']['partial_unexpected_counts']
            #     except KeyError:
            #         pass

            cols_df.unpersist()

            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__

        return inner_wrapper

    @classmethod
    def multicolumn_map_expectation(cls, func):
        """
        The multicolumn_map_expectation decorator handles boilerplate issues surrounding the common pattern of
        evaluating truthiness of some condition on a per row basis across a set of columns.
        """
        argspec = inspect.getfullargspec(func)[0][1:]

        @cls.expectation(argspec)
        @wraps(func)
        def inner_wrapper(
            self,
            column_list,
            mostly=None,
            ignore_row_if="all_values_are_missing",
            result_format=None,
            *args,
            **kwargs
        ):
            if result_format is None:
                result_format = self.default_expectation_args["result_format"]

            result_format = parse_result_format(result_format)

            # this is a little dangerous: expectations that specify "COMPLETE" result format and have a very
            # large number of unexpected results could hang for a long time. we should either call this out in docs
            # or put a limit on it
            if result_format["result_format"] == "COMPLETE":
                unexpected_count_limit = None
            else:
                unexpected_count_limit = result_format["partial_unexpected_count"]

            temp_df = self.spark_df.select(*column_list)  # pyspark.sql.DataFrame

            # a couple of tests indicate that caching here helps performance
            temp_df.cache()
            element_count = self.get_row_count()

            if ignore_row_if == "all_values_are_missing":
                boolean_mapped_skip_values = temp_df.select(
                    [
                        *column_list,
                        reduce(
                            lambda a, b: a & b, [col(c).isNull() for c in column_list]
                        ).alias("__null_val"),
                    ]
                )
            elif ignore_row_if == "any_value_is_missing":
                boolean_mapped_skip_values = temp_df.select(
                    [
                        *column_list,
                        reduce(
                            lambda a, b: a | b, [col(c).isNull() for c in column_list]
                        ).alias("__null_val"),
                    ]
                )
            elif ignore_row_if == "never":
                boolean_mapped_skip_values = temp_df.select(
                    [*column_list, lit(False).alias("__null_val")]
                )
            else:
                raise ValueError("Unknown value of ignore_row_if: %s", (ignore_row_if,))

            nonnull_df = boolean_mapped_skip_values.filter("__null_val = False")
            nonnull_count = nonnull_df.count()

            cols_df = nonnull_df.select(*column_list)

            success_df = func(self, cols_df, *args, **kwargs)
            success_count = success_df.filter("__success = True").count()

            unexpected_count = nonnull_count - success_count
            if unexpected_count == 0:
                maybe_limited_unexpected_list = []
            else:
                # here's an example of a place where we could do optimizations if we knew result format: see
                # comment block below
                unexpected_df = success_df.filter("__success = False")
                if unexpected_count_limit:
                    unexpected_df = unexpected_df.limit(unexpected_count_limit)
                maybe_limited_unexpected_list = [
                    OrderedDict((c, row[c]) for c in column_list)
                    for row in unexpected_df.collect()
                ]

                if "output_strftime_format" in kwargs:
                    output_strftime_format = kwargs["output_strftime_format"]
                    parsed_maybe_limited_unexpected_list = []
                    for val in maybe_limited_unexpected_list:
                        if val is None or not all(v for k, v in val):
                            parsed_maybe_limited_unexpected_list.append(val)
                        else:
                            if all(isinstance(v, str) for k, v in val):
                                val = OrderedDict((k, parse(v)) for k, v in val)
                            parsed_maybe_limited_unexpected_list.append(
                                OrderedDict(
                                    (k, datetime.strftime(v, output_strftime_format))
                                    for k, v in val
                                )
                            )
                    maybe_limited_unexpected_list = parsed_maybe_limited_unexpected_list

            success, percent_success = self._calc_map_expectation_success(
                success_count, nonnull_count, mostly
            )

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

            temp_df.unpersist()

            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__

        return inner_wrapper


class SparkDFDataset(MetaSparkDFDataset):
    """
    This class holds an attribute `spark_df` which is a spark.sql.DataFrame.
    """

    @classmethod
    def from_dataset(cls, dataset=None):
        if isinstance(dataset, SparkDFDataset):
            return cls(spark_df=dataset.spark_df)
        else:
            raise ValueError("from_dataset requires a SparkDFDataset dataset")

    def __init__(self, spark_df, *args, **kwargs):
        # Creation of the Spark DataFrame is done outside this class
        self.spark_df = spark_df
        self._persist = kwargs.pop("persist", True)
        if self._persist:
            self.spark_df.persist()
        super(SparkDFDataset, self).__init__(*args, **kwargs)

    def head(self, n=5):
        """Returns a *PandasDataset* with the first *n* rows of the given Dataset"""
        return PandasDataset(
            self.spark_df.limit(n).toPandas(),
            expectation_suite=self.get_expectation_suite(
                discard_failed_expectations=False,
                discard_result_format_kwargs=False,
                discard_catch_exceptions_kwargs=False,
                discard_include_config_kwargs=False,
            ),
        )

    def get_row_count(self):
        return self.spark_df.count()

    def get_column_count(self):
        return len(self.spark_df.columns)

    def get_table_columns(self) -> List[str]:
        return self.spark_df.columns

    def get_column_nonnull_count(self, column):
        return self.spark_df.filter(col(column).isNotNull()).count()

    def get_column_mean(self, column):
        # TODO need to apply this logic to other such methods?
        types = dict(self.spark_df.dtypes)
        if types[column] not in ("int", "float", "double", "bigint"):
            raise TypeError("Expected numeric column type for function mean()")
        result = self.spark_df.select(column).groupBy().mean().collect()[0]
        return result[0] if len(result) > 0 else None

    def get_column_sum(self, column):
        return self.spark_df.select(column).groupBy().sum().collect()[0][0]

    # TODO: consider getting all basic statistics in one go:
    def _describe_column(self, column):
        # temp_column = self.spark_df.select(column).where(col(column).isNotNull())
        # return self.spark_df.select(
        #     [
        #         count(temp_column),
        #         mean(temp_column),
        #         stddev(temp_column),
        #         min(temp_column),
        #         max(temp_column)
        #     ]
        # )
        pass

    def get_column_max(self, column, parse_strings_as_datetimes=False):
        temp_column = self.spark_df.select(column).where(col(column).isNotNull())
        if parse_strings_as_datetimes:
            temp_column = self._apply_dateutil_parse(temp_column)
        result = temp_column.agg({column: "max"}).collect()
        if not result or not result[0]:
            return None
        return result[0][0]

    def get_column_min(self, column, parse_strings_as_datetimes=False):
        temp_column = self.spark_df.select(column).where(col(column).isNotNull())
        if parse_strings_as_datetimes:
            temp_column = self._apply_dateutil_parse(temp_column)
        result = temp_column.agg({column: "min"}).collect()
        if not result or not result[0]:
            return None
        return result[0][0]

    def get_column_value_counts(self, column, sort="value", collate=None):
        if sort not in ["value", "count", "none"]:
            raise ValueError("sort must be either 'value', 'count', or 'none'")
        if collate is not None:
            raise ValueError("collate parameter is not supported in SparkDFDataset")
        value_counts = (
            self.spark_df.select(column)
            .where(col(column).isNotNull())
            .groupBy(column)
            .count()
        )
        if sort == "value":
            value_counts = value_counts.orderBy(column)
        elif sort == "count":
            value_counts = value_counts.orderBy(desc("count"))
        value_counts = value_counts.collect()
        series = pd.Series(
            [row["count"] for row in value_counts],
            index=pd.Index(data=[row[column] for row in value_counts], name="value"),
            name="count",
        )
        return series

    def get_column_unique_count(self, column):
        return self.spark_df.agg(countDistinct(column)).collect()[0][0]

    def get_column_modes(self, column):
        """leverages computation done in _get_column_value_counts"""
        s = self.get_column_value_counts(column)
        return list(s[s == s.max()].index)

    def get_column_median(self, column):
        # We will get the two middle values by choosing an epsilon to add
        # to the 50th percentile such that we always get exactly the middle two values
        # (i.e. 0 < epsilon < 1 / (2 * values))

        # Note that this can be an expensive computation; we are not exposing
        # spark's ability to estimate.
        # We add two to 2 * n_values to maintain a legitimate quantile
        # in the degnerate case when n_values = 0
        result = self.spark_df.approxQuantile(
            column, [0.5, 0.5 + (1 / (2 + (2 * self.get_row_count())))], 0
        )
        return np.mean(result)

    def get_column_quantiles(self, column, quantiles, allow_relative_error=False):
        if allow_relative_error is False:
            allow_relative_error = 0.0
        if (
            not isinstance(allow_relative_error, float)
            or allow_relative_error < 0
            or allow_relative_error > 1
        ):
            raise ValueError(
                "SparkDFDataset requires relative error to be False or to be a float between 0 and 1."
            )
        return self.spark_df.approxQuantile(
            column, list(quantiles), allow_relative_error
        )

    def get_column_stdev(self, column):
        return self.spark_df.select(stddev_samp(col(column))).collect()[0][0]

    def get_column_hist(self, column, bins):
        """return a list of counts corresponding to bins"""
        bins = list(
            copy.deepcopy(bins)
        )  # take a copy since we are inserting and popping
        if bins[0] == -np.inf or bins[0] == -float("inf"):
            added_min = False
            bins[0] = -float("inf")
        else:
            added_min = True
            bins.insert(0, -float("inf"))

        if bins[-1] == np.inf or bins[-1] == float("inf"):
            added_max = False
            bins[-1] = float("inf")
        else:
            added_max = True
            bins.append(float("inf"))

        temp_column = self.spark_df.select(column).where(col(column).isNotNull())
        bucketizer = Bucketizer(splits=bins, inputCol=column, outputCol="buckets")
        bucketed = bucketizer.setHandleInvalid("skip").transform(temp_column)

        # This is painful to do, but: bucketizer cannot handle values outside of a range
        # (hence adding -/+ infinity above)

        # Further, it *always* follows the numpy convention of lower_bound <= bin < upper_bound
        # for all but the last bin

        # But, since the last bin in our case will often be +infinity, we need to
        # find the number of values exactly equal to the upper bound to add those

        # We'll try for an optimization by asking for it at the same time
        if added_max:
            upper_bound_count = (
                temp_column.select(column).filter(col(column) == bins[-2]).count()
            )
        else:
            upper_bound_count = 0

        hist_rows = bucketed.groupBy("buckets").count().collect()
        # Spark only returns buckets that have nonzero counts.
        hist = [0] * (len(bins) - 1)
        for row in hist_rows:
            hist[int(row["buckets"])] = row["count"]

        hist[-2] += upper_bound_count

        if added_min:
            below_bins = hist.pop(0)
            bins.pop(0)
            if below_bins > 0:
                logger.warning("Discarding histogram values below lowest bin.")

        if added_max:
            above_bins = hist.pop(-1)
            bins.pop(-1)
            if above_bins > 0:
                logger.warning("Discarding histogram values above highest bin.")

        return hist

    def get_column_count_in_range(
        self, column, min_val=None, max_val=None, strict_min=False, strict_max=True
    ):
        if min_val is None and max_val is None:
            raise ValueError("Must specify either min or max value")
        if min_val is not None and max_val is not None and min_val > max_val:
            raise ValueError("Min value must be <= to max value")

        result = self.spark_df.select(column)
        if min_val is not None:
            if strict_min:
                result = result.filter(col(column) > min_val)
            else:
                result = result.filter(col(column) >= min_val)
        if max_val is not None:
            if strict_max:
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
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        if value_set is None:
            # vacuously true
            return column.withColumn("__success", lit(True))
        if parse_strings_as_datetimes:
            column = self._apply_dateutil_parse(column)
            value_set = [
                parse(value) if isinstance(value, str) else value for value in value_set
            ]
        if None in value_set:
            # spark isin returns None when any value is compared to None
            logger.error(
                "expect_column_values_to_be_in_set cannot support a None in the value_set in spark"
            )
            raise ValueError(
                "expect_column_values_to_be_in_set cannot support a None in the value_set in spark"
            )
        return column.withColumn("__success", column[0].isin(value_set))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_not_be_in_set(
        self,
        column,  # pyspark.sql.DataFrame
        value_set,  # List[Any]
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        if None in value_set:
            # spark isin returns None when any value is compared to None
            logger.error(
                "expect_column_values_to_not_be_in_set cannot support a None in the value_set in spark"
            )
            raise ValueError(
                "expect_column_values_to_not_be_in_set cannot support a None in the value_set in spark"
            )
        return column.withColumn("__success", ~column[0].isin(value_set))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_be_between(
        self,
        column,
        min_value=None,
        max_value=None,
        strict_min=False,
        strict_max=False,
        parse_strings_as_datetimes=None,
        output_strftime_format=None,
        allow_cross_type_comparisons=None,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        # NOTE: This function is implemented using native functions instead of UDFs, which is a faster
        # implementation. Please ensure new spark implementations migrate to the new style where possible
        if allow_cross_type_comparisons:
            raise ValueError("Cross-type comparisons are not valid for SparkDFDataset")

        if parse_strings_as_datetimes:
            min_value = parse(min_value)
            max_value = parse(max_value)

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")
        elif min_value is None:
            if strict_max:
                return column.withColumn(
                    "__success",
                    when(column[0] < max_value, lit(True)).otherwise(lit(False)),
                )
            else:
                return column.withColumn(
                    "__success",
                    when(column[0] <= max_value, lit(True)).otherwise(lit(False)),
                )
        elif max_value is None:
            if strict_min:
                return column.withColumn(
                    "__success",
                    when(column[0] > min_value, lit(True)).otherwise(lit(False)),
                )
            else:
                return column.withColumn(
                    "__success",
                    when(column[0] >= min_value, lit(True)).otherwise(lit(False)),
                )
        else:
            if min_value > max_value:
                raise ValueError("minvalue cannot be greater than max_value")
            if strict_min and strict_max:
                return column.withColumn(
                    "__success",
                    when(
                        (min_value < column[0]) & (column[0] < max_value), lit(True)
                    ).otherwise(lit(False)),
                )
            elif strict_min:
                return column.withColumn(
                    "__success",
                    when(
                        (min_value < column[0]) & (column[0] <= max_value), lit(True)
                    ).otherwise(lit(False)),
                )
            elif strict_max:
                return column.withColumn(
                    "__success",
                    when(
                        (min_value <= column[0]) & (column[0] < max_value), lit(True)
                    ).otherwise(lit(False)),
                )
            else:
                return column.withColumn(
                    "__success",
                    when(
                        (min_value <= column[0]) & (column[0] <= max_value), lit(True)
                    ).otherwise(lit(False)),
                )

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_value_lengths_to_be_between(
        self,
        column,
        min_value=None,
        max_value=None,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        if min_value is None and max_value is None:
            return column.withColumn("__success", lit(True))
        elif min_value is None:
            return column.withColumn(
                "__success",
                when(length_(column[0]) <= max_value, lit(True)).otherwise(lit(False)),
            )
        elif max_value is None:
            return column.withColumn(
                "__success",
                when(length_(column[0]) >= min_value, lit(True)).otherwise(lit(False)),
            )
        # FIXME: whether the below condition is enforced seems to be somewhat inconsistent

        # else:
        #     if min_value > max_value:
        #         raise ValueError("minvalue cannot be greater than max_value")

        return column.withColumn(
            "__success",
            when(
                (min_value <= length_(column[0])) & (length_(column[0]) <= max_value),
                lit(True),
            ).otherwise(lit(False)),
        )

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_be_unique(
        self,
        column,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        return column.withColumn(
            "__success", count(lit(1)).over(Window.partitionBy(column[0])) <= 1
        )

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_value_lengths_to_equal(
        self,
        column,
        value,  # int
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        return column.withColumn(
            "__success",
            when(length_(column[0]) == value, lit(True)).otherwise(lit(False)),
        )

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_match_strftime_format(
        self,
        column,
        strftime_format,  # str
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        # Below is a simple validation that the provided format can both format and parse a datetime object.
        # %D is an example of a format that can format but not parse, e.g.
        try:
            datetime.strptime(
                datetime.strftime(datetime.now(), strftime_format), strftime_format
            )
        except ValueError as e:
            raise ValueError("Unable to use provided strftime_format. " + e.message)

        def is_parseable_by_format(val):
            try:
                datetime.strptime(val, strftime_format)
                return True
            except TypeError:
                raise TypeError(
                    "Values passed to expect_column_values_to_match_strftime_format must be of type string.\nIf you want to validate a column of dates or timestamps, please call the expectation before converting from string format."
                )
            except ValueError:
                return False

        success_udf = udf(is_parseable_by_format)
        return column.withColumn("__success", success_udf(column[0]))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_not_be_null(
        self,
        column,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        return column.withColumn("__success", column[0].isNotNull())

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_be_null(
        self,
        column,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        return column.withColumn("__success", column[0].isNull())

    @DocInherit
    @DataAsset.expectation(["column", "type_", "mostly"])
    def expect_column_values_to_be_of_type(
        self,
        column,
        type_,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        if mostly is not None:
            raise ValueError(
                "SparkDFDataset does not support column map semantics for column types"
            )

        try:
            col_data = [f for f in self.spark_df.schema.fields if f.name == column][0]
            col_type = type(col_data.dataType)
        except IndexError:
            raise ValueError("Unrecognized column: %s" % column)
        except KeyError:
            raise ValueError("No type data available for column: %s" % column)

        try:
            if type_ is None:
                # vacuously true
                success = True
            else:
                success = issubclass(col_type, getattr(sparktypes, type_))

            return {"success": success, "result": {"observed_value": col_type.__name__}}

        except AttributeError:
            raise ValueError("Unrecognized spark type: %s" % type_)

    @DocInherit
    @DataAsset.expectation(["column", "type_", "mostly"])
    def expect_column_values_to_be_in_type_list(
        self,
        column,
        type_list,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        if mostly is not None:
            raise ValueError(
                "SparkDFDataset does not support column map semantics for column types"
            )

        try:
            col_data = [f for f in self.spark_df.schema.fields if f.name == column][0]
            col_type = type(col_data.dataType)
        except IndexError:
            raise ValueError("Unrecognized column: %s" % column)
        except KeyError:
            raise ValueError("No database type data available for column: %s" % column)

        if type_list is None:
            success = True
        else:
            types = []
            for type_ in type_list:
                try:
                    type_class = getattr(sparktypes, type_)
                    types.append(type_class)
                except AttributeError:
                    logger.debug("Unrecognized type: %s" % type_)
            if len(types) == 0:
                raise ValueError("No recognized spark types in type_list")
            types = tuple(types)
            success = issubclass(col_type, types)
        return {"success": success, "result": {"observed_value": col_type.__name__}}

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_match_regex(
        self,
        column,
        regex,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        return column.withColumn("__success", column[0].rlike(regex))

    @DocInherit
    @MetaSparkDFDataset.column_map_expectation
    def expect_column_values_to_not_match_regex(
        self,
        column,
        regex,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        return column.withColumn("__success", ~column[0].rlike(regex))

    @DocInherit
    @MetaSparkDFDataset.column_pair_map_expectation
    def expect_column_pair_values_to_be_equal(
        self,
        column_A,
        column_B,
        ignore_row_if="both_values_are_missing",
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        column_A_name = column_A.schema.names[1]
        column_B_name = column_B.schema.names[1]
        join_df = column_A.join(
            column_B, column_A["__row"] == column_B["__row"], how="inner"
        )
        return join_df.withColumn(
            "__success",
            when(col(column_A_name) == col(column_B_name), True).otherwise(False),
        )

    @DocInherit
    @MetaSparkDFDataset.column_pair_map_expectation
    def expect_column_pair_values_A_to_be_greater_than_B(
        self,
        column_A,
        column_B,
        or_equal=None,
        parse_strings_as_datetimes=None,
        allow_cross_type_comparisons=None,
        ignore_row_if="both_values_are_missing",
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        # FIXME
        if allow_cross_type_comparisons:
            raise NotImplementedError

        column_A_name = column_A.schema.names[1]
        column_B_name = column_B.schema.names[1]

        if parse_strings_as_datetimes:
            _udf = udf(parse, sparktypes.TimestampType())
            # Create new columns for comparison without replacing original values.
            (timestamp_column_A, timestamp_column_B) = (
                "__ts_{0}".format(column_A_name),
                "__ts_{0}".format(column_B_name),
            )
            temp_column_A = column_A.withColumn(timestamp_column_A, _udf(column_A_name))
            temp_column_B = column_B.withColumn(timestamp_column_B, _udf(column_B_name))
            # Use the new columns to compare instead of original columns.
            (column_A_name, column_B_name) = (timestamp_column_A, timestamp_column_B)

        else:
            temp_column_A = column_A
            temp_column_B = column_B

        join_df = temp_column_A.join(
            temp_column_B, temp_column_A["__row"] == temp_column_B["__row"], how="inner"
        )

        if or_equal:
            return join_df.withColumn(
                "__success",
                when(col(column_A_name) >= col(column_B_name), True).otherwise(False),
            )
        else:
            return join_df.withColumn(
                "__success",
                when(col(column_A_name) > col(column_B_name), True).otherwise(False),
            )

    @DocInherit
    @MetaSparkDFDataset.multicolumn_map_expectation
    def expect_multicolumn_values_to_be_unique(
        self,
        column_list,  # pyspark.sql.DataFrame
        ignore_row_if="all_values_are_missing",
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        column_names = column_list.schema.names[:]
        conditions = []
        for i in range(0, len(column_names) - 1):
            # Negate the `eqNullSafe` result and append to the conditions.
            conditions.append(
                ~(col(column_names[i]).eqNullSafe(col(column_names[i + 1])))
            )

        return column_list.withColumn(
            "__success", reduce(lambda a, b: a & b, conditions)
        )
