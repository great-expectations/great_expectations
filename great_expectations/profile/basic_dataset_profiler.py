import logging
from .base import DatasetProfiler
from ..dataset.util import build_categorical_partition_object

logger = logging.getLogger(__name__)


class BasicDatasetProfilerBase(DatasetProfiler):
    """BasicDatasetProfilerBase provides basic logic of inferring the type and the cardinality of columns
    that is used by the dataset profiler classes that extend this class.
    """

    INT_TYPE_NAMES = {"INTEGER", "int", "INT", "TINYINT", "BYTEINT", "SMALLINT", "BIGINT", "IntegerType", "LongType", "DECIMAL"}
    FLOAT_TYPE_NAMES = {"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE_PRECISION", "NUMERIC", "FloatType", "DoubleType", "float"}
    STRING_TYPE_NAMES = {"CHAR", "VARCHAR", "TEXT", "StringType", "string", "str"}
    BOOLEAN_TYPE_NAMES = {"BOOLEAN", "BOOL", "bool", "BooleanType"}
    DATETIME_TYPE_NAMES = {"DATETIME", "DATE", "TIMESTAMP", "DateType", "TimestampType", "datetime64", "Timestamp"}

    @classmethod
    def _get_column_type(cls, df, column):
        # list of types is used to support pandas and sqlalchemy
        df.set_config_value("interactive_evaluation", True)
        try:
            if df.expect_column_values_to_be_in_type_list(column, type_list=sorted(list(cls.INT_TYPE_NAMES))).success:
                type_ = "int"

            elif df.expect_column_values_to_be_in_type_list(column, type_list=sorted(list(cls.FLOAT_TYPE_NAMES))).success:
                type_ = "float"

            elif df.expect_column_values_to_be_in_type_list(column, type_list=sorted(list(cls.STRING_TYPE_NAMES))).success:
                type_ = "string"

            elif df.expect_column_values_to_be_in_type_list(column, type_list=sorted(list(cls.BOOLEAN_TYPE_NAMES))).success:
                type_ = "bool"

            elif df.expect_column_values_to_be_in_type_list(column, type_list=sorted(list(cls.DATETIME_TYPE_NAMES))).success:
                type_ = "datetime"

            else:
                df.expect_column_values_to_be_in_type_list(column, type_list=None)
                type_ = "unknown"
        except NotImplementedError:
            type_ = "unknown"

        df.set_config_value('interactive_evaluation', False)
        return type_

    @classmethod
    def _get_column_cardinality(cls, df, column):
        num_unique = None
        pct_unique = None
        df.set_config_value("interactive_evaluation", True)

        try:
            num_unique = df.expect_column_unique_value_count_to_be_between(column, None, None).result['observed_value']
            pct_unique = df.expect_column_proportion_of_unique_values_to_be_between(
                column, None, None).result['observed_value']
        except KeyError:  # if observed_value value is not set
            logger.error("Failed to get cardinality of column {0:s} - continuing...".format(column))

        if num_unique is None or num_unique == 0 or pct_unique is None:
            cardinality = "none"

        elif pct_unique == 1.0:
            cardinality = "unique"

        elif pct_unique > .1:
            cardinality = "very many"

        elif pct_unique > .02:
            cardinality = "many"

        else:
            cardinality = "complicated"
            if num_unique == 1:
                cardinality = "one"

            elif num_unique == 2:
                cardinality = "two"

            elif num_unique < 60:
                cardinality = "very few"

            elif num_unique < 1000:
                cardinality = "few"

            else:
                cardinality = "many"
        # print('col: {0:s}, num_unique: {1:s}, pct_unique: {2:s}, card: {3:s}'.format(column, str(num_unique), str(pct_unique), cardinality))

        df.set_config_value('interactive_evaluation', False)

        return cardinality




class BasicDatasetProfiler(BasicDatasetProfilerBase):
    """BasicDatasetProfiler is inspired by the beloved pandas_profiling project.

    The profiler examines a batch of data and creates a report that answers the basic questions
    most data practitioners would ask about a dataset during exploratory data analysis.
    The profiler reports how unique the values in the column are, as well as the percentage of empty values in it.
    Based on the column's type it provides a description of the column by computing a number of statistics,
    such as min, max, mean and median, for numeric columns, and distribution of values, when appropriate.
    """

    @classmethod
    def _profile(cls, dataset):
        df = dataset

        df.set_default_expectation_argument("catch_exceptions", True)

        df.expect_table_row_count_to_be_between(min_value=0, max_value=None)
        df.expect_table_columns_to_match_ordered_list(None)
        df.set_config_value('interactive_evaluation', False)

        columns = df.get_table_columns()

        meta_columns = {}
        for column in columns:
            meta_columns[column] = {"description": ""}

        number_of_columns = len(columns)
        for i, column in enumerate(columns):
            logger.info("            Preparing column {} of {}: {}".format(i+1, number_of_columns, column))

            # df.expect_column_to_exist(column)

            type_ = cls._get_column_type(df, column)
            cardinality = cls._get_column_cardinality(df, column)
            df.expect_column_values_to_not_be_null(column, mostly=0.5) # The renderer will show a warning for columns that do not meet this expectation
            df.expect_column_values_to_be_in_set(column, [], result_format="SUMMARY")

            if type_ == "int":
                if cardinality == "unique":
                    df.expect_column_values_to_be_unique(column)
                elif cardinality in ["one", "two", "very few", "few"]:
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format="SUMMARY")
                elif cardinality in ["many", "very many", "unique"]:
                    df.expect_column_min_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_max_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_mean_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_median_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_stdev_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_quantile_values_to_be_between(column,
                                                                   quantile_ranges={
                                                                       "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                                                                       "value_ranges": [[None, None], [None, None], [None, None], [None, None], [None, None]]
                                                                   }
                                                                   )
                    df.expect_column_kl_divergence_to_be_less_than(column, partition_object=None,
                                                           threshold=None, result_format='COMPLETE')
                else: # unknown cardinality - skip
                    pass
            elif type_ == "float":
                if cardinality == "unique":
                    df.expect_column_values_to_be_unique(column)

                elif cardinality in ["one", "two", "very few", "few"]:
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format="SUMMARY")

                elif cardinality in ["many", "very many", "unique"]:
                    df.expect_column_min_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_max_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_mean_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_median_to_be_between(column, min_value=None, max_value=None)
                    df.expect_column_quantile_values_to_be_between(column,
                                                                   quantile_ranges={
                                                                       "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                                                                       "value_ranges": [[None, None], [None, None], [None, None], [None, None], [None, None]]
                                                                   }
                                                                   )
                    df.expect_column_kl_divergence_to_be_less_than(column, partition_object=None,

                                                           threshold=None, result_format='COMPLETE')
                else:  # unknown cardinality - skip
                    pass

            elif type_ == "string":
                # Check for leading and trailing whitespace.
                #!!! It would be nice to build additional Expectations here, but
                #!!! the default logic for remove_expectations prevents us.
                df.expect_column_values_to_not_match_regex(column, r"^\s+|\s+$")

                if cardinality == "unique":
                    df.expect_column_values_to_be_unique(column)

                elif cardinality in ["one", "two", "very few", "few"]:
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format="SUMMARY")
                else:
                    # print(column, type_, cardinality)
                    pass

            elif type_ == "datetime":
                df.expect_column_min_to_be_between(column, min_value=None, max_value=None)

                df.expect_column_max_to_be_between(column, min_value=None, max_value=None)

                # Re-add once kl_divergence has been modified to support datetimes
                # df.expect_column_kl_divergence_to_be_less_than(column, partition_object=None,
                #                                            threshold=None, result_format='COMPLETE')

                if cardinality in ["one", "two", "very few", "few"]:
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format="SUMMARY")



            else:
                if cardinality == "unique":
                    df.expect_column_values_to_be_unique(column)

                elif cardinality in ["one", "two", "very few", "few"]:
                    df.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format="SUMMARY")
                else:
                    # print(column, type_, cardinality)
                    pass

        df.set_config_value("interactive_evaluation", True)
        expectation_suite = df.get_expectation_suite(suppress_warnings=True, discard_failed_expectations=False)
        expectation_suite.meta["columns"] = meta_columns

        return expectation_suite


class SampleExpectationsDatasetProfiler(BasicDatasetProfilerBase):
    """The goal of SampleExpectationsDatasetProfiler is to generate an expectation suite that
    contains one instance of every interesting expectation type.

    This expectation suite is intended to serve as a demo of the expressive power of expectations
    and provide a service similar to the one expectations glossary documentation page, but on
    users' own data.

    Ranges of acceptable values in the expectations created by this profiler (e.g., min/max
    of the median in expect_column_median_to_be_between) are created only to demonstrate
    the functionality and should not be taken as the actual ranges outside which the data
    should be considered incorrect.
    """

    @classmethod
    def _get_column_type_with_caching(cls, dataset, column_name, cache):
        column_cache_entry = cache.get(column_name)
        if not column_cache_entry:
            column_cache_entry = {}
            cache[column_name] = column_cache_entry
        column_type = column_cache_entry.get("type")
        if not column_type:
            column_type = cls._get_column_type(dataset, column_name)
            column_cache_entry["type"] = column_type
            # remove the expectation
            dataset.remove_expectation(expectation_type="expect_column_values_to_be_in_type_list")
            dataset.set_config_value('interactive_evaluation', True)

        return column_type


    @classmethod
    def _get_column_cardinality_with_caching(cls, dataset, column_name, cache):
        column_cache_entry = cache.get(column_name)
        if not column_cache_entry:
            column_cache_entry = {}
            cache[column_name] = column_cache_entry
        column_cardinality = column_cache_entry.get("cardinality")
        if not column_cardinality:
            column_cardinality = cls._get_column_cardinality(dataset, column_name)
            column_cache_entry["cardinality"] = column_cardinality
            # remove the expectations
            dataset.remove_expectation(expectation_type="expect_column_unique_value_count_to_be_between")
            dataset.remove_expectation(expectation_type="expect_column_proportion_of_unique_values_to_be_between")
            dataset.set_config_value('interactive_evaluation', True)

        return column_cardinality

    @classmethod
    def _create_expectations_for_low_card_column(cls, dataset, column):
        cls._create_non_nullity_expectations(dataset, column)

        value_set = \
        dataset.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format="SUMMARY").result[
            "observed_value"]
        dataset.expect_column_distinct_values_to_be_in_set(column, value_set=value_set, result_format="SUMMARY")
        partition_object = build_categorical_partition_object(dataset, column)

        dataset.expect_column_kl_divergence_to_be_less_than(column, partition_object=partition_object,
                                                            threshold=0.6)

    @classmethod
    def _create_non_nullity_expectations(cls, dataset, column):
        not_null_result = dataset.expect_column_values_to_not_be_null(column)
        if not not_null_result.success:
            mostly_value = max(0.001, (100.0 - not_null_result.result["unexpected_percent"] - 10) / 100.0)
            dataset.expect_column_values_to_not_be_null(column, mostly=mostly_value)

    @classmethod
    def _create_expectations_for_numeric_column(cls, dataset, column):
        cls._create_non_nullity_expectations(dataset, column)

        value = \
        dataset.expect_column_min_to_be_between(column, min_value=None, max_value=None, result_format="SUMMARY").result[
            "observed_value"]
        value = dataset.expect_column_min_to_be_between(column, min_value=value - 1, max_value=value + 1)

        value = \
        dataset.expect_column_max_to_be_between(column, min_value=None, max_value=None, result_format="SUMMARY").result[
            "observed_value"]
        value = dataset.expect_column_max_to_be_between(column, min_value=value - 1, max_value=value + 1)

        value = dataset.expect_column_mean_to_be_between(column, min_value=None, max_value=None,
                                                         result_format="SUMMARY").result["observed_value"]
        dataset.expect_column_mean_to_be_between(column, min_value=value - 1, max_value=value + 1)

        value = dataset.expect_column_median_to_be_between(column, min_value=None, max_value=None,
                                                           result_format="SUMMARY").result["observed_value"]
        dataset.expect_column_median_to_be_between(column, min_value=value - 1, max_value=value + 1)

        result = dataset.expect_column_quantile_values_to_be_between(column,
                                                                     quantile_ranges={
                                                                         "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                                                                         "value_ranges": [[None, None], [None, None],
                                                                                          [None, None], [None, None],
                                                                                          [None, None]]
                                                                     },
                                                                     result_format="SUMMARY"
                                                                     )
        dataset.expect_column_quantile_values_to_be_between(column,
                                                            quantile_ranges={
                                                                "quantiles": result.result["observed_value"][
                                                                    "quantiles"],
                                                                "value_ranges": [[v - 1, v + 1] for v in
                                                                                 result.result["observed_value"][
                                                                                     "values"]]
                                                            }
                                                            )

    @classmethod
    def _create_expectations_for_string_column(cls, dataset, column):
        cls._create_non_nullity_expectations(dataset, column)
        dataset.expect_column_value_lengths_to_be_between(column, min_value=1)


    @classmethod
    def _find_next_low_card_column(cls, dataset, columns, profiled_columns, column_cache):
        for column in columns:
            if column in profiled_columns["low_card"]:
                continue
            cardinality = cls._get_column_cardinality_with_caching(dataset, column, column_cache)
            if cardinality in ["two", "very few", "few"]:
                return column

        return None


    @classmethod
    def _find_next_numeric_column(cls, dataset, columns, profiled_columns, column_cache):
        for column in columns:
            if column in profiled_columns["numeric"]:
                continue
            if column.lower().strip() == "id" or column.lower().strip().find("_id") > -1:
                continue

            cardinality = cls._get_column_cardinality_with_caching(dataset, column, column_cache)
            type = cls._get_column_type_with_caching(dataset, column, column_cache)

            if cardinality in ["many", "very many", "unique"] and type in ["int", "float"]:
                return column

        return None

    @classmethod
    def _find_next_string_column(cls, dataset, columns, profiled_columns, column_cache):
        for column in columns:
            if column in profiled_columns["string"]:
                continue

            cardinality = cls._get_column_cardinality_with_caching(dataset, column, column_cache)
            type = cls._get_column_type_with_caching(dataset, column, column_cache)

            if cardinality in ["many", "very many", "unique"] and type not in ["int", "float"]:
                return column

        return None

    @classmethod
    def _profile(cls, dataset):

        dataset.set_default_expectation_argument("catch_exceptions", True)

        value = dataset.expect_table_row_count_to_be_between(min_value=0, max_value=None).result["observed_value"]
        dataset.expect_table_row_count_to_be_between(min_value=max(0, value-10), max_value=value+10)

        dataset.set_config_value('interactive_evaluation', True)

        columns = dataset.get_table_columns()

        dataset.expect_table_column_count_to_equal(len(columns))
        dataset.expect_table_columns_to_match_ordered_list(columns)

        meta_columns = {}
        for column in columns:
            meta_columns[column] = {"description": ""}

        column_cache = {}
        profiled_columns = {
            "numeric": [],
            "low_card": [],
            "string": []
        }

        column = cls._find_next_low_card_column(dataset, columns, profiled_columns, column_cache)
        if column:
            cls._create_expectations_for_low_card_column(dataset, column)
            profiled_columns["low_card"].append(column)


        column = cls._find_next_numeric_column(dataset, columns, profiled_columns, column_cache)
        if column:
            cls._create_expectations_for_numeric_column(dataset, column)
            profiled_columns["low_card"].append(column)


        column = cls._find_next_string_column(dataset, columns, profiled_columns, column_cache)
        if column:
            cls._create_expectations_for_string_column(dataset, column)
            profiled_columns["low_card"].append(column)




        expectation_suite = dataset.get_expectation_suite(suppress_warnings=True, discard_failed_expectations=True)
        if not expectation_suite.meta:
            expectation_suite.meta = {"columns": meta_columns, "notes": {""}}
        else:
            expectation_suite.meta["columns"] = meta_columns

        expectation_suite.meta["notes"] = {
            "format": "markdown",
            "content": [
                """#### This is an _example_ suite

- This suite was made by quickly glancing at 1000 rows of your data.
- This is **not a production suite**. It is meant to show examples of expectations.
- Because this suite was auto-generated using a very basic profiler that does not know your data like you do, many of the expectations may not be meaningful.
"""
            ]
        }

        return expectation_suite

