import datetime

from dateutil.parser import parse

from great_expectations.dataset.util import build_categorical_partition_object
from great_expectations.profile.basic_dataset_profiler import (
    BasicDatasetProfilerBase,
    logger,
)


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
    def _create_expectations_for_low_card_column(cls, dataset, column, column_cache):
        cls._create_non_nullity_expectations(dataset, column)

        value_set = \
        dataset.expect_column_distinct_values_to_be_in_set(column, value_set=None, result_format="SUMMARY").result[
            "observed_value"]
        dataset.expect_column_distinct_values_to_be_in_set(column, value_set=value_set, result_format="SUMMARY")

        if cls._get_column_cardinality_with_caching(dataset, column, column_cache) in ["two", "very few"]:
            partition_object = build_categorical_partition_object(dataset, column)
            dataset.expect_column_kl_divergence_to_be_less_than(column, partition_object=partition_object,
                                                                threshold=0.6, catch_exceptions=True)

    @classmethod
    def _create_non_nullity_expectations(cls, dataset, column):
        not_null_result = dataset.expect_column_values_to_not_be_null(column)
        if not not_null_result.success:
            unexpected_percent = float(not_null_result.result["unexpected_percent"])
            mostly_value = max(0.001, (100.0 - unexpected_percent - 10) / 100.0)
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

        result = dataset.expect_column_quantile_values_to_be_between(
            column,
            quantile_ranges={
                "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                "value_ranges": [
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                ],
            },
            result_format="SUMMARY",
            catch_exceptions=True
        )
        if result.exception_info and (
                result.exception_info["exception_traceback"]
                or result.exception_info["exception_message"]
        ):
            # TODO quantiles are not implemented correctly on sqlite, and likely other sql dialects
            logger.debug(result.exception_info["exception_traceback"])
            logger.debug(result.exception_info["exception_message"])
        else:
            dataset.set_config_value('interactive_evaluation', False)
            dataset.expect_column_quantile_values_to_be_between(
                column,
                quantile_ranges={
                    "quantiles": result.result["observed_value"]["quantiles"],
                    "value_ranges": [
                        [v - 1, v + 1] for v in
                        result.result["observed_value"]["values"]
                    ],
                },
                catch_exceptions=True
            )
            dataset.set_config_value('interactive_evaluation', True)

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

            if cardinality in ["many", "very many", "unique"] and type in ["string", "unknown"]:
                return column

        return None

    @classmethod
    def _find_next_datetime_column(cls, dataset, columns, profiled_columns, column_cache):
        for column in columns:
            if column in profiled_columns["datetime"]:
                continue

            cardinality = cls._get_column_cardinality_with_caching(dataset, column, column_cache)
            type = cls._get_column_type_with_caching(dataset, column, column_cache)

            if cardinality in ["many", "very many", "unique"] and type in ["datetime"]:
                return column

        return None

    @classmethod
    def _create_expectations_for_datetime_column(cls, dataset, column):
        cls._create_non_nullity_expectations(dataset, column)

        min_value = \
        dataset.expect_column_min_to_be_between(column, min_value=None, max_value=None, result_format="SUMMARY").result[
            "observed_value"]

        if min_value is not None:
            dataset.remove_expectation(expectation_type="expect_column_min_to_be_between", column=column)
            try:
                min_value = min_value + datetime.timedelta(days=-365)
            except OverflowError as o_err:
                min_value = datetime.datetime.min
            except TypeError as o_err:
                min_value = parse(min_value) + datetime.timedelta(days=-365)


        max_value = \
        dataset.expect_column_max_to_be_between(column, min_value=None, max_value=None, result_format="SUMMARY").result[
            "observed_value"]
        if max_value is not None:
            dataset.remove_expectation(expectation_type="expect_column_max_to_be_between", column=column)
            try:
                max_value = max_value + datetime.timedelta(days=365)
            except OverflowError as o_err:
                max_value = datetime.datetime.max
            except TypeError as o_err:
                max_value = parse(max_value) + datetime.timedelta(days=365)

        if min_value is not None or max_value is not None:
            dataset.expect_column_values_to_be_between(column, min_value, max_value, parse_strings_as_datetimes=True)


    @classmethod
    def _profile(cls, dataset):

        dataset.set_default_expectation_argument("catch_exceptions", False)

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
            "string": [],
            "datetime": []
        }

        column = cls._find_next_low_card_column(dataset, columns, profiled_columns, column_cache)
        if column:
            cls._create_expectations_for_low_card_column(dataset, column, column_cache)
            profiled_columns["low_card"].append(column)


        column = cls._find_next_numeric_column(dataset, columns, profiled_columns, column_cache)
        if column:
            cls._create_expectations_for_numeric_column(dataset, column)
            profiled_columns["numeric"].append(column)


        column = cls._find_next_string_column(dataset, columns, profiled_columns, column_cache)
        if column:
            cls._create_expectations_for_string_column(dataset, column)
            profiled_columns["string"].append(column)

        column = cls._find_next_datetime_column(dataset, columns, profiled_columns, column_cache)
        if column:
            cls._create_expectations_for_datetime_column(dataset, column)
            profiled_columns["datetime"].append(column)

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
