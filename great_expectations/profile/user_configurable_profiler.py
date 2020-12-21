import datetime
import decimal
from typing import Iterable

import numpy as np
from dateutil.parser import parse

from great_expectations.core import ExpectationSuite
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.dataset.util import build_categorical_partition_object
from great_expectations.exceptions import ProfilerError
from great_expectations.profile.base import ProfilerCardinality, ProfilerDataType
from great_expectations.profile.basic_dataset_profiler import (
    BasicDatasetProfilerBase,
    logger,
)


class UserConfigurableProfiler(BasicDatasetProfilerBase):
    # TODO: Confirm tolerance for every expectation

    # TODO: Configure excluded expectations and excluded columns

    # TODO: Add profiler

    """
    This profiler helps build strict expectations for the purposes of ensuring
    that two tables are the same.

    config = {
                “primary_key”: [“col_a”, “col_b”],
                “value_set”: [“gender”,”country_code”,”user_type”],
                “numeric”: [“age”, “months_subscribed”],
                “date”: [“start_date”],
                “text”: [“country_code”],
            }

        or

    config = {
                “user_id”: [“primary_key”],
                “gender”: [“value_set”],
                “age”: [“numeric”],
                “start_date”: [“date”],
                “country_code”: [“string”, “value_set”]
            }


    Separate suite builder, which takes a dictionary where the keys are columns and the values are the expectations to
    build for those columns. The profiler will populate this dictionary in different ways. It might take a prepopulated
    set of the columns and expectations and add to them.


    The table_profile method returns a dictionary where they keys are domain tuples, and the values are dictionaries of
    expectations with their success_kwargs. After profiling, this profile object can be inspected and edited. Example

    profile = {
                ("user_id,) = { "expect_column_values_to_be_in_set":{
                                                                        "value_set
                                                                    }


                                }
                }

    We need to get a list of ExpectationConfiguration objects and use those to initialize a new suite. We should also
    provide convenience methods to inspect and modify that list of ECs (view by expectation type, view by domain, remove
    individual ECs, remove by column, remove by expectation type.
    """
    semantic_type_functions = {
        "datetime": "_build_expectations_datetime",
        "numeric": "_build_expectations_numeric",
        "primary_or_compound_key": "_build_expectations_primary_or_compound_key",
        "primary_key": "_build_expectations_primary_or_compound_key",
        "compound_key": "_build_expectations_primary_or_compound_key",
        "string": "_build_expectations_string",
        "value_set": "_build_expectations_value_set",
        "boolean": "_build_expectations_boolean",
        "other": "_build_expectations_other",
    }

    @classmethod
    def _get_column_names(cls, dataset):
        pass

    @classmethod
    def _get_column_types(cls, dataset):
        pass

    @classmethod
    def table_profile(
        cls, dataset, type_config, primary_or_compound_key=None, tolerance=0
    ) -> dict:
        semantic_types = {
            "datetime",
            "numeric",
            "primary_or_compound_key",
            "string",
            "value_set",
            "boolean",
            "other",
        }

    @classmethod
    def _build_expectations_value_set(cls, dataset, column, cache=None):
        if "expect_column_distinct_values_to_be_in_set" not in cache.get(
            "excluded_expectations"
        ):
            value_set = dataset.expect_column_distinct_values_to_be_in_set(
                column, value_set=None, result_format="SUMMARY"
            ).result["observed_value"]

            dataset.expect_column_values_to_be_in_set(column, value_set=value_set)

    @classmethod
    def _build_expectations_numeric(cls, dataset, column, tolerance=0, cache=None):
        # TODO: Apply the lossy conversion when creating the expectations themselves
        # min
        if "expect_column_min_to_be_between" not in cache.get("excluded_expectations"):
            observed_min = dataset.expect_column_min_to_be_between(
                column, min_value=None, max_value=None, result_format="SUMMARY"
            ).result["observed_value"]
            # TODO: Add logic for cases where the min is a string (confirm types) or where column has multiple types
            if not _is_nan(observed_min):
                # places = len(str(observed_min)[str(observed_min).find('.') + 1:])
                # tolerance = 10 ** int(-places)
                # tolerance = float(decimal.Decimal.from_float(float(observed_min)) - decimal.Decimal(str(observed_min)))
                dataset.expect_column_min_to_be_between(
                    column,
                    min_value=observed_min - tolerance,
                    max_value=observed_min + tolerance,
                )

            else:
                dataset.remove_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_min_to_be_between",
                        kwargs={"column": column},
                    ),
                    match_type="domain",
                )
                logger.debug(
                    f"Skipping expect_column_min_to_be_between because observed value is nan: {observed_min}"
                )

        # max
        if "expect_column_max_to_be_between" not in cache.get("excluded_expectations"):
            observed_max = dataset.expect_column_max_to_be_between(
                column, min_value=None, max_value=None, result_format="SUMMARY"
            ).result["observed_value"]
            if not _is_nan(observed_max):
                # tolerance = float(decimal.Decimal.from_float(float(observed_max)) - decimal.Decimal(str(observed_max)))
                dataset.expect_column_max_to_be_between(
                    column,
                    min_value=observed_max - tolerance,
                    max_value=observed_max + tolerance,
                )

            else:
                dataset.remove_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_max_to_be_between",
                        kwargs={"column": column},
                    ),
                    match_type="domain",
                )
                logger.debug(
                    f"Skipping expect_column_max_to_be_between because observed value is nan: {observed_max}"
                )

        # mean
        if "expect_column_mean_to_be_between" not in cache.get("excluded_expectations"):
            observed_mean = dataset.expect_column_mean_to_be_between(
                column, min_value=None, max_value=None, result_format="SUMMARY"
            ).result["observed_value"]
            if not _is_nan(observed_mean):
                # tolerance = float(decimal.Decimal.from_float(float(observed_mean)) - decimal.Decimal(str(observed_mean)))
                dataset.expect_column_mean_to_be_between(
                    column,
                    min_value=observed_mean - tolerance,
                    max_value=observed_mean + tolerance,
                )

            else:
                dataset.remove_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_mean_to_be_between",
                        kwargs={"column": column},
                    ),
                    match_type="domain",
                )
                logger.debug(
                    f"Skipping expect_column_mean_to_be_between because observed value is nan: {observed_mean}"
                )

        # median
        if "expect_column_median_to_be_between" not in cache.get(
            "excluded_expectations"
        ):
            observed_median = dataset.expect_column_median_to_be_between(
                column, min_value=None, max_value=None, result_format="SUMMARY"
            ).result["observed_value"]
            if not _is_nan(observed_median):
                # places = len(str(observed_median)[str(observed_median).find('.') + 1:])
                # tolerance = 10 ** int(-places)
                # tolerance = float(decimal.Decimal.from_float(float(observed_median)) - decimal.Decimal(str(observed_median)))
                dataset.expect_column_median_to_be_between(
                    column,
                    min_value=observed_median - tolerance,
                    max_value=observed_median + tolerance,
                )

            else:
                dataset.remove_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_median_to_be_between",
                        kwargs={"column": column},
                    ),
                    match_type="domain",
                )
                logger.debug(
                    f"Skipping expect_column_median_to_be_between because observed value is nan: {observed_median}"
                )

        # quantile values
        if "expect_column_quantile_values_to_be_between" not in cache.get(
            "excluded_expectations"
        ):
            allow_relative_error: bool = dataset.attempt_allowing_relative_error()
            quantile_result = dataset.expect_column_quantile_values_to_be_between(
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
                allow_relative_error=allow_relative_error,
                result_format="SUMMARY",
                catch_exceptions=True,
            )
            if quantile_result.exception_info and (
                quantile_result.exception_info["exception_traceback"]
                or quantile_result.exception_info["exception_message"]
            ):
                # TODO quantiles are not implemented correctly on sqlite, and likely other sql dialects
                dataset.remove_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_quantile_values_to_be_between",
                        kwargs={"column": column},
                    ),
                    match_type="domain",
                )
                logger.debug(quantile_result.exception_info["exception_traceback"])
                logger.debug(quantile_result.exception_info["exception_message"])
            else:
                dataset.set_config_value("interactive_evaluation", False)

                dataset.expect_column_quantile_values_to_be_between(
                    column,
                    quantile_ranges={
                        "quantiles": quantile_result.result["observed_value"][
                            "quantiles"
                        ],
                        "value_ranges": [
                            [v - 1, v + 1]
                            for v in quantile_result.result["observed_value"]["values"]
                        ],
                    },
                    allow_relative_error=allow_relative_error,
                    catch_exceptions=True,
                )
                dataset.set_config_value("interactive_evaluation", True)

    @classmethod
    def _build_expectations_primary_or_compound_key(
        cls, dataset, column_or_domain, cache=None
    ):
        # expectation_list = []

        # uniqueness
        if isinstance(column_or_domain, list):
            if len(
                column_or_domain
            ) > 1 and "expect_compound_column_values_to_be_unique" not in cache.get(
                "excluded_expectations"
            ):
                dataset.expect_compound_column_values_to_be_unique(column_or_domain)
            elif len(column_or_domain) < 1:
                raise ValueError(
                    "When specifying a primary or compound key, column_or_domain must not be empty"
                )
            else:
                [column] = column_or_domain
        else:
            column = column_or_domain
        if "expect_column_values_to_be_unique" not in cache.get(
            "excluded_expectations"
        ):
            dataset.expect_column_values_to_be_unique(column)

    @classmethod
    def _build_expectations_string(cls, dataset, column, tolerance=0, cache=None):
        print(column)
        # value_lengths
        # TODO: Figure out if there is a way to introspect to get value_lengths
        if "expect_column_value_lengths_to_be_between" not in cache.get(
            "excluded_expectations"
        ):
            result = dataset.expect_column_value_lengths_to_be_between(
                column, min_value=0, max_value=0, result_format="SUMMARY"
            )
            observed_value_lengths = result.result["observed_value"]

            if not _is_nan(observed_value_lengths):
                dataset.expect_column_value_lengths_to_be_between(
                    column, min_value=0, max_value=observed_value_lengths + tolerance
                )
            else:
                logger.debug(
                    f"Skipping expect_column_value_lengths_to_be_between because observed value is nan: {observed_value_lengths}"
                )

    @classmethod
    def _build_expectations_datetime(cls, dataset, column, tolerance=0, cache=None):
        if "expect_column_values_to_be_between" not in cache.get(
            "excluded_expectations"
        ):
            # TODO: Confirm that tolerance here makes sense
            min_value = dataset.expect_column_min_to_be_between(
                column,
                min_value=None,
                max_value=None,
                parse_strings_as_datetimes=True,
                result_format="SUMMARY",
            ).result["observed_value"]

            if min_value is not None:
                try:
                    min_value = min_value + datetime.timedelta(days=-365 * tolerance)
                except OverflowError:
                    min_value = datetime.datetime.min
                except TypeError:
                    min_value = parse(min_value) + datetime.timedelta(
                        days=(-365 * tolerance)
                    )

            dataset.remove_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_min_to_be_between",
                    kwargs={"column": column},
                ),
                match_type="domain",
            )

            max_value = dataset.expect_column_max_to_be_between(
                column,
                min_value=None,
                max_value=None,
                parse_strings_as_datetimes=True,
                result_format="SUMMARY",
            ).result["observed_value"]
            if max_value is not None:
                try:
                    max_value = max_value + datetime.timedelta(days=(365 * tolerance))
                except OverflowError:
                    max_value = datetime.datetime.max
                except TypeError:
                    max_value = parse(max_value) + datetime.timedelta(
                        days=(365 * tolerance)
                    )

            dataset.remove_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_max_to_be_between",
                    kwargs={"column": column},
                ),
                match_type="domain",
            )
            if min_value is not None or max_value is not None:
                dataset.expect_column_values_to_be_between(
                    column,
                    min_value=min_value,
                    max_value=max_value,
                    parse_strings_as_datetimes=True,
                )

    @classmethod
    def _build_expectations_other(cls, dataset, column, tolerance=0, cache=None):
        # TODO: Test nullity or non nullity expectation
        if "expect_column_values_to_not_be_null" not in cache.get(
            "excluded_expectations"
        ):
            not_null_result = dataset.expect_column_values_to_not_be_null(column)
            if not not_null_result.success:
                unexpected_percent = float(not_null_result.result["unexpected_percent"])
                if unexpected_percent >= 50:
                    potential_mostly_value = (unexpected_percent + tolerance) / 100.0
                    safe_mostly_value = round(potential_mostly_value, 3)
                    dataset.expect_column_values_to_be_null(
                        column, mostly=safe_mostly_value
                    )
                else:
                    potential_mostly_value = (
                        100.0 - unexpected_percent - tolerance
                    ) / 100.0
                    safe_mostly_value = round(max(0.001, potential_mostly_value), 3)
                    dataset.expect_column_values_to_not_be_null(
                        column, mostly=safe_mostly_value
                    )
        if "expect_column_proportion_of_unique_values_to_be_between" not in cache.get(
            "excluded_expectations"
        ):
            pct_unique = dataset.expect_column_proportion_of_unique_values_to_be_between(
                column, None, None
            ).result[
                "observed_value"
            ]

            if not _is_nan(pct_unique):
                dataset.expect_column_proportion_of_unique_values_to_be_between(
                    column, min_value=pct_unique, max_value=pct_unique
                )
            else:
                dataset.remove_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_proportion_of_unique_values_to_be_between",
                        kwargs={"column": column},
                    ),
                    match_type="domain",
                )

                logger.debug(
                    f"Skipping expect_column_proportion_of_unique_values_to_be_between because observed value is nan: {pct_unique}"
                )

    @classmethod
    def _build_expectations_table(cls, dataset, tolerance=0, cache=None):
        # expectation_list = []
        if "expect_table_columns_to_match_ordered_list" not in cache.get(
            "excluded_expectations"
        ):
            columns = dataset.get_table_columns()
            dataset.expect_table_columns_to_match_ordered_list(columns)

        if "expect_table_row_count_to_be_between" not in cache.get(
            "excluded_expectations"
        ):
            row_count = dataset.expect_table_row_count_to_be_between(
                min_value=0, max_value=None
            ).result["observed_value"]
            min_value = max(0, int(row_count * (1 - tolerance)))
            max_value = int(row_count * (1 + tolerance))

            dataset.expect_table_row_count_to_be_between(
                min_value=min_value, max_value=max_value
            )

    @classmethod
    def build_expectation_list(cls, dataset, config, tolerance=0, cache=None):
        cache = {}
        cls._set_up_metadata_with_caching(dataset, config, cache=cache)
        semantic_type_dict = config.get("semantic_types")
        for semantic_type, column_list in semantic_type_dict.items():
            if semantic_type in ("boolean", "value_set"):
                for column in column_list:
                    cls._build_expectations_value_set(dataset, column, cache=cache)
            elif semantic_type == "datetime":
                for column in column_list:
                    cls._build_expectations_datetime(dataset, column, cache=cache)
            elif semantic_type == "numeric":
                for column in column_list:
                    cls._build_expectations_numeric(dataset, column, cache=cache)
            elif semantic_type in (
                "primary_or_compound_key",
                "primary_key",
                "compound_key",
            ):
                cls._build_expectations_primary_or_compound_key(
                    dataset, column_list, cache=cache
                )
            elif semantic_type == "string":
                pass
                # for column in column_list:
                #     cls._build_expectations_string(dataset, column, cache=cache)
            else:
                logger.warn(f"Semantic_type: {semantic_type} is unknown. Skipping")

        columns = dataset.get_table_columns()
        ignored_columns = config.get("ignored_columns")
        included_columns = set(columns) - set(ignored_columns)

        for column in included_columns:
            cls._build_expectations_other(dataset, column, cache=cache)

        # for expectation in expectation_list:
        #     print(expectation)
        expectation_suite = cls._build_column_description_metadata(dataset)
        logger.debug("")
        cls.display_suite_by_column(expectation_suite)  # include in the actual profiler

        return expectation_suite

    @classmethod
    def _set_up_metadata_with_caching(cls, dataset, config, cache):  # TODO: Rename
        semantic_type_dict = config.get("semantic_types")
        cache["ignored_columns"] = config.get("ignored_columns")
        # TODO: Add check to make sure the type_list expectation is not in excluded expectations.
        cache["excluded_expectations"] = set(config["excluded_expectations"])
        # cache["column_data_types"] = {
        #     column: cls._get_column_type_and_build_type_expectations_with_caching(dataset, column, cache) for column in dataset.get_table_columns()
        # }
        # cache["column_cardinality"] = {
        #     column: cls._get_column_cardinality_with_caching(dataset, column, cache) for column in dataset.get_table_columns()
        # }
        # cache["semantic_types_by_column"] = {
        #     column: semantic_type for semantic_type, column_list in semantic_type_dict.items() for column in column_list
        # }
        for column_name in dataset.get_table_columns():
            cls._get_column_cardinality_with_caching(dataset, column_name, cache)
            cls._get_column_type_and_build_type_expectations_with_caching(
                dataset, column_name, cache
            )
            cls._add_semantic_types_by_column_from_config_to_cache(
                dataset, config, column_name, cache
            )

    @classmethod
    def _validate_config(cls, dataset, config, cache=None):
        selected_columns = [
            column
            for column_list in config.get("semantic_types").values()
            for column in column_list
        ]
        _check_that_columns_exist(dataset, selected_columns)

        dataset.set_default_expectation_argument("catch_exceptions", False)

        if cache is None:
            cache = {}

        if not cache.get("column_data_types"):
            cls._set_up_metadata_with_caching(dataset, config, cache)

        semantic_type_dict = config.get("semantic_types")

        for semantic_type, column_list in semantic_type_dict.items():
            if semantic_type in ("boolean", "value_set"):
                pass  # maybe confirm low cardinality
            elif semantic_type == "datetime":
                pass  # confirm datetime or numeric or string maybe?
            elif semantic_type == "numeric":
                pass  # confirm any of the numeric data types
            elif semantic_type in (
                "primary_or_compound_key",
                "primary_key",
                "compound_key",
            ):
                pass  # can we do anything here?
            elif semantic_type == "string":
                pass  # do we have any string expectations? is this necessary?
            else:
                logger.warn(f"Semantic_type: {semantic_type} is unknown. Skipping")

    @classmethod
    def _get_column_type_and_build_type_expectations_with_caching(
        cls, dataset, column_name, cache
    ):
        column_cache_entry = cache.get(column_name)
        if not column_cache_entry:
            column_cache_entry = {}
            cache[column_name] = column_cache_entry
        column_type = column_cache_entry.get("type")
        # TODO: Ensure that not getting column type won't mess anything else up.
        if not column_type and column_name not in cache["ignored_columns"]:
            column_type = cls._get_column_type(dataset, column_name)
            column_cache_entry["type"] = column_type

            if (
                "expect_column_values_to_be_in_type_list"
                in cache["excluded_expectations"]
            ):
                # remove the expectation
                # TODO: Does this change with different config format?
                dataset.remove_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_in_type_list",
                        kwargs={"column": column_name},
                    )
                )
            dataset.set_config_value("interactive_evaluation", True)

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
            dataset.remove_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_unique_value_count_to_be_between",
                    kwargs={"column": column_name},
                )
            )
            dataset.remove_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_proportion_of_unique_values_to_be_between",
                    kwargs={"column": column_name},
                )
            )
            dataset.set_config_value("interactive_evaluation", True)

        return column_cardinality

    @classmethod
    def _add_semantic_types_by_column_from_config_to_cache(
        cls, dataset, config, column_name, cache
    ):
        column_cache_entry = cache.get(column_name)
        if not column_cache_entry:
            column_cache_entry = {}
            cache[column_name] = column_cache_entry

        semantic_types = cache.get("semantic_types")

        if not semantic_types:
            semantic_type_dict = config.get("semantic_types")
            semantic_types = []
            for semantic_type, column_list in semantic_type_dict.items():
                if column_name in column_list:
                    semantic_types.append(semantic_type)
            column_cache_entry["semantic_types"] = semantic_types

        return semantic_types

    @classmethod
    def _build_column_description_metadata(cls, dataset):
        columns = dataset.get_table_columns()
        expectation_suite = dataset.get_expectation_suite(
            suppress_warnings=True, discard_failed_expectations=False
        )

        meta_columns = {}
        for column in columns:
            meta_columns[column] = {"description": ""}
        if not expectation_suite.meta:
            expectation_suite.meta = {"columns": meta_columns, "notes": {""}}
        else:
            expectation_suite.meta["columns"] = meta_columns

        return expectation_suite

    @classmethod
    def build_suite(cls, suite_name, expectation_list):
        # probably not a necessary function.
        return ExpectationSuite(suite_name, expectations=expectation_list)

    @classmethod
    def display_suite_by_column(cls, suite, verbose=False):
        expectations = suite.expectations
        expectations_by_column = {}
        for expectation in expectations:
            domain = expectation["kwargs"].get("column") or expectation["kwargs"].get(
                "column_list"
            )
            if expectations_by_column.get(domain) is None:
                expectations_by_column[domain] = [expectation]
            else:
                expectations_by_column[domain].append(expectation)

        if verbose:
            for column in expectations_by_column:
                print(column)
                for expectation in expectations_by_column.get(column):
                    print(expectation)
                print("\n")

        else:
            for column in expectations_by_column:
                print(column)
                for expectation in expectations_by_column.get(column):
                    print(expectation["expectation_type"])
                print("\n")

    @classmethod
    def _get_column_cardinality(cls, df, column):
        num_unique = None
        pct_unique = None
        df.set_config_value("interactive_evaluation", True)

        try:
            num_unique = df.expect_column_unique_value_count_to_be_between(
                column, None, None
            ).result["observed_value"]
            pct_unique = df.expect_column_proportion_of_unique_values_to_be_between(
                column, None, None
            ).result["observed_value"]
        except KeyError:  # if observed_value value is not set
            logger.error(
                "Failed to get cardinality of column {:s} - continuing...".format(
                    column
                )
            )

        if num_unique is None or num_unique == 0 or pct_unique is None:
            cardinality = ProfilerCardinality.NONE
        elif pct_unique == 1.0:
            cardinality = ProfilerCardinality.UNIQUE
        elif pct_unique > 0.1:
            cardinality = ProfilerCardinality.VERY_MANY
        elif pct_unique > 0.02:
            cardinality = ProfilerCardinality.MANY
        else:
            if num_unique == 1:
                cardinality = ProfilerCardinality.ONE
            elif num_unique == 2:
                cardinality = ProfilerCardinality.TWO

            # This previously was 60, which seems very high. Lowering it arbitrarily to something that makes sense, but I'm open.
            elif num_unique < 20:
                cardinality = ProfilerCardinality.VERY_FEW

            # This previously was 1000, which seems very high. Lowering it arbitrarily to something that makes sense, but I'm open.
            elif num_unique < 50:
                cardinality = ProfilerCardinality.FEW
            else:
                cardinality = ProfilerCardinality.MANY

        df.set_config_value("interactive_evaluation", False)

        return cardinality


def _check_that_expectations_are_available(dataset, expectations):
    if expectations:
        for expectation in expectations:
            if expectation not in dataset.list_available_expectation_types():
                raise ProfilerError(f"Expectation {expectation} is not available.")


def _check_that_columns_exist(dataset, columns):
    if columns:
        for column in columns:
            if column not in dataset.get_table_columns():
                raise ProfilerError(f"Column {column} does not exist.")


def _is_nan(value):
    try:
        return np.isnan(value)
    except TypeError:
        return False
