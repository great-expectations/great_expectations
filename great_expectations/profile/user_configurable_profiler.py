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

    # TODO: Figure out how to build in a minimal tolerance when going between pandas and sql (and look into the reason
    #  for this - is it also a float to decimal issue)?
    """
    This profiler helps build strict expectations for the purposes of ensuring
    that two tables are the same.

    config = { "semantic_types":
               {
                "numeric": ["c_acctbal"],
                "string": ["c_address","c_custkey"],
                "value_set": ["c_nationkey","c_mktsegment", 'c_custkey', 'c_name', 'c_address', 'c_phone'],
                },
            "ignored_columns": ignored_columns,
            "excluded_expectations":[],
            "value_set_threshold": ["unique"]
            "primary_or_compound_key": ["c_name", "c_custkey"]
    }

        or


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
        "string": "_build_expectations_string",
        "value_set": "_build_expectations_value_set",
        "boolean": "_build_expectations_value_set",
        "other": "_build_expectations_other",
    }

    _cardinality_enumeration = {
        "none": 0,
        "one": 1,
        "two": 2,
        "very_few": 3,
        "few": 4,
        "many": 5,
        "very_many": 6,
        "unique": 7,
    }
    _semantic_types = {
        "datetime",
        "numeric",
        "string",
        "value_set",
        "boolean",
        "other",
    }

    @classmethod
    def build_expectation_list_from_config(cls, dataset, config, tolerance=0):
        cache = cls._initialize_cache_with_metadata(dataset, config=config)
        cls._validate_config(dataset, config, cache)
        cls._build_expectations_table(dataset, cache=cache)
        ignored_columns = cache.get("ignored_columns") or {}
        value_set_threshold = cache.get("value_set_threshold")
        cache_columns = {
            k: v
            for k, v in cache.items()
            if (
                isinstance(v, dict)
                and "cardinality" in v.keys()
                and k not in ignored_columns
            )
        }
        primary_or_compound_key = cache.get("primary_or_compound_key")
        if primary_or_compound_key is not None:
            cls._build_expectations_primary_or_compound_key(
                dataset, primary_or_compound_key, cache=cache
            )

        for column_name, column_info in cache_columns.items():
            semantic_types = column_info.get("semantic_types")
            for semantic_type in semantic_types:
                semantic_type_fn = cls.semantic_type_functions.get(semantic_type)
                getattr(cls, semantic_type_fn)(dataset, column_name, cache, tolerance)

        for column_name in cache_columns.keys():
            cls._build_expectations_other(dataset, column_name, cache=cache)

        expectation_suite = cls._build_column_description_metadata(dataset)
        logger.debug("")
        cls.display_suite_by_column(expectation_suite)  # include in the actual profiler
        for k, v in cache.items():  # TODO: remove printing cache
            print(k, v)
        return expectation_suite

    @classmethod
    def profile_and_build_expectation_list(cls, dataset, config=None, tolerance=0):
        # TODO: enable a partial config - like just allowing excluded_expectations without semantic types
        cache = cls._initialize_cache_with_metadata(dataset=dataset, config=config)
        ignored_columns = cache.get("ignored_columns") or {}
        value_set_threshold = cache.get("value_set_threshold")
        cache_columns = {
            k: v
            for k, v in cache.items()
            if (
                isinstance(v, dict)
                and "cardinality" in v.keys()
                and k not in ignored_columns
            )
        }
        primary_or_compound_key = cache.get("primary_or_compound_key")
        if primary_or_compound_key is not None:
            cls._build_expectations_primary_or_compound_key(
                dataset=dataset, column_list=primary_or_compound_key, cache=cache
            )
        cls._build_expectations_table(dataset=dataset, cache=cache, tolerance=tolerance)
        for column_name, column_info in cache_columns.items():
            data_type = column_info.get("type").value  # change types to enums
            cardinality = column_info.get("cardinality")

            if data_type in ("float", "int"):
                cls._build_expectations_numeric(
                    dataset=dataset,
                    column=column_name,
                    cache=cache,
                    tolerance=tolerance,
                )

            if data_type == "datetime":
                cls._build_expectations_datetime(
                    dataset=dataset,
                    column=column_name,
                    cache=cache,
                    tolerance=tolerance,
                )

            if cls._cardinality_enumeration.get(
                *value_set_threshold
            ) >= cls._cardinality_enumeration.get(cardinality):
                cls._build_expectations_value_set(
                    dataset=dataset, column=column_name, cache=cache
                )

            cls._build_expectations_other(
                dataset=dataset, column=column_name, cache=cache
            )

        expectation_suite = cls._build_column_description_metadata(dataset)
        logger.debug("")
        cls.display_suite_by_column(expectation_suite)  # include in the actual profiler
        for k, v in cache.items():
            print(k, v)
        return expectation_suite

    @classmethod
    def _initialize_cache_with_metadata(cls, dataset, config=None):
        cache = {}
        if config is not None:
            semantic_type_dict = config.get("semantic_types")
            cache["primary_or_compound_key"] = config.get("primary_or_compound_key")
            cache["ignored_columns"] = config.get("ignored_columns")
            cache["value_set_threshold"] = config.get("value_set_threshold") or ["many"]
            # TODO: Add check to make sure the type_list expectation is not in excluded expectations.
            cache["excluded_expectations"] = config.get("excluded_expectations")

        included_columns = [
            column_name
            for column_name in dataset.get_table_columns()
            if column_name not in cache.get("ignored_columns")
        ]
        for column_name in included_columns:
            cls._get_column_cardinality_with_caching(dataset, column_name, cache)
            cls._get_column_type_and_build_type_expectations_with_caching(
                dataset, column_name, cache
            )
            if config is not None and config.get("semantic_types") is not None:
                cls._add_semantic_types_by_column_from_config_to_cache(
                    dataset, config, column_name, cache
                )

        return cache

    @classmethod
    def _validate_config(cls, dataset, config, cache=None):
        semantic_type_dict = config.get("semantic_types")
        for k, v in semantic_type_dict.items():
            assert isinstance(v, list), (
                "Entries in semantic type dict must be lists rather than scalars e.g. "
                '{"semantic_types": {"numeric": ["number_of_transactions"]}}'
            )
            if k not in cls._semantic_types:
                logger.warn(
                    f"{k} is not a recognized semantic_type and will be skipped."
                )

        selected_columns = [
            column
            for column_list in semantic_type_dict.values()
            for column in column_list
        ]
        _check_that_columns_exist(dataset, selected_columns)

        dataset.set_default_expectation_argument("catch_exceptions", False)

        if cache is None:
            cls._initialize_cache_with_metadata(dataset, config)

        cached_config_parameters = (
            "ignored_columns",
            "excluded_expectations",
            "primary_or_compound_key",
            "value_set_threshold",
        )
        for config_parameter in cached_config_parameters:
            param = cache.get(config_parameter)
            if param is not None:
                assert isinstance(
                    param, list
                ), f"{config_parameter} must be formatted as a list rather than a scalar"

        cached_columns = {
            k: v
            for k, v in cache.items()
            if k not in cached_config_parameters and len(v.get("semantic_types")) > 0
        }
        # primary_or_compound_key = cache.get("primary_or_compound_key")
        # if primary_or_compound_key is not None:
        # if primary_or_compound_key is not None:
        #     if len(primary_or_compound_key) == 1:
        #         column_name = [primary_or_compound_key]
        #         assert cached_columns.get(column_name).get("cardinality").value == "unique", \
        #             f"The values in column {column_name} are not unique"

        for column_name, column_info in cached_columns.items():
            config_semantic_types = column_info["semantic_types"]
            for semantic_type in config_semantic_types:
                if semantic_type == "datetime":
                    assert column_info["type"].value in ("datetime", "string"), (
                        f"Column {column_name} must be a datetime column or a string but appears to be "
                        f"{column_info['type'].value}"
                    )
                elif semantic_type == "numeric":
                    assert column_info["type"].value in (
                        "int",
                        "float",
                    ), f"Column {column_name} must be an int or a float but appears to be {column_info['type'].value}"
                elif semantic_type in ("string", "value_set"):
                    pass  # do we have any string expectations?
                # elif semantic_type in ("boolean", "value_set"):
                #     if column_info["cardinality"] in ("many", "very many", "unique"):
                #         logger.warn(f"Column {column_name} appears to have high cardinality. Creating a "
                #                     f"{semantic_type} expectation, but ensure that this is correctly configured.")
                # else:
                #     logger.warn(f"Semantic_type: {semantic_type} is unknown. Skipping")

    @classmethod
    def _get_column_type_and_build_type_expectations_with_caching(
        cls, dataset, column_name, cache
    ):
        column_cache_entry = cache.get(column_name)
        if not column_cache_entry:
            column_cache_entry = {}
            cache[column_name] = column_cache_entry
        column_type = column_cache_entry.get("type")
        if not column_type:
            column_type = cls._get_column_type(dataset, column_name)
            column_cache_entry["type"] = column_type

            if (
                "expect_column_values_to_be_in_type_list"
                in cache["excluded_expectations"]
            ):
                # remove the expectation
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

        semantic_types = column_cache_entry.get("semantic_types")

        if not semantic_types:
            semantic_type_dict = config.get("semantic_types")
            semantic_types = []
            for semantic_type, column_list in semantic_type_dict.items():
                if column_name in column_list and semantic_type in cls._semantic_types:
                    semantic_types.append(semantic_type)
                # if semantic_type not in cls._semantic_types:
                #     logger.warn(f"ADD Semantic_type: {semantic_type} is unknown. Skipping")
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
    def display_suite_by_column(cls, suite, verbose=False):
        expectations = suite.expectations
        expectations_by_column = {}
        for expectation in expectations:
            domain = expectation["kwargs"].get("column") or "table_level_expectations"
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
        # TODO: Currently, if we have 25 possible categories out of 1000 rows, this comes up as many, because
        #  it's greater 0.02. This should come up as few instead. Figure out the bins
        if num_unique is None or num_unique == 0 or pct_unique is None:
            cardinality = "none"
        elif pct_unique == 1.0:
            cardinality = "unique"
        elif pct_unique > 0.1:
            cardinality = "very_many"
        elif pct_unique > 0.02:
            cardinality = "many"
        else:
            if num_unique == 1:
                cardinality = "one"
            elif num_unique == 2:
                cardinality = "two"

            elif num_unique < 60:
                cardinality = "very_few"

            # This previously was 1000, which seems very high. Lowering it arbitrarily to something that makes sense, but I'm open.
            elif num_unique < 120:
                cardinality = "few"
            else:
                cardinality = "many"

        df.set_config_value("interactive_evaluation", False)

        return cardinality

    @classmethod
    def _build_expectations_value_set(cls, dataset, column, cache=None, tolerance=0):
        if "expect_column_distinct_values_to_be_in_set" not in cache.get(
            "excluded_expectations"
        ):
            value_set = dataset.expect_column_distinct_values_to_be_in_set(
                column, value_set=None, result_format="SUMMARY"
            ).result["observed_value"]

            dataset.remove_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_distinct_values_to_be_in_set",
                    kwargs={"column": column},
                ),
                match_type="domain",
            )

            dataset.expect_column_values_to_be_in_set(column, value_set=value_set)

    @classmethod
    def _build_expectations_numeric(cls, dataset, column, cache=None, tolerance=0):
        # min
        if "expect_column_min_to_be_between" not in cache.get("excluded_expectations"):
            observed_min = dataset.expect_column_min_to_be_between(
                column, min_value=None, max_value=None, result_format="SUMMARY"
            ).result["observed_value"]
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
        cls, dataset, column_list, cache=None
    ):
        # uniqueness
        if len(
            column_list
        ) > 1 and "expect_compound_columns_to_be_unique" not in cache.get(
            "excluded_expectations"
        ):
            dataset.expect_compound_columns_to_be_unique(column_list)
        elif len(column_list) < 1:
            raise ValueError(
                "When specifying a primary or compound key, column_list must not be empty"
            )
        else:
            [column] = column_list
            if "expect_column_values_to_be_unique" not in cache.get(
                "excluded_expectations"
            ):
                dataset.expect_column_values_to_be_unique(column)

    @classmethod
    def _build_expectations_string(cls, dataset, column, cache=None, tolerance=0):
        # value_lengths

        if "expect_column_value_lengths_to_be_between" not in cache.get(
            "excluded_expectations"
        ):
            # With the 0.12 API there isn't a quick way to introspect for value_lengths - if we did that, we could
            #  build a potentially useful value_lengths expectation here.
            pass

    @classmethod
    def _build_expectations_datetime(cls, dataset, column, cache=None, tolerance=0):
        if "expect_column_values_to_be_between" not in cache.get(
            "excluded_expectations"
        ):
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
    def _build_expectations_other(cls, dataset, column, cache=None, tolerance=0):
        if "expect_column_values_to_not_be_null" not in cache.get(
            "excluded_expectations"
        ):
            not_null_result = dataset.expect_column_values_to_not_be_null(column)
            if not not_null_result.success:
                unexpected_percent = float(not_null_result.result["unexpected_percent"])
                if unexpected_percent >= 50:
                    potential_mostly_value = (unexpected_percent + tolerance) / 100.0
                    safe_mostly_value = round(potential_mostly_value, 3)
                    dataset.remove_expectation(
                        ExpectationConfiguration(
                            expectation_type="expect_column_values_to_not_be_null",
                            kwargs={"column": column},
                        ),
                        match_type="domain",
                    )
                    if "expect_column_values_to_be_null" not in cache.get(
                        "excluded_expectations"
                    ):
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
            pct_unique = (
                dataset.expect_column_proportion_of_unique_values_to_be_between(
                    column, None, None
                ).result["observed_value"]
            )

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
    def _build_expectations_table(cls, dataset, cache=None, tolerance=0):
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

    # @classmethod
    # @property
    # def _cardinality_enumeration(cls):
    #     return {
    #         "none": 0,
    #         "one": 1,
    #         "two": 2,
    #         "very_few": 3,
    #         "few": 4,
    #         "many": 5,
    #         "very_many": 6,
    #         "unique": 7
    #     }


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
