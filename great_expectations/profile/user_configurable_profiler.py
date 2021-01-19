import datetime
import decimal
from typing import Iterable

import numpy as np
from dateutil.parser import parse

from great_expectations.core import ExpectationSuite
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.dataset.util import build_categorical_partition_object
from great_expectations.exceptions import ProfilerError
from great_expectations.profile.base import (
    OrderedProfilerCardinality,
    ProfilerCardinality,
    ProfilerDataType,
    ProfilerDataTypesWithMapping,
    ProfilerSemanticTypes,
    ProfilerTypeMapping,
)
from great_expectations.profile.basic_dataset_profiler import (
    BasicDatasetProfilerBase,
    logger,
)


class UserConfigurableProfiler(BasicDatasetProfilerBase):
    # TODO: Confirm tolerance for every expectation

    """
    The UserConfigurableProfiler is used to build an expectation suite from a dataset. The expectations built are
    strict - they can be used to determine whether two tables are the same.

    The profiler may be instantiated with or without a config. The config may contain a semantic_types dict or not. Once
    a profiler is instantiated, if config items change, a new profiler will be needed.

    A profiler is used to build a suite without a config as follows:

    profiler = UserConfigurableProfiler(dataset)
    suite = profiler.build_suite()

    A profiler is used to build a suite with a config, but without a semantic_types dict, as follows:

    example_config_without_semantic_types = {
                    "ignored_columns": ignored_columns,
                    "excluded_expectations":["expect_column_values_to_be_between"],
                    "value_set_threshold": "unique"
                    "primary_or_compound_key": ["user_id"],
                    "table_expectations_only": False,
                    "not_null_only": False,
                }

    profiler = UserConfigurableProfiler(dataset, config=example_config_without_semantic_types)
    suite = profiler.build_suite()


    A profiler is used to build a suite with a config and a semantic_types dict, as follows:

    example_config_with_semantic_types = { "semantic_types":
               {
                "numeric": ["c_acctbal"],
                "string": ["c_address","c_custkey"],
                "value_set": ["c_nationkey","c_mktsegment", 'c_custkey', 'c_name', 'c_address', 'c_phone'],
                },
            "ignored_columns": ignored_columns,
            "excluded_expectations":[],
            "value_set_threshold": "unique"
            "primary_or_compound_key": ["c_name", "c_custkey"],
            "table_expectations_only": False
    }

    profiler = UserConfigurableProfiler(dataset, config=example_config_with_semantic_types)
    suite = profiler.build_suite()
    """

    def __init__(self, dataset, config=None):
        """
        The UserConfigurableProfiler is used to build an expectation suite from a dataset. The profiler may be
        instantiated with or without a config. The config may contain a semantic_types dict or not. Once a profiler is
        instantiated, if config items change, a new profiler will be needed.

        Args:
            dataset: A GE dataset object
            config: A dictionary which can contain any or all of the following:
                "excluded_expectations": A list of expectations to not include in the suite
                "ignored_columns": A list of columns for which you would like to NOT create expectations
                "not_null_only": Boolean, default False. By default, each column is evaluated for nullity. If the column
                    values contain fewer than 50% null values, then the profiler will add
                    `expect_column_values_to_not_be_null`; if greater than 50% it will add
                    `expect_column_values_to_be_null`. If not_null_only is set to True, the profiler will add a
                    not_null expectation irrespective of the percent nullity (and therefore will not add an
                    `expect_column_values_to_be_null`
                "primary_or_compound_key": A list containing one or more columns which are a dataset's primary or
                    compound key. This will create an `expect_column_values_to_be_unique` or
                    `expect_compound_columns_to_be_unique` expectation. This will occur even if one or more of the
                    primary_or_compound_key columns are specified in ignored_columns
                "semantic_types": A dictionary where the keys are available semantic_types (see ProfilerSemanticTypes)
                    and the values are lists of columns for which you would like to create semantic_type specific
                    expectations e.g.:
                    "semantic_types": { "value_set": ["state","country"], "numeric":["age", "amount_due"]}
                "table_expectations_only": Boolean, default False. If True, this will only create the two table level expectations
                    available to this profiler (`expect_table_columns_to_match_ordered_list` and
                    `expect_table_row_count_to_be_between`). If a primary_or_compound key is specified, it will create
                    a uniqueness expectation for that column as well
                "value_set_threshold": Takes a string from the following ordered list - "none", "one", "two",
                    "very_few", "few", "many", "very_many", "unique". When the profiler runs withou a semantic_types
                    dict, each column is profiled for cardinality. This threshold determines the greatest cardinality
                    for which to add `expect_column_values_to_be_in_set`. For example, if value_set_threshold is set to
                    "unique", it will add a value_set expectation for every included column. If set to "few", it will
                    add a value_set expectation for columns whose cardinality is one of "one", "two", "very_few" or
                    "few". The default value is "many". For the purposes of comparing whether two tables are identical,
                    it might make the most sense to set this to "unique"
        """
        self.dataset = dataset
        self.config = config or {}
        self.column_info = {}
        self.semantic_type_dict = None
        if self.config is not None:
            self._validate_config(self.config)

        self.ignored_columns = self.config.get("ignored_columns") or []

        self.excluded_expectations = self.config.get("excluded_expectations") or []
        self.value_set_threshold = self.config.get("value_set_threshold")
        self.not_null_only = self.config.get("not_null_only")
        self.table_expectations_only = self.config.get("table_expectations_only")
        if self.table_expectations_only:
            self.ignored_columns = self.dataset.get_table_columns()
        self.primary_or_compound_key = self.config.get("primary_or_compound_key") or []

        if self.primary_or_compound_key:
            for column in self.primary_or_compound_key:
                if column not in dataset.get_table_columns():
                    raise ValueError(
                        f"Column {column} not found. Please ensure that this column is in the dataset if"
                        f"you would like to use it as a primary_or_compound_key."
                    )

        included_columns = [
            column_name
            for column_name in dataset.get_table_columns()
            if column_name not in self.ignored_columns
        ]

        for column_name in included_columns:
            self._add_column_cardinality_to_column_info(dataset, column_name)
            self._add_column_type_to_column_info(dataset, column_name)

        self.semantic_type_dict = self.config.get("semantic_types")
        if self.semantic_type_dict is not None:
            self._validate_semantic_types_dict(self.dataset, self.config)
            for column_name in included_columns:
                self._add_semantic_types_by_column_from_config_to_column_info(
                    column_name
                )
        self.semantic_type_functions = {
            "datetime": self._build_expectations_datetime,
            "numeric": self._build_expectations_numeric,
            "string": self._build_expectations_string,
            "value_set": self._build_expectations_value_set,
            "boolean": self._build_expectations_value_set,
            "other": self._build_expectations_for_all_column_types,
        }

    def build_suite(self, tolerance=0):
        """
        User-facing expectation-suite building function. Works with an instantiated UserConfigurableProfiler object.
        Args:
            tolerance: The tolerance to use when adding expectations

        Returns:
            An expectation suite built either with or without a semantic_types dict

        """
        if len(self.dataset.get_expectation_suite().expectations) > 0:
            suite_name = self.dataset._expectation_suite.expectation_suite_name
            self.dataset._expectation_suite = ExpectationSuite(suite_name)

        if self.table_expectations_only is True:
            logger.debug(
                "table_expectations_only is set to True. Ignoring all columns and creating expectations only \
                       at the table level"
            )

        if self.semantic_type_dict:
            return self._build_expectation_suite_from_semantic_types_dict(
                tolerance=tolerance
            )

        return self._profile_and_build_expectation_suite(tolerance=tolerance)

    def _build_expectation_suite_from_semantic_types_dict(self, tolerance=0):
        """
        Uses a semantic_type dict to determine which expectations to add to the suite, then builds the suite
        Args:
            tolerance: The tolerance to use when adding expectations

        Returns:
            An expectation suite built from a semantic_types dict
        """
        if not self.semantic_type_dict:
            raise ValueError(
                "A config with a semantic_types dict must be included in order to use this profiler."
            )
        self._build_expectations_table(self.dataset)

        if self.value_set_threshold:
            logger.debug(
                "Using this profiler with a semantic_types dict will ignore the value_set_threshold parameter. If "
                "you would like to include value_set expectations, you can include a 'value_set' entry in your "
                "semantic_types dict with any columns for which you would like a value_set expectation, or you can "
                "remove the semantic_types dict from the config."
            )

        if self.primary_or_compound_key is not None:
            self._build_expectations_primary_or_compound_key(
                self.dataset, self.primary_or_compound_key
            )

        for column_name, column_info in self.column_info.items():
            semantic_types = column_info.get("semantic_types")
            for semantic_type in semantic_types:
                semantic_type_fn = self.semantic_type_functions.get(semantic_type)
                semantic_type_fn(
                    dataset=self.dataset, column=column_name, tolerance=tolerance
                )

        for column_name in self.column_info.keys():
            self._build_expectations_for_all_column_types(self.dataset, column_name)

        expectation_suite = self._build_column_description_metadata(self.dataset)
        logger.debug("")
        self._display_suite_by_column(suite=expectation_suite)
        return expectation_suite

    def _profile_and_build_expectation_suite(self, tolerance=0):
        """
        Profiles the provided dataset to determine which expectations to add to the suite, then builds the suite
        Args:
            tolerance: The tolerance to use when adding expectations

        Returns:
            An expectation suite built after profiling the dataset
        """
        if not self.value_set_threshold:
            self.value_set_threshold = "MANY"

        if self.primary_or_compound_key:
            self._build_expectations_primary_or_compound_key(
                dataset=self.dataset, column_list=self.primary_or_compound_key
            )
        self._build_expectations_table(dataset=self.dataset, tolerance=tolerance)
        for column_name, column_info in self.column_info.items():
            data_type = column_info.get("type")
            cardinality = column_info.get("cardinality")

            if data_type in ("FLOAT", "INT", "NUMERIC"):
                self._build_expectations_numeric(
                    dataset=self.dataset,
                    column=column_name,
                    tolerance=tolerance,
                )

            if data_type == "DATETIME":
                self._build_expectations_datetime(
                    dataset=self.dataset,
                    column=column_name,
                    tolerance=tolerance,
                )

            if (
                OrderedProfilerCardinality[self.value_set_threshold.upper()]
                >= OrderedProfilerCardinality[cardinality.upper()]
            ):
                self._build_expectations_value_set(
                    dataset=self.dataset, column=column_name
                )

            self._build_expectations_for_all_column_types(
                dataset=self.dataset, column=column_name
            )

        expectation_suite = self._build_column_description_metadata(self.dataset)
        logger.debug("")
        self._display_suite_by_column(
            suite=expectation_suite
        )  # include in the actual profiler
        return expectation_suite

    def _validate_config(self, config):
        """
        Validates a config dict to ensure that value types are correct.
        Args:
            config: A config dict (see __init__ for more details)

        Returns:
            The validated config dict
        """
        config_parameters = {
            "ignored_columns": list,
            "excluded_expectations": list,
            "primary_or_compound_key": list,
            "value_set_threshold": str,
            "semantic_types": dict,
            "table_expectations_only": bool,
            "not_null_only": bool,
        }

        for k, v in config.items():
            assert (
                k in config_parameters
            ), f"Parameter {k} from config is not recognized."
            if v:
                assert isinstance(
                    v, config_parameters.get(k)
                ), f"Config parameter {k} must be formatted as a {config_parameters.get(k)} rather than a {type(v)}."

        return config

    def _validate_semantic_types_dict(self, dataset, config):
        """
        Validates a semantic_types dict to ensure correct formatting, that all semantic_types are recognized, and that
        the semantic_types align with the column data types
        Args:
            dataset: A GE dataset
            config: A config dictionary

        Returns:
            The validated semantic_types dictionary

        """
        semantic_type_dict = config.get("semantic_types")
        if not isinstance(semantic_type_dict, dict):
            raise ValueError(
                f"The semantic_types dict in the config must be a dictionary, but is currently a "
                f"{type(semantic_type_dict)}. Please reformat."
            )
        for k, v in semantic_type_dict.items():
            assert isinstance(v, list), (
                "Entries in semantic type dict must be lists of column names e.g. "
                "{'semantic_types': {'numeric': ['number_of_transactions']}}"
            )
            if k.upper() not in ProfilerSemanticTypes.__members__:
                raise ValueError(
                    f"{k} is not a recognized semantic_type. Please only include one of "
                    f"{[i for i in ProfilerSemanticTypes.__members__]}"
                )

        selected_columns = [
            column
            for column_list in semantic_type_dict.values()
            for column in column_list
        ]
        if selected_columns:
            for column in selected_columns:
                if column not in dataset.get_table_columns():
                    raise ProfilerError(f"Column {column} does not exist.")

        dataset.set_default_expectation_argument("catch_exceptions", False)

        for semantic_type, column_list in self.semantic_type_dict.items():
            for column_name in column_list:
                processed_column = self.column_info.get(column_name)
                if semantic_type == "datetime":
                    assert processed_column.get("type") in (
                        "DATETIME",
                        "STRING",
                    ), (  # TODO: Should we allow strings here?
                        f"Column {column_name} must be a datetime column or a string but appears to be "
                        f"{processed_column.get('type')}"
                    )
                elif semantic_type == "numeric":
                    assert processed_column.get("type") in (
                        "INT",
                        "FLOAT",
                        "NUMERIC",
                    ), f"Column {column_name} must be an int or a float but appears to be {processed_column.get('type')}"
                elif semantic_type in ("STRING", "VALUE_SET"):
                    pass
                # Should we validate value_set expectations if the cardinality is unexpected? This behavior conflicts
                #  with the compare two tables functionality, which is why I am not including it for now.
                # elif semantic_type in ("boolean", "value_set"):
                #     if column_info["cardinality"] in ("many", "very many", "unique"):
                #         logger.debug(f"Column {column_name} appears to have high cardinality. Creating a "
                #                     f"{semantic_type} expectation, but ensure that this is correctly configured.")
                # else:
                #     logger.debug(f"Semantic_type: {semantic_type} is unknown. Skipping")
        return semantic_type_dict

    def _add_column_type_to_column_info(self, dataset, column_name):
        """
        Adds the data type of a column to the column_info dictionary on self
        Args:
            dataset: A GE dataset
            column_name: The name of the column for which to retrieve the data type

        Returns:
            The type of the column
        """
        if "expect_column_values_to_be_in_type_list" in self.excluded_expectations:
            logger.debug(
                "expect_column_values_to_be_in_type_list is in the excluded_expectations list. This"
                "expectation is required to establish column data, so it will be run and then removed from the"
                "expectation suite."
            )
        column_info_entry = self.column_info.get(column_name)
        if not column_info_entry:
            column_info_entry = {}
            self.column_info[column_name] = column_info_entry
        column_type = column_info_entry.get("type")
        if not column_type:
            column_type = self._get_column_type(dataset, column_name)
            column_info_entry["type"] = column_type

            dataset.set_config_value("interactive_evaluation", True)

        return column_type

    def _get_column_type(self, dataset, column):
        """
        Determines the data type of a column by evaluating the success of `expect_column_values_to_be_in_type_list`.
        In the case of type Decimal, this data type is returned as NUMERIC, which contains the type lists for both INTs
        and FLOATs.

        The type_list expectation used here is removed, since it will need to be built once the build_suite function is
        actually called. This is because calling build_suite wipes any existing expectations, so expectations called
        during the init of the profiler do not persist.

        Args:
            dataset: A GE dataset
            column: The column for which to get the data type

        Returns:
            The data type of the specified column
        """
        # list of types is used to support pandas and sqlalchemy
        type_ = None
        dataset.set_config_value("interactive_evaluation", True)
        try:

            if (
                dataset.expect_column_values_to_be_in_type_list(
                    column, type_list=sorted(list(ProfilerTypeMapping.INT_TYPE_NAMES))
                ).success
                and dataset.expect_column_values_to_be_in_type_list(
                    column, type_list=sorted(list(ProfilerTypeMapping.FLOAT_TYPE_NAMES))
                ).success
            ):
                type_ = "NUMERIC"

            elif dataset.expect_column_values_to_be_in_type_list(
                column, type_list=sorted(list(ProfilerTypeMapping.INT_TYPE_NAMES))
            ).success:
                type_ = "INT"

            elif dataset.expect_column_values_to_be_in_type_list(
                column, type_list=sorted(list(ProfilerTypeMapping.FLOAT_TYPE_NAMES))
            ).success:
                type_ = "FLOAT"

            elif dataset.expect_column_values_to_be_in_type_list(
                column, type_list=sorted(list(ProfilerTypeMapping.STRING_TYPE_NAMES))
            ).success:
                type_ = "STRING"

            elif dataset.expect_column_values_to_be_in_type_list(
                column, type_list=sorted(list(ProfilerTypeMapping.BOOLEAN_TYPE_NAMES))
            ).success:
                type_ = "BOOLEAN"

            elif dataset.expect_column_values_to_be_in_type_list(
                column, type_list=sorted(list(ProfilerTypeMapping.DATETIME_TYPE_NAMES))
            ).success:
                type_ = "DATETIME"

            else:
                dataset.expect_column_values_to_be_in_type_list(column, type_list=None)
                type_ = "UNKNOWN"
        except NotImplementedError:
            type_ = "unknown"

        if type_ == "numeric":
            dataset.expect_column_values_to_be_in_type_list(
                column,
                type_list=sorted(list(ProfilerTypeMapping.INT_TYPE_NAMES))
                + sorted(list(ProfilerTypeMapping.FLOAT_TYPE_NAMES)),
            )

        dataset.set_config_value("interactive_evaluation", False)
        dataset.remove_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_type_list",
                kwargs={"column": column},
            )
        )
        return type_

    def _add_column_cardinality_to_column_info(self, dataset, column_name):
        """
        Adds the cardinality of a column to the column_info dictionary on self
        Args:
            dataset: A GE Dataset
            column_name: The name of the column for which to add cardinality

        Returns:
            The cardinality of the column
        """
        column_info_entry = self.column_info.get(column_name)
        if not column_info_entry:
            column_info_entry = {}
            self.column_info[column_name] = column_info_entry
        column_cardinality = column_info_entry.get("cardinality")
        if not column_cardinality:
            column_cardinality = self._get_column_cardinality(dataset, column_name)
            column_info_entry["cardinality"] = column_cardinality
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

    def _get_column_cardinality(self, dataset, column):
        """
        Determines the cardinality of a column using the get_basic_column_cardinality method from
        OrderedProfilerCardinality
        Args:
            dataset: A GE Dataset
            column: The column for which to get cardinality

        Returns:
            The cardinality of the specified column
        """
        num_unique = None
        pct_unique = None
        dataset.set_config_value("interactive_evaluation", True)

        try:
            num_unique = dataset.expect_column_unique_value_count_to_be_between(
                column, None, None
            ).result["observed_value"]
            pct_unique = (
                dataset.expect_column_proportion_of_unique_values_to_be_between(
                    column, None, None
                ).result["observed_value"]
            )
        except KeyError:  # if observed_value value is not set
            logger.error(
                "Failed to get cardinality of column {:s} - continuing...".format(
                    column
                )
            )
        # Previously, if we had 25 possible categories out of 1000 rows, this would comes up as many, because of its
        #  percentage, so it was tweaked here, but is still experimental.
        cardinality = OrderedProfilerCardinality.get_basic_column_cardinality(
            num_unique, pct_unique
        )

        dataset.set_config_value("interactive_evaluation", False)

        return cardinality

    def _add_semantic_types_by_column_from_config_to_column_info(self, column_name):
        """
        Adds the semantic type of a column to the column_info dict on self, for display purposes after suite creation
        Args:
            column_name: The name of the column

        Returns:
            A list of semantic_types for a given colum
        """
        column_info_entry = self.column_info.get(column_name)
        if not column_info_entry:
            column_info_entry = {}
            self.column_info[column_name] = column_info_entry

        semantic_types = column_info_entry.get("semantic_types")

        if not semantic_types:
            assert isinstance(
                self.semantic_type_dict, dict
            ), f"The semantic_types dict in the config must be a dictionary, but is currently a {type(self.semantic_type_dict)}. Please reformat."
            semantic_types = []
            for semantic_type, column_list in self.semantic_type_dict.items():
                if (
                    column_name in column_list
                    and semantic_type.upper() in ProfilerSemanticTypes.__members__
                ):
                    semantic_types.append(semantic_type)
            column_info_entry["semantic_types"] = semantic_types

        return semantic_types

    def _build_column_description_metadata(self, dataset):
        """
        Adds column description metadata to the suite on a Dataset object
        Args:
            dataset: A GE Dataset

        Returns:
            An expectation suite with column description metadata
        """
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

    def _display_suite_by_column(self, suite):
        """
        Displays the expectations of a suite by column, along with the column cardinality, and semantic or data type so
        that a user can easily see which expectations were created for which columns
        Args:
            suite: An ExpectationSuite

        Returns:
            The ExpectationSuite
        """
        expectations = suite.expectations
        expectations_by_column = {}
        for expectation in expectations:
            domain = expectation["kwargs"].get("column") or "table_level_expectations"
            if expectations_by_column.get(domain) is None:
                expectations_by_column[domain] = [expectation]
            else:
                expectations_by_column[domain].append(expectation)

        if not expectations_by_column:
            print("No expectations included in suite.")
        else:
            print("Creating an expectation suite with the following expectations:\n")

        if "table_level_expectations" in expectations_by_column:
            table_level_expectations = expectations_by_column.pop(
                "table_level_expectations"
            )
            print("Table-Level Expectations")
            for expectation in sorted(
                table_level_expectations, key=lambda x: x.expectation_type
            ):
                print(expectation.expectation_type)

        if expectations_by_column:
            print("\nExpectations by Column")

        for column in sorted(expectations_by_column):
            info_column = self.column_info.get(column) or {}

            semantic_types = info_column.get("semantic_types")
            type_ = info_column.get("type")
            cardinality = info_column.get("cardinality")

            if semantic_types:
                type_string = f" | Semantic Type: {semantic_types[0] if len(semantic_types)==1 else semantic_types}"
            elif type_:
                type_string = f" | Column Data Type: {type_}"
            else:
                type_string = ""

            if cardinality:
                cardinality_string = f" | Cardinality: {cardinality}"
            else:
                cardinality_string = ""

            column_string = (
                f"Column Name: {column}{type_string or ''}{cardinality_string or ''}"
            )
            print(column_string)

            for expectation in sorted(
                expectations_by_column.get(column), key=lambda x: x.expectation_type
            ):
                print(expectation.expectation_type)
            print("\n")

            return suite

    def _build_expectations_value_set(self, dataset, column, **kwargs):
        """
        Adds a value_set expectation for a given column
        Args:
            dataset: A GE Dataset
            column: The column for which to add an expectation
            **kwargs:

        Returns:
            The GE Dataset
        """
        if "expect_column_values_to_be_in_set" not in self.excluded_expectations:
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
        return dataset

    def _build_expectations_numeric(self, dataset, column, **kwargs):
        """
        Adds a set of numeric expectations for a given column
        Args:
            dataset: A GE Dataset
            column: The column for which to add expectations
            **kwargs:

        Returns:
            The GE Dataset
        """
        tolerance = kwargs.get("tolerance") or 0

        # min
        if "expect_column_min_to_be_between" not in self.excluded_expectations:
            observed_min = dataset.expect_column_min_to_be_between(
                column, min_value=None, max_value=None, result_format="SUMMARY"
            ).result["observed_value"]
            if not self._is_nan(observed_min):
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
        if "expect_column_max_to_be_between" not in self.excluded_expectations:
            observed_max = dataset.expect_column_max_to_be_between(
                column, min_value=None, max_value=None, result_format="SUMMARY"
            ).result["observed_value"]
            if not self._is_nan(observed_max):
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
        if "expect_column_mean_to_be_between" not in self.excluded_expectations:
            observed_mean = dataset.expect_column_mean_to_be_between(
                column, min_value=None, max_value=None, result_format="SUMMARY"
            ).result["observed_value"]
            if not self._is_nan(observed_mean):
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
        if "expect_column_median_to_be_between" not in self.excluded_expectations:
            observed_median = dataset.expect_column_median_to_be_between(
                column, min_value=None, max_value=None, result_format="SUMMARY"
            ).result["observed_value"]
            if not self._is_nan(observed_median):
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
        if (
            "expect_column_quantile_values_to_be_between"
            not in self.excluded_expectations
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
        return dataset

    def _build_expectations_primary_or_compound_key(
        self, dataset, column_list, **kwargs
    ):
        """
        Adds a uniqueness expectation for a given column or set of columns
        Args:
            dataset: A GE Dataset
            column_list: A list containing one or more columns for which to add a uniqueness expectation
            **kwargs:

        Returns:
            The GE Dataset
        """
        # uniqueness
        if (
            len(column_list) > 1
            and "expect_compound_columns_to_be_unique" not in self.excluded_expectations
        ):
            dataset.expect_compound_columns_to_be_unique(column_list)
        elif len(column_list) < 1:
            raise ValueError(
                "When specifying a primary or compound key, column_list must not be empty"
            )
        else:
            [column] = column_list
            if "expect_column_values_to_be_unique" not in self.excluded_expectations:
                dataset.expect_column_values_to_be_unique(column)
        return dataset

    def _build_expectations_string(self, dataset, column, **kwargs):
        """
        Adds a set of string expectations for a given column. Currently does not do anything.
        With the 0.12 API there isn't a quick way to introspect for value_lengths - if we did that, we could build a
        potentially useful value_lengths expectation here.
        Args:
            dataset: A GE Dataset
            column: The column for which to add expectations
            **kwargs:

        Returns:
            The GE Dataset
        """

        if (
            "expect_column_value_lengths_to_be_between"
            not in self.excluded_expectations
        ):

            pass
        return dataset

    def _build_expectations_datetime(self, dataset, column, **kwargs):
        """
        Adds `expect_column_values_to_be_between` for a given column
        Args:
            dataset: A GE Dataset
            column: The column for which to add the expectation
            **kwargs:

        Returns:
            The GE Dataset
        """
        tolerance = kwargs.get("tolerance") or 0

        if "expect_column_values_to_be_between" not in self.excluded_expectations:
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
        return dataset

    def _build_expectations_for_all_column_types(self, dataset, column, **kwargs):
        """
        Adds these expectations for all included columns irrespective of type. Includes:
            - `expect_column_values_to_not_be_null` (or `expect_column_values_to_be_null`)
            - `expect_column_proportion_of_unique_values_to_be_between`
            - `expect_column_values_to_be_in_type_list`
        Args:
            dataset: A GE Dataset
            column: The column for which to add the expectations
            **kwargs:

        Returns:
            The GE Dataset
        """
        tolerance = kwargs.get("tolerance") or 0
        if "expect_column_values_to_not_be_null" not in self.excluded_expectations:
            not_null_result = dataset.expect_column_values_to_not_be_null(column)
            if not not_null_result.success:
                unexpected_percent = float(not_null_result.result["unexpected_percent"])
                if unexpected_percent >= 50 and not self.not_null_only:
                    potential_mostly_value = (unexpected_percent + tolerance) / 100.0
                    safe_mostly_value = round(potential_mostly_value, 3)
                    dataset.remove_expectation(
                        ExpectationConfiguration(
                            expectation_type="expect_column_values_to_not_be_null",
                            kwargs={"column": column},
                        ),
                        match_type="domain",
                    )
                    if (
                        "expect_column_values_to_be_null"
                        not in self.excluded_expectations
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
        if (
            "expect_column_proportion_of_unique_values_to_be_between"
            not in self.excluded_expectations
        ):
            pct_unique = (
                dataset.expect_column_proportion_of_unique_values_to_be_between(
                    column, None, None
                ).result["observed_value"]
            )

            if not self._is_nan(pct_unique):
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

        if "expect_column_values_to_be_in_type_list" not in self.excluded_expectations:
            col_type = self.column_info.get(column).get("type")
            type_list = ProfilerDataTypesWithMapping[col_type.upper()].value
            dataset.expect_column_values_to_be_in_type_list(column, type_list=type_list)

    def _build_expectations_table(self, dataset, **kwargs):
        """
        Adds two table level expectations to the dataset
        Args:
            dataset: A GE Dataset
            **kwargs:

        Returns:
            The GE Dataset
        """
        tolerance = kwargs.get("tolerance") or 0

        if (
            "expect_table_columns_to_match_ordered_list"
            not in self.excluded_expectations
        ):
            columns = dataset.get_table_columns()
            dataset.expect_table_columns_to_match_ordered_list(columns)

        if "expect_table_row_count_to_be_between" not in self.excluded_expectations:
            row_count = dataset.expect_table_row_count_to_be_between(
                min_value=0, max_value=None
            ).result["observed_value"]
            min_value = max(0, int(row_count * (1 - tolerance)))
            max_value = int(row_count * (1 + tolerance))

            dataset.expect_table_row_count_to_be_between(
                min_value=min_value, max_value=max_value
            )

    def _is_nan(self, value):
        """
        Test element-wise for NaN and return result as a boolean array.
        Args:
            value: The value to test

        Returns:
            The results of the test
        """
        try:
            return np.isnan(value)
        except TypeError:
            return False
