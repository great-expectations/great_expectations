import datetime
import json
import logging
import warnings
from collections import namedtuple
from copy import deepcopy
from typing import Any, List, Union

import jsonpatch
from dateutil.parser import parse
from IPython import get_ipython

from great_expectations import __version__ as ge_version
from great_expectations.core.data_context_key import DataContextKey
from great_expectations.core.evaluation_parameters import (
    find_evaluation_parameter_dependencies,
)
from great_expectations.core.urn import ge_urn
from great_expectations.core.util import nested_update
from great_expectations.exceptions import (
    DataContextError,
    InvalidCacheValueError,
    InvalidExpectationConfigurationError,
    InvalidExpectationKwargsError,
    ParserError,
    UnavailableMetricError,
)
from great_expectations.marshmallow__shade import (
    Schema,
    ValidationError,
    fields,
    post_load,
    pre_dump,
)
from great_expectations.types import DictDot

logger = logging.getLogger(__name__)

RESULT_FORMATS = ["BOOLEAN_ONLY", "BASIC", "COMPLETE", "SUMMARY"]

EvaluationParameterIdentifier = namedtuple(
    "EvaluationParameterIdentifier",
    ["expectation_suite_name", "metric_name", "metric_kwargs_id"],
)


# function to determine if code is being run from a Jupyter notebook
def in_jupyter_notebook():
    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


def get_metric_kwargs_id(metric_name, metric_kwargs):
    ###
    #
    # WARNING
    # WARNING
    # THIS IS A PLACEHOLDER UNTIL WE HAVE REFACTORED EXPECTATIONS TO HANDLE THIS LOGIC THEMSELVES
    # WE ARE NO WORSE OFF THAN THE PREVIOUS SYSTEM, BUT NOT FULLY CUSTOMIZABLE
    # WARNING
    # WARNING
    #
    ###
    if "metric_kwargs_id" in metric_kwargs:
        return metric_kwargs["metric_kwargs_id"]
    if "column" in metric_kwargs:
        return "column=" + metric_kwargs.get("column")
    return None


def convert_to_json_serializable(data):
    """
    Helper function to convert an object to one that is json serializable

    Args:
        data: an object to attempt to convert a corresponding json-serializable object

    Returns:
        (dict) A converted test_object

    Warning:
        test_obj may also be converted in place.

    """
    import datetime
    import decimal
    import sys

    import numpy as np
    import pandas as pd

    # If it's one of our types, we use our own conversion; this can move to full schema
    # once nesting goes all the way down
    if isinstance(
        data,
        (
            ExpectationConfiguration,
            ExpectationSuite,
            ExpectationValidationResult,
            ExpectationSuiteValidationResult,
            RunIdentifier,
        ),
    ):
        return data.to_json_dict()

    try:
        if not isinstance(data, list) and pd.isna(data):
            # pd.isna is functionally vectorized, but we only want to apply this to single objects
            # Hence, why we test for `not isinstance(list))`
            return None
    except TypeError:
        pass
    except ValueError:
        pass

    if isinstance(data, (str, int, float, bool)):
        # No problem to encode json
        return data

    elif isinstance(data, dict):
        new_dict = {}
        for key in data:
            # A pandas index can be numeric, and a dict key can be numeric, but a json key must be a string
            new_dict[str(key)] = convert_to_json_serializable(data[key])

        return new_dict

    elif isinstance(data, (list, tuple, set)):
        new_list = []
        for val in data:
            new_list.append(convert_to_json_serializable(val))

        return new_list

    elif isinstance(data, (np.ndarray, pd.Index)):
        # test_obj[key] = test_obj[key].tolist()
        # If we have an array or index, convert it first to a list--causing coercion to float--and then round
        # to the number of digits for which the string representation will equal the float representation
        return [convert_to_json_serializable(x) for x in data.tolist()]

    # Note: This clause has to come after checking for np.ndarray or we get:
    #      `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`
    elif data is None:
        # No problem to encode json
        return data

    elif isinstance(data, (datetime.datetime, datetime.date)):
        return data.isoformat()

    # Use built in base type from numpy, https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    # https://github.com/numpy/numpy/pull/9505
    elif np.issubdtype(type(data), np.bool_):
        return bool(data)

    elif np.issubdtype(type(data), np.integer) or np.issubdtype(type(data), np.uint):
        return int(data)

    elif np.issubdtype(type(data), np.floating):
        # Note: Use np.floating to avoid FutureWarning from numpy
        return float(round(data, sys.float_info.dig))

    elif isinstance(data, pd.Series):
        # Converting a series is tricky since the index may not be a string, but all json
        # keys must be strings. So, we use a very ugly serialization strategy
        index_name = data.index.name or "index"
        value_name = data.name or "value"
        return [
            {
                index_name: convert_to_json_serializable(idx),
                value_name: convert_to_json_serializable(val),
            }
            for idx, val in data.iteritems()
        ]

    elif isinstance(data, pd.DataFrame):
        return convert_to_json_serializable(data.to_dict(orient="records"))

    elif isinstance(data, decimal.Decimal):
        if not (-1e-55 < decimal.Decimal.from_float(float(data)) - data < 1e-55):
            logger.warning(
                "Using lossy conversion for decimal %s to float object to support serialization."
                % str(data)
            )
        return float(data)

    else:
        raise TypeError(
            "%s is of type %s which cannot be serialized."
            % (str(data), type(data).__name__)
        )


def ensure_json_serializable(data):
    """
    Helper function to convert an object to one that is json serializable

    Args:
        data: an object to attempt to convert a corresponding json-serializable object

    Returns:
        (dict) A converted test_object

    Warning:
        test_obj may also be converted in place.

    """
    import datetime
    import decimal

    import numpy as np
    import pandas as pd

    # If it's one of our types, we use our own conversion; this can move to full schema
    # once nesting goes all the way down
    if isinstance(
        data,
        (
            ExpectationConfiguration,
            ExpectationSuite,
            ExpectationValidationResult,
            ExpectationSuiteValidationResult,
            RunIdentifier,
        ),
    ):
        return

    try:
        if not isinstance(data, list) and pd.isna(data):
            # pd.isna is functionally vectorized, but we only want to apply this to single objects
            # Hence, why we test for `not isinstance(list))`
            return
    except TypeError:
        pass
    except ValueError:
        pass

    if isinstance(data, (str, int, float, bool)):
        # No problem to encode json
        return

    elif isinstance(data, dict):
        for key in data:
            str(key)  # key must be cast-able to string
            ensure_json_serializable(data[key])

        return

    elif isinstance(data, (list, tuple, set)):
        for val in data:
            ensure_json_serializable(val)
        return

    elif isinstance(data, (np.ndarray, pd.Index)):
        # test_obj[key] = test_obj[key].tolist()
        # If we have an array or index, convert it first to a list--causing coercion to float--and then round
        # to the number of digits for which the string representation will equal the float representation
        _ = [ensure_json_serializable(x) for x in data.tolist()]
        return

    # Note: This clause has to come after checking for np.ndarray or we get:
    #      `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`
    elif data is None:
        # No problem to encode json
        return

    elif isinstance(data, (datetime.datetime, datetime.date)):
        return

    # Use built in base type from numpy, https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    # https://github.com/numpy/numpy/pull/9505
    elif np.issubdtype(type(data), np.bool_):
        return

    elif np.issubdtype(type(data), np.integer) or np.issubdtype(type(data), np.uint):
        return

    elif np.issubdtype(type(data), np.floating):
        # Note: Use np.floating to avoid FutureWarning from numpy
        return

    elif isinstance(data, pd.Series):
        # Converting a series is tricky since the index may not be a string, but all json
        # keys must be strings. So, we use a very ugly serialization strategy
        index_name = data.index.name or "index"
        value_name = data.name or "value"
        _ = [
            {
                index_name: ensure_json_serializable(idx),
                value_name: ensure_json_serializable(val),
            }
            for idx, val in data.iteritems()
        ]
        return
    elif isinstance(data, pd.DataFrame):
        return ensure_json_serializable(data.to_dict(orient="records"))

    elif isinstance(data, decimal.Decimal):
        return

    else:
        raise InvalidExpectationConfigurationError(
            "%s is of type %s which cannot be serialized to json"
            % (str(data), type(data).__name__)
        )


class RunIdentifier(DataContextKey):
    """A RunIdentifier identifies a run (collection of validations) by run_name and run_time."""

    def __init__(self, run_name=None, run_time=None):
        super().__init__()
        assert run_name is None or isinstance(
            run_name, str
        ), "run_name must be an instance of str"
        assert run_time is None or isinstance(run_time, (datetime.datetime, str)), (
            "run_time must be either None or " "an instance of str or datetime"
        )
        self._run_name = run_name

        if isinstance(run_time, str):
            try:
                run_time = parse(run_time)
            except (ValueError, TypeError):
                warnings.warn(
                    f'Unable to parse provided run_time str ("{run_time}") to datetime. Defaulting '
                    f"run_time to current time."
                )
                run_time = datetime.datetime.now(datetime.timezone.utc)

        if not run_time:
            try:
                run_time = parse(run_name)
            except (ValueError, TypeError):
                run_time = None

        run_time = run_time or datetime.datetime.now(datetime.timezone.utc)
        if not run_time.tzinfo:
            # this takes the given time and just adds timezone (no conversion)
            run_time = run_time.replace(tzinfo=datetime.timezone.utc)
        else:
            # this takes given time and converts to utc
            run_time = run_time.astimezone(tz=datetime.timezone.utc)
        self._run_time = run_time

    @property
    def run_name(self):
        return self._run_name

    @property
    def run_time(self):
        return self._run_time

    def to_tuple(self):
        return (
            self._run_name or "__none__",
            self._run_time.strftime("%Y%m%dT%H%M%S.%fZ"),
        )

    def to_fixed_length_tuple(self):
        return (
            self._run_name or "__none__",
            self._run_time.strftime("%Y%m%dT%H%M%S.%fZ"),
        )

    def __repr__(self):
        return json.dumps(self.to_json_dict())

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self):
        myself = runIdentifierSchema.dump(self)
        return myself

    @classmethod
    def from_tuple(cls, tuple_):
        return cls(tuple_[0], tuple_[1])

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        return cls(tuple_[0], tuple_[1])


class RunIdentifierSchema(Schema):
    run_name = fields.Str()
    run_time = fields.DateTime(format="iso")

    @post_load
    def make_run_identifier(self, data, **kwargs):
        return RunIdentifier(**data)


def _deduplicate_evaluation_parameter_dependencies(dependencies):
    deduplicated = dict()
    for suite_name, required_metrics in dependencies.items():
        deduplicated[suite_name] = []
        metrics = set()
        metric_kwargs = dict()
        for metric in required_metrics:
            if isinstance(metric, str):
                metrics.add(metric)
            elif isinstance(metric, dict):
                # There is a single metric_kwargs_id object in this construction
                for kwargs_id, metric_list in metric["metric_kwargs_id"].items():
                    if kwargs_id not in metric_kwargs:
                        metric_kwargs[kwargs_id] = set()
                    for metric_name in metric_list:
                        metric_kwargs[kwargs_id].add(metric_name)
        deduplicated[suite_name] = list(metrics)
        if len(metric_kwargs) > 0:
            deduplicated[suite_name] = deduplicated[suite_name] + [
                {
                    "metric_kwargs_id": {
                        metric_kwargs: list(metrics_set)
                        for (metric_kwargs, metrics_set) in metric_kwargs.items()
                    }
                }
            ]

    return deduplicated


class ExpectationConfiguration(DictDot):
    """ExpectationConfiguration defines the parameters and name of a specific expectation."""

    kwarg_lookup_dict = {
        "expect_column_to_exist": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["column_index"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "column_index": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_table_columns_to_match_ordered_list": {
            "domain_kwargs": [],
            "success_kwargs": ["column_list"],
            "default_kwarg_values": {
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_table_columns_to_match_set": {
            "domain_kwargs": [],
            "success_kwargs": ["column_set", "exact_match"],
            "default_kwarg_values": {
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_table_column_count_to_be_between": {
            "domain_kwargs": [],
            "success_kwargs": ["min_value", "max_value"],
            "default_kwarg_values": {
                "min_value": None,
                "max_value": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_table_column_count_to_equal": {
            "domain_kwargs": [],
            "success_kwargs": ["value"],
            "default_kwarg_values": {
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_table_row_count_to_be_between": {
            "domain_kwargs": [],
            "success_kwargs": ["min_value", "max_value"],
            "default_kwarg_values": {
                "min_value": None,
                "max_value": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_table_row_count_to_equal": {
            "domain_kwargs": [],
            "success_kwargs": ["value"],
            "default_kwarg_values": {
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_be_unique": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_not_be_null": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_be_null": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_be_of_type": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["type_", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_be_in_type_list": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["type_list", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_be_in_set": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["value_set", "mostly", "parse_strings_as_datetimes"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "parse_strings_as_datetimes": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_not_be_in_set": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["value_set", "mostly", "parse_strings_as_datetimes"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "parse_strings_as_datetimes": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_be_between": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": [
                "min_value",
                "max_value",
                "strict_min",
                "strict_max",
                "allow_cross_type_comparisons",
                "parse_strings_as_datetimes",
                "output_strftime_format",
                "mostly",
            ],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "min_value": None,
                "max_value": None,
                "strict_min": False,
                "strict_max": False,
                "allow_cross_type_comparisons": None,
                "parse_strings_as_datetimes": None,
                "output_strftime_format": None,
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_be_increasing": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["strictly", "parse_strings_as_datetimes", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "strictly": None,
                "parse_strings_as_datetimes": None,
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_be_decreasing": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["strictly", "parse_strings_as_datetimes", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "strictly": None,
                "parse_strings_as_datetimes": None,
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_value_lengths_to_be_between": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["min_value", "max_value", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "min_value": None,
                "max_value": None,
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_value_lengths_to_equal": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["value", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_match_regex": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["regex", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_not_match_regex": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["regex", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_match_regex_list": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["regex_list", "match_on", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "match_on": "any",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_not_match_regex_list": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["regex_list", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_match_strftime_format": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["strftime_format", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_be_dateutil_parseable": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_be_json_parseable": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_values_to_match_json_schema": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["json_schema", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["distribution", "p_value", "params"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "p_value": 0.05,
                "params": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_distinct_values_to_be_in_set": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["value_set", "parse_strings_as_datetimes"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "parse_strings_as_datetimes": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_distinct_values_to_equal_set": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["value_set", "parse_strings_as_datetimes"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "parse_strings_as_datetimes": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_distinct_values_to_contain_set": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["value_set", "parse_strings_as_datetimes"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "parse_strings_as_datetimes": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_mean_to_be_between": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["min_value", "max_value", "strict_min", "strict_max"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "min_value": None,
                "max_value": None,
                "strict_min": False,
                "strict_max": False,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_median_to_be_between": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["min_value", "max_value", "strict_min", "strict_max"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "min_value": None,
                "max_value": None,
                "strict_min": False,
                "strict_max": False,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_quantile_values_to_be_between": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["quantile_ranges", "allow_relative_error"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "allow_relative_error": False,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_stdev_to_be_between": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["min_value", "max_value", "strict_min", "strict_max"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "min_value": None,
                "max_value": None,
                "strict_min": False,
                "strict_max": False,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_unique_value_count_to_be_between": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["min_value", "max_value"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "min_value": None,
                "max_value": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_proportion_of_unique_values_to_be_between": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["min_value", "max_value", "strict_min", "strict_max"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "min_value": None,
                "max_value": None,
                "strict_min": False,
                "strict_max": False,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_most_common_value_to_be_in_set": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["value_set", "ties_okay"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "ties_okay": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_sum_to_be_between": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["min_value", "max_value", "strict_min", "strict_max"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "min_value": None,
                "max_value": None,
                "strict_min": False,
                "strict_max": False,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_min_to_be_between": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": [
                "min_value",
                "max_value",
                "strict_min",
                "strict_max",
                "parse_strings_as_datetimes",
                "output_strftime_format",
            ],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "min_value": None,
                "max_value": None,
                "strict_min": False,
                "strict_max": False,
                "parse_strings_as_datetimes": None,
                "output_strftime_format": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_max_to_be_between": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": [
                "min_value",
                "max_value",
                "strict_min",
                "strict_max",
                "parse_strings_as_datetimes",
                "output_strftime_format",
            ],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "min_value": None,
                "max_value": None,
                "strict_min": False,
                "strict_max": False,
                "parse_strings_as_datetimes": None,
                "output_strftime_format": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_chisquare_test_p_value_to_be_greater_than": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["partition_object", "p", "tail_weight_holdout"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "partition_object": None,
                "p": 0.05,
                "tail_weight_holdout": 0,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_bootstrapped_ks_test_p_value_to_be_greater_than": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": [
                "partition_object",
                "p",
                "bootstrap_samples",
                "bootstrap_sample_size",
            ],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "partition_object": None,
                "p": 0.05,
                "bootstrap_samples": None,
                "bootstrap_sample_size": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_kl_divergence_to_be_less_than": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": [
                "partition_object",
                "threshold",
                "tail_weight_holdout",
                "internal_weight_holdout",
                "bucketize_data",
            ],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "partition_object": None,
                "threshold": None,
                "tail_weight_holdout": 0,
                "internal_weight_holdout": 0,
                "bucketize_data": True,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_pair_values_to_be_equal": {
            "domain_kwargs": [
                "column_A",
                "column_B",
                "row_condition",
                "condition_parser",
            ],
            "success_kwargs": ["ignore_row_if"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "ignore_row_if": "both_values_are_missing",
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_pair_values_A_to_be_greater_than_B": {
            "domain_kwargs": [
                "column_A",
                "column_B",
                "row_condition",
                "condition_parser",
            ],
            "success_kwargs": [
                "or_equal",
                "parse_strings_as_datetimes",
                "allow_cross_type_comparisons",
                "ignore_row_if",
            ],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "or_equal": None,
                "parse_strings_as_datetimes": None,
                "allow_cross_type_comparisons": None,
                "ignore_row_if": "both_values_are_missing",
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_column_pair_values_to_be_in_set": {
            "domain_kwargs": [
                "column_A",
                "column_B",
                "row_condition",
                "condition_parser",
            ],
            "success_kwargs": ["value_pairs_set", "ignore_row_if"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "ignore_row_if": "both_values_are_missing",
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_select_column_values_to_be_unique_within_record": {
            "domain_kwargs": ["column_list", "row_condition", "condition_parser"],
            "success_kwargs": ["ignore_row_if", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "ignore_row_if": "all_values_are_missing",
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "expect_compound_columns_to_be_unique": {
            "domain_kwargs": ["column_list", "row_condition", "condition_parser"],
            "success_kwargs": ["ignore_row_if", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "ignore_row_if": "all_values_are_missing",
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "_expect_column_values_to_be_of_type__aggregate": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["type_", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "_expect_column_values_to_be_of_type__map": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["type_", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "_expect_column_values_to_be_in_type_list__aggregate": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["type_list", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
        "_expect_column_values_to_be_in_type_list__map": {
            "domain_kwargs": ["column", "row_condition", "condition_parser"],
            "success_kwargs": ["type_list", "mostly"],
            "default_kwarg_values": {
                "row_condition": None,
                "condition_parser": "pandas",
                "mostly": None,
                "result_format": "BASIC",
                "include_config": True,
                "catch_exceptions": False,
            },
        },
    }

    runtime_kwargs = ["result_format", "include_config", "catch_exceptions"]

    def __init__(self, expectation_type, kwargs, meta=None, success_on_last_run=None):
        if not isinstance(expectation_type, str):
            raise InvalidExpectationConfigurationError(
                "expectation_type must be a string"
            )
        self._expectation_type = expectation_type
        if not isinstance(kwargs, dict):
            raise InvalidExpectationConfigurationError(
                "expectation configuration kwargs must be a dict."
            )
        self._kwargs = kwargs
        if meta is None:
            meta = {}
        # We require meta information to be serializable, but do not convert until necessary
        ensure_json_serializable(meta)
        self.meta = meta
        self.success_on_last_run = success_on_last_run

    def patch(self, op: str, path: str, value: Any) -> "ExpectationConfiguration":
        """

        Args:
            op: A jsonpatch operation. One of 'add', 'replace', or 'remove'
            path: A jsonpatch path for the patch operation
            value: The value to patch

        Returns:
            The patched ExpectationConfiguration object
        """
        if op not in ["add", "replace", "remove"]:
            raise ValueError("Op must be either 'add', 'replace', or 'remove'")

        try:
            valid_path = path.split("/")[1]
        except IndexError:
            raise IndexError(
                "Ensure you have a valid jsonpatch path of the form '/path/foo' "
                "(see http://jsonpatch.com/)"
            )

        if valid_path not in self.get_runtime_kwargs().keys():
            raise ValueError("Path not available in kwargs (see http://jsonpatch.com/)")

        # TODO: Call validate_kwargs when implemented
        patch = jsonpatch.JsonPatch([{"op": op, "path": path, "value": value}])

        patch.apply(self.kwargs, in_place=True)
        return self

    @property
    def expectation_type(self):
        return self._expectation_type

    @property
    def kwargs(self):
        return self._kwargs

    def _get_default_custom_kwargs(self):
        # NOTE: this is a holdover until class-first expectations control their
        # defaults, and so defaults are inherited.
        if self.expectation_type.startswith("expect_column_pair"):
            return {
                "domain_kwargs": [
                    "column_A",
                    "column_B",
                    "row_condition",
                    "condition_parser",
                ],
                # NOTE: this is almost certainly incomplete; subclasses should override
                "success_kwargs": [],
                "default_kwarg_values": {
                    "column_A": None,
                    "column_B": None,
                    "row_condition": None,
                    "condition_parser": None,
                },
            }
        elif self.expectation_type.startswith("expect_column"):
            return {
                "domain_kwargs": ["column", "row_condition", "condition_parser"],
                # NOTE: this is almost certainly incomplete; subclasses should override
                "success_kwargs": [],
                "default_kwarg_values": {
                    "column": None,
                    "row_condition": None,
                    "condition_parser": None,
                },
            }

        logger.error("Requested kwargs for an unrecognized expectation.")
        return {
            "domain_kwargs": [],
            # NOTE: this is almost certainly incomplete; subclasses should override
            "success_kwargs": [],
            "default_kwarg_values": {},
        }

    def get_domain_kwargs(self):
        expectation_kwargs_dict = self.kwarg_lookup_dict.get(
            self.expectation_type, None
        )
        if expectation_kwargs_dict is None:
            expectation_kwargs_dict = self._get_default_custom_kwargs()
        domain_kwargs = {
            key: self.kwargs.get(
                key, expectation_kwargs_dict.get("default_kwarg_values").get(key)
            )
            for key in expectation_kwargs_dict["domain_kwargs"]
        }
        missing_kwargs = set(expectation_kwargs_dict["domain_kwargs"]) - set(
            domain_kwargs.keys()
        )
        if missing_kwargs:
            raise InvalidExpectationKwargsError(
                f"Missing domain kwargs: {list(missing_kwargs)}"
            )
        return domain_kwargs

    def get_success_kwargs(self):
        expectation_kwargs_dict = self.kwarg_lookup_dict.get(
            self.expectation_type, None
        )
        if expectation_kwargs_dict is None:
            expectation_kwargs_dict = self._get_default_custom_kwargs()
        domain_kwargs = self.get_domain_kwargs()
        success_kwargs = {
            key: self.kwargs.get(
                key, expectation_kwargs_dict.get("default_kwarg_values").get(key)
            )
            for key in expectation_kwargs_dict["success_kwargs"]
        }
        success_kwargs.update(domain_kwargs)
        return success_kwargs

    def get_runtime_kwargs(self):
        expectation_kwargs_dict = self.kwarg_lookup_dict.get(
            self.expectation_type, None
        )
        if expectation_kwargs_dict is None:
            expectation_kwargs_dict = self._get_default_custom_kwargs()
        success_kwargs = self.get_success_kwargs()
        runtime_kwargs = {
            key: self.kwargs.get(
                key, expectation_kwargs_dict.get("default_kwarg_values").get(key)
            )
            for key in self.runtime_kwargs
        }
        runtime_kwargs.update(success_kwargs)
        return runtime_kwargs

    def applies_to_same_domain(self, other_expectation_configuration):
        if (
            not self.expectation_type
            == other_expectation_configuration.expectation_type
        ):
            return False
        return (
            self.get_domain_kwargs()
            == other_expectation_configuration.get_domain_kwargs()
        )

    def isEquivalentTo(self, other, match_type="success"):
        """ExpectationConfiguration equivalence does not include meta, and relies on *equivalence* of kwargs."""
        if not isinstance(other, self.__class__):
            if isinstance(other, dict):
                try:
                    other = expectationConfigurationSchema.load(other)
                except ValidationError:
                    logger.debug(
                        "Unable to evaluate equivalence of ExpectationConfiguration object with dict because "
                        "dict other could not be instantiated as an ExpectationConfiguration"
                    )
                    return NotImplemented
            else:
                # Delegate comparison to the other instance
                return NotImplemented
        if match_type == "domain":
            return all(
                (
                    self.expectation_type == other.expectation_type,
                    self.get_domain_kwargs() == other.get_domain_kwargs(),
                )
            )

        elif match_type == "success":
            return all(
                (
                    self.expectation_type == other.expectation_type,
                    self.get_success_kwargs() == other.get_success_kwargs(),
                )
            )

        elif match_type == "runtime":
            return all(
                (
                    self.expectation_type == other.expectation_type,
                    self.kwargs == other.kwargs,
                )
            )

    def __eq__(self, other):
        """ExpectationConfiguration equality does include meta, but ignores instance identity."""
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return all(
            (
                self.expectation_type == other.expectation_type,
                self.kwargs == other.kwargs,
                self.meta == other.meta,
            )
        )

    def __ne__(self, other):
        # By using the == operator, the returned NotImplemented is handled correctly.
        return not self == other

    def __repr__(self):
        return json.dumps(self.to_json_dict())

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self):
        myself = expectationConfigurationSchema.dump(self)
        # NOTE - JPC - 20191031: migrate to expectation-specific schemas that subclass result with properly-typed
        # schemas to get serialization all-the-way down via dump
        myself["kwargs"] = convert_to_json_serializable(myself["kwargs"])
        return myself

    def get_evaluation_parameter_dependencies(self):
        parsed_dependencies = dict()
        for key, value in self.kwargs.items():
            if isinstance(value, dict) and "$PARAMETER" in value:
                param_string_dependencies = find_evaluation_parameter_dependencies(
                    value["$PARAMETER"]
                )
                nested_update(parsed_dependencies, param_string_dependencies)

        dependencies = dict()
        urns = parsed_dependencies.get("urns", [])
        for string_urn in urns:
            try:
                urn = ge_urn.parseString(string_urn)
            except ParserError:
                logger.warning(
                    "Unable to parse great_expectations urn {}".format(
                        value["$PARAMETER"]
                    )
                )
                continue

            if not urn.get("metric_kwargs"):
                nested_update(
                    dependencies, {urn["expectation_suite_name"]: [urn["metric_name"]]},
                )
            else:
                nested_update(
                    dependencies,
                    {
                        urn["expectation_suite_name"]: [
                            {
                                "metric_kwargs_id": {
                                    urn["metric_kwargs"]: [urn["metric_name"]]
                                }
                            }
                        ]
                    },
                )

        dependencies = _deduplicate_evaluation_parameter_dependencies(dependencies)
        return dependencies


class ExpectationConfigurationSchema(Schema):
    expectation_type = fields.Str(
        required=True,
        error_messages={
            "required": "expectation_type missing in expectation configuration"
        },
    )
    kwargs = fields.Dict()
    meta = fields.Dict()

    # noinspection PyUnusedLocal
    @post_load
    def make_expectation_configuration(self, data, **kwargs):
        return ExpectationConfiguration(**data)


# TODO: re-enable once we can allow arbitrary keys but still add this sort of validation
# class MetaDictSchema(Schema):
#     """The MetaDict """
#
#     # noinspection PyUnusedLocal
#     @validates_schema
#     def validate_json_serializable(self, data, **kwargs):
#         import json
#         try:
#             json.dumps(data)
#         except (TypeError, OverflowError):
#             raise ValidationError("meta information must be json serializable.")


class ExpectationSuite:
    """
    This ExpectationSuite object has create, read, update, and delete functionality for its expectations:
        -create: self.add_expectation()
        -read: self.find_expectation_indexes()
        -update: self.add_expectation() or self.patch_expectation()
        -delete: self.remove_expectation()
    """

    def __init__(
        self,
        expectation_suite_name,
        expectations=None,
        evaluation_parameters=None,
        data_asset_type=None,
        meta=None,
    ):
        self.expectation_suite_name = expectation_suite_name
        if expectations is None:
            expectations = []
        self.expectations = [
            ExpectationConfiguration(**expectation)
            if isinstance(expectation, dict)
            else expectation
            for expectation in expectations
        ]
        if evaluation_parameters is None:
            evaluation_parameters = {}
        self.evaluation_parameters = evaluation_parameters
        self.data_asset_type = data_asset_type
        if meta is None:
            meta = {"great_expectations_version": ge_version}
        if (
            "great_expectations.__version__" not in meta.keys()
            and "great_expectations_version" not in meta.keys()
        ):
            meta["great_expectations_version"] = ge_version
        # We require meta information to be serializable, but do not convert until necessary
        ensure_json_serializable(meta)
        self.meta = meta

    def add_citation(
        self,
        comment,
        batch_kwargs=None,
        batch_markers=None,
        batch_parameters=None,
        citation_date=None,
    ):
        if "citations" not in self.meta:
            self.meta["citations"] = []
        self.meta["citations"].append(
            {
                "citation_date": citation_date
                or datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y%m%dT%H%M%S.%fZ"
                ),
                "batch_kwargs": batch_kwargs,
                "batch_markers": batch_markers,
                "batch_parameters": batch_parameters,
                "comment": comment,
            }
        )

    def isEquivalentTo(self, other):
        """
        ExpectationSuite equivalence relies only on expectations and evaluation parameters. It does not include:
        - data_asset_name
        - expectation_suite_name
        - meta
        - data_asset_type
        """
        if not isinstance(other, self.__class__):
            if isinstance(other, dict):
                try:
                    other = expectationSuiteSchema.load(other)
                except ValidationError:
                    logger.debug(
                        "Unable to evaluate equivalence of ExpectationConfiguration object with dict because "
                        "dict other could not be instantiated as an ExpectationConfiguration"
                    )
                    return NotImplemented
            else:
                # Delegate comparison to the other instance
                return NotImplemented

        return len(self.expectations) == len(other.expectations) and all(
            [
                mine.isEquivalentTo(theirs)
                for (mine, theirs) in zip(self.expectations, other.expectations)
            ]
        )

    def __eq__(self, other):
        """ExpectationSuite equality ignores instance identity, relying only on properties."""
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return all(
            (
                self.expectation_suite_name == other.expectation_suite_name,
                self.expectations == other.expectations,
                self.evaluation_parameters == other.evaluation_parameters,
                self.data_asset_type == other.data_asset_type,
                self.meta == other.meta,
            )
        )

    def __ne__(self, other):
        # By using the == operator, the returned NotImplemented is handled correctly.
        return not self == other

    def __repr__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self):
        myself = expectationSuiteSchema.dump(self)
        # NOTE - JPC - 20191031: migrate to expectation-specific schemas that subclass result with properly-typed
        # schemas to get serialization all-the-way down via dump
        myself["expectations"] = convert_to_json_serializable(myself["expectations"])
        try:
            myself["evaluation_parameters"] = convert_to_json_serializable(
                myself["evaluation_parameters"]
            )
        except KeyError:
            pass  # Allow evaluation parameters to be missing if empty
        myself["meta"] = convert_to_json_serializable(myself["meta"])
        return myself

    def get_evaluation_parameter_dependencies(self):
        dependencies = {}
        for expectation in self.expectations:
            t = expectation.get_evaluation_parameter_dependencies()
            nested_update(dependencies, t)

        dependencies = _deduplicate_evaluation_parameter_dependencies(dependencies)
        return dependencies

    def get_citations(self, sort=True, require_batch_kwargs=False):
        citations = self.meta.get("citations", [])
        if require_batch_kwargs:
            citations = self._filter_citations(citations, "batch_kwargs")
        if not sort:
            return citations
        return self._sort_citations(citations)

    def get_table_expectations(self):
        """Return a list of table expectations."""
        return [
            e
            for e in self.expectations
            if e.expectation_type.startswith("expect_table_")
        ]

    def get_column_expectations(self):
        """Return a list of column map expectations."""
        return [e for e in self.expectations if "column" in e.kwargs]

    @staticmethod
    def _filter_citations(citations, filter_key):
        citations_with_bk = []
        for citation in citations:
            if filter_key in citation and citation.get(filter_key):
                citations_with_bk.append(citation)
        return citations_with_bk

    @staticmethod
    def _sort_citations(citations):
        return sorted(citations, key=lambda x: x["citation_date"])

    # CRUD methods #

    def append_expectation(self, expectation_config):
        """Appends an expectation.

           Args:
               expectation_config (ExpectationConfiguration): \
                   The expectation to be added to the list.

           Notes:
               May want to add type-checking in the future.
        """
        self.expectations.append(expectation_config)

    def remove_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        match_type: str = "domain",
        remove_multiple_matches: bool = False,
    ) -> List[ExpectationConfiguration]:
        """

        Args:
            expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against for
                for the removal of expectations.
            match_type: This determines what kwargs to use when matching. Options are 'domain' to match based
                on the data evaluated by that expectation, 'success' to match based on all configuration parameters
                 that influence whether an expectation succeeds based on a given batch of data, and 'runtime' to match
                 based on all configuration parameters
            remove_multiple_matches: If True, will remove multiple matching expectations. If False, will raise a ValueError.
        Returns: The list of deleted ExpectationConfigurations

        Raises:
            No match
            More than 1 match, if remove_multiple_matches = False
        """
        found_expectation_indexes = self.find_expectation_indexes(
            expectation_configuration, match_type
        )
        if len(found_expectation_indexes) < 1:
            raise ValueError("No matching expectation was found.")

        elif len(found_expectation_indexes) > 1:
            if remove_multiple_matches:
                removed_expectations = []
                for index in sorted(found_expectation_indexes, reverse=True):
                    removed_expectations.append(self.expectations.pop(index))
                return removed_expectations
            else:
                raise ValueError(
                    "More than one matching expectation was found. Specify more precise matching criteria,"
                    "or set remove_multiple_matches=True"
                )

        else:
            return [self.expectations.pop(found_expectation_indexes[0])]

    def remove_all_expectations_of_type(
        self, expectation_types: Union[List[str], str]
    ) -> List[ExpectationConfiguration]:
        if isinstance(expectation_types, str):
            expectation_types = [expectation_types]
        removed_expectations = [
            expectation
            for expectation in self.expectations
            if expectation.expectation_type in expectation_types
        ]
        self.expectations = [
            expectation
            for expectation in self.expectations
            if expectation.expectation_type not in expectation_types
        ]

        return removed_expectations

    def find_expectation_indexes(
        self,
        expectation_configuration: ExpectationConfiguration,
        match_type: str = "domain",
    ) -> List[int]:
        """

        Args:
            expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against to
                find the index of any matching Expectation Configurations on the suite.
            match_type: This determines what kwargs to use when matching. Options are 'domain' to match based
                on the data evaluated by that expectation, 'success' to match based on all configuration parameters
                 that influence whether an expectation succeeds based on a given batch of data, and 'runtime' to match
                 based on all configuration parameters

        Returns: A list of indexes of matching ExpectationConfiguration

        Raises:
            InvalidExpectationConfigurationError

        """
        if not isinstance(expectation_configuration, ExpectationConfiguration):
            raise InvalidExpectationConfigurationError(
                "Ensure that expectation configuration is valid."
            )
        match_indexes = []
        for idx, expectation in enumerate(self.expectations):
            if expectation.isEquivalentTo(expectation_configuration, match_type):
                match_indexes.append(idx)

        return match_indexes

    def find_expectations(
        self,
        expectation_configuration: ExpectationConfiguration,
        match_type: str = "domain",
    ) -> List[ExpectationConfiguration]:
        found_expectation_indexes = self.find_expectation_indexes(
            expectation_configuration, match_type
        )
        if len(found_expectation_indexes) > 0:
            return [
                expectation
                for idx, expectation in enumerate(self.expectations)
                if idx in found_expectation_indexes
            ]
        else:
            return []

    def patch_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        op: str,
        path: str,
        value: Any,
        match_type: str,
    ) -> ExpectationConfiguration:
        """

       Args:
            expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against to
                find the expectation to patch.
            op: A jsonpatch operation (one of 'add','update', or 'remove') (see http://jsonpatch.com/)
            path: A jsonpatch path for the patch operation (see http://jsonpatch.com/)
            value: The value to patch (see http://jsonpatch.com/)
            match_type: The match type to use for find_expectation_index()

       Returns: The patched ExpectationConfiguration

       Raises:
           No match
           More than 1 match

               """
        found_expectation_indexes = self.find_expectation_indexes(
            expectation_configuration, match_type
        )

        if len(found_expectation_indexes) < 1:
            raise ValueError("No matching expectation was found.")
        elif len(found_expectation_indexes) > 1:
            raise ValueError(
                "More than one matching expectation was found. Please be more specific with your search "
                "criteria"
            )

        self.expectations[found_expectation_indexes[0]].patch(op, path, value)
        return self.expectations[found_expectation_indexes[0]]

    def add_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        match_type: str = "domain",
        overwrite_existing: bool = True,
    ) -> ExpectationConfiguration:
        """

        Args:
            expectation_configuration: The ExpectationConfiguration to add or update
            match_type: The criteria used to determine whether the Suite already has an ExpectationConfiguration
                and so whether we should add or replace.
            overwrite_existing: If the expectation already exists, this will overwrite if True and raise an error if
                False.
        Returns:
            The ExpectationConfiguration to add or replace.
        Raises:
            More than one match
            One match if overwrite_existing = False
        """
        found_expectation_indexes = self.find_expectation_indexes(
            expectation_configuration, match_type
        )

        if len(found_expectation_indexes) > 1:
            raise ValueError(
                "More than one matching expectation was found. Please be more specific with your search "
                "criteria"
            )
        elif len(found_expectation_indexes) == 1:
            # Currently, we completely replace the expectation_configuration, but we could potentially use patch_expectation
            # to update instead. We need to consider how to handle meta in that situation.
            # patch_expectation = jsonpatch.make_patch(self.expectations[found_expectation_index] \
            #   .kwargs, expectation_configuration.kwargs)
            # patch_expectation.apply(self.expectations[found_expectation_index].kwargs, in_place=True)
            if overwrite_existing:
                self.expectations[
                    found_expectation_indexes[0]
                ] = expectation_configuration
            else:
                raise DataContextError(
                    "A matching ExpectationConfiguration already exists. If you would like to overwrite this "
                    "ExpectationConfiguration, set overwrite_existing=True"
                )
        else:
            self.append_expectation(expectation_configuration)

        return expectation_configuration


class ExpectationSuiteSchema(Schema):
    expectation_suite_name = fields.Str()
    expectations = fields.List(fields.Nested(ExpectationConfigurationSchema))
    evaluation_parameters = fields.Dict(allow_none=True)
    data_asset_type = fields.Str(allow_none=True)
    meta = fields.Dict()

    # NOTE: 20191107 - JPC - we may want to remove clean_empty and update tests to require the other fields;
    # doing so could also allow us not to have to make a copy of data in the pre_dump method.
    def clean_empty(self, data):
        if not hasattr(data, "evaluation_parameters"):
            pass
        elif len(data.evaluation_parameters) == 0:
            del data.evaluation_parameters

        if not hasattr(data, "meta"):
            pass
        elif data.meta is None or data.meta == []:
            pass
        elif len(data.meta) == 0:
            del data.meta
        return data

    # noinspection PyUnusedLocal
    @pre_dump
    def prepare_dump(self, data, **kwargs):
        data = deepcopy(data)
        data.meta = convert_to_json_serializable(data.meta)
        data = self.clean_empty(data)
        return data

    # noinspection PyUnusedLocal
    @post_load
    def make_expectation_suite(self, data, **kwargs):
        return ExpectationSuite(**data)


class ExpectationValidationResult:
    def __init__(
        self,
        success=None,
        expectation_config=None,
        result=None,
        meta=None,
        exception_info=None,
    ):
        if result and not self.validate_result_dict(result):
            raise InvalidCacheValueError(result)
        self.success = success
        self.expectation_config = expectation_config
        # TODO: re-add
        # assert_json_serializable(result, "result")
        if result is None:
            result = {}
        self.result = result
        if meta is None:
            meta = {}
        # We require meta information to be serializable, but do not convert until necessary
        ensure_json_serializable(meta)
        self.meta = meta
        self.exception_info = exception_info

    def __eq__(self, other):
        """ExpectationValidationResult equality ignores instance identity, relying only on properties."""
        # NOTE: JPC - 20200213 - need to spend some time thinking about whether we want to
        # consistently allow dict as a comparison alternative in situations like these...
        # if isinstance(other, dict):
        #     try:
        #         other = ExpectationValidationResult(**other)
        #     except ValueError:
        #         return NotImplemented
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        try:
            return all(
                (
                    self.success == other.success,
                    (
                        self.expectation_config is None
                        and other.expectation_config is None
                    )
                    or (
                        self.expectation_config is not None
                        and self.expectation_config.isEquivalentTo(
                            other.expectation_config
                        )
                    ),
                    # Result is a dictionary allowed to have nested dictionaries that are still of complex types (e.g.
                    # numpy) consequently, series' comparison can persist. Wrapping in all() ensures comparision is
                    # handled appropriately.
                    (self.result is None and other.result is None)
                    or (all(self.result) == all(other.result)),
                    self.meta == other.meta,
                    self.exception_info == other.exception_info,
                )
            )
        except (ValueError, TypeError):
            # if invalid comparisons are attempted, the objects are not equal.
            return False

    def __ne__(self, other):
        # Negated implementation of '__eq__'. TODO the method should be deleted when it will coincide with __eq__.
        # return not self == other
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __ne__.
            return NotImplemented
        try:
            return any(
                (
                    self.success != other.success,
                    (
                        self.expectation_config is None
                        and other.expectation_config is not None
                    )
                    or (
                        self.expectation_config is not None
                        and not self.expectation_config.isEquivalentTo(
                            other.expectation_config
                        )
                    ),
                    # TODO should it be wrapped in all()/any()? Since it is the only difference to __eq__:
                    (self.result is None and other.result is not None)
                    or (self.result != other.result),
                    self.meta != other.meta,
                    self.exception_info != other.exception_info,
                )
            )
        except (ValueError, TypeError):
            # if invalid comparisons are attempted, the objects are not equal.
            return True

    def __repr__(self):
        if in_jupyter_notebook():
            json_dict = self.to_json_dict()
            json_dict.pop("expectation_config")
            return json.dumps(json_dict, indent=2)
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def validate_result_dict(self, result):
        if result.get("unexpected_count") and result["unexpected_count"] < 0:
            return False
        if result.get("unexpected_percent") and (
            result["unexpected_percent"] < 0 or result["unexpected_percent"] > 100
        ):
            return False
        if result.get("missing_percent") and (
            result["missing_percent"] < 0 or result["missing_percent"] > 100
        ):
            return False
        if result.get("unexpected_percent_nonmissing") and (
            result["unexpected_percent_nonmissing"] < 0
            or result["unexpected_percent_nonmissing"] > 100
        ):
            return False
        if result.get("missing_count") and result["missing_count"] < 0:
            return False
        return True

    def to_json_dict(self):
        myself = expectationValidationResultSchema.dump(self)
        # NOTE - JPC - 20191031: migrate to expectation-specific schemas that subclass result with properly-typed
        # schemas to get serialization all-the-way down via dump
        if "result" in myself:
            myself["result"] = convert_to_json_serializable(myself["result"])
        if "meta" in myself:
            myself["meta"] = convert_to_json_serializable(myself["meta"])
        if "exception_info" in myself:
            myself["exception_info"] = convert_to_json_serializable(
                myself["exception_info"]
            )
        return myself

    def get_metric(self, metric_name, **kwargs):
        if not self.expectation_config:
            raise UnavailableMetricError(
                "No ExpectationConfig found in this ExpectationValidationResult. Unable to "
                "return a metric."
            )

        metric_name_parts = metric_name.split(".")
        metric_kwargs_id = get_metric_kwargs_id(metric_name, kwargs)

        if metric_name_parts[0] == self.expectation_config.expectation_type:
            curr_metric_kwargs = get_metric_kwargs_id(
                metric_name, self.expectation_config.kwargs
            )
            if metric_kwargs_id != curr_metric_kwargs:
                raise UnavailableMetricError(
                    "Requested metric_kwargs_id (%s) does not match the configuration of this "
                    "ExpectationValidationResult (%s)."
                    % (metric_kwargs_id or "None", curr_metric_kwargs or "None")
                )
            if len(metric_name_parts) < 2:
                raise UnavailableMetricError(
                    "Expectation-defined metrics must include a requested metric."
                )
            elif len(metric_name_parts) == 2:
                if metric_name_parts[1] == "success":
                    return self.success
                else:
                    raise UnavailableMetricError(
                        "Metric name must have more than two parts for keys other than "
                        "success."
                    )
            elif metric_name_parts[1] == "result":
                try:
                    if len(metric_name_parts) == 3:
                        return self.result.get(metric_name_parts[2])
                    elif metric_name_parts[2] == "details":
                        return self.result["details"].get(metric_name_parts[3])
                except KeyError:
                    raise UnavailableMetricError(
                        "Unable to get metric {} -- KeyError in "
                        "ExpectationValidationResult.".format(metric_name)
                    )
        raise UnavailableMetricError("Unrecognized metric name {}".format(metric_name))


class ExpectationValidationResultSchema(Schema):
    success = fields.Bool()
    expectation_config = fields.Nested(ExpectationConfigurationSchema)
    result = fields.Dict()
    meta = fields.Dict()
    exception_info = fields.Dict()

    # noinspection PyUnusedLocal
    @pre_dump
    def convert_result_to_serializable(self, data, **kwargs):
        data = deepcopy(data)
        data.result = convert_to_json_serializable(data.result)
        return data

    # # noinspection PyUnusedLocal
    # @pre_dump
    # def clean_empty(self, data, **kwargs):
    #     # if not hasattr(data, 'meta'):
    #     #     pass
    #     # elif len(data.meta) == 0:
    #     #     del data.meta
    #     # return data
    #     pass

    # noinspection PyUnusedLocal
    @post_load
    def make_expectation_validation_result(self, data, **kwargs):
        return ExpectationValidationResult(**data)


class ExpectationSuiteValidationResult(DictDot):
    def __init__(
        self,
        success=None,
        results=None,
        evaluation_parameters=None,
        statistics=None,
        meta=None,
    ):
        self.success = success
        if results is None:
            results = []
        self.results = results
        if evaluation_parameters is None:
            evaluation_parameters = {}
        self.evaluation_parameters = evaluation_parameters
        if statistics is None:
            statistics = {}
        self.statistics = statistics
        if meta is None:
            meta = {}
        ensure_json_serializable(
            meta
        )  # We require meta information to be serializable.
        self.meta = meta
        self._metrics = {}

    def __eq__(self, other):
        """ExpectationSuiteValidationResult equality ignores instance identity, relying only on properties."""
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return all(
            (
                self.success == other.success,
                self.results == other.results,
                self.evaluation_parameters == other.evaluation_parameters,
                self.statistics == other.statistics,
                self.meta == other.meta,
            )
        )

    def __repr__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def to_json_dict(self):
        myself = deepcopy(self)
        # NOTE - JPC - 20191031: migrate to expectation-specific schemas that subclass result with properly-typed
        # schemas to get serialization all-the-way down via dump
        myself["evaluation_parameters"] = convert_to_json_serializable(
            myself["evaluation_parameters"]
        )
        myself["statistics"] = convert_to_json_serializable(myself["statistics"])
        myself["meta"] = convert_to_json_serializable(myself["meta"])
        myself = expectationSuiteValidationResultSchema.dump(myself)
        return myself

    def get_metric(self, metric_name, **kwargs):
        metric_name_parts = metric_name.split(".")
        metric_kwargs_id = get_metric_kwargs_id(metric_name, kwargs)

        metric_value = None
        # Expose overall statistics
        if metric_name_parts[0] == "statistics":
            if len(metric_name_parts) == 2:
                return self.statistics.get(metric_name_parts[1])
            else:
                raise UnavailableMetricError(
                    "Unrecognized metric {}".format(metric_name)
                )

        # Expose expectation-defined metrics
        elif metric_name_parts[0].lower().startswith("expect_"):
            # Check our cache first
            if (metric_name, metric_kwargs_id) in self._metrics:
                return self._metrics[(metric_name, metric_kwargs_id)]
            else:
                for result in self.results:
                    try:
                        if (
                            metric_name_parts[0]
                            == result.expectation_config.expectation_type
                        ):
                            metric_value = result.get_metric(metric_name, **kwargs)
                            break
                    except UnavailableMetricError:
                        pass
                if metric_value is not None:
                    self._metrics[(metric_name, metric_kwargs_id)] = metric_value
                    return metric_value

        raise UnavailableMetricError(
            "Metric {} with metric_kwargs_id {} is not available.".format(
                metric_name, metric_kwargs_id
            )
        )


class ExpectationSuiteValidationResultSchema(Schema):
    success = fields.Bool()
    results = fields.List(fields.Nested(ExpectationValidationResultSchema))
    evaluation_parameters = fields.Dict()
    statistics = fields.Dict()
    meta = fields.Dict(allow_none=True)

    # noinspection PyUnusedLocal
    @pre_dump
    def prepare_dump(self, data, **kwargs):
        data = deepcopy(data)
        data.meta = convert_to_json_serializable(data.meta)
        return data

    # noinspection PyUnusedLocal
    @post_load
    def make_expectation_suite_validation_result(self, data, **kwargs):
        return ExpectationSuiteValidationResult(**data)


expectationConfigurationSchema = ExpectationConfigurationSchema()
expectationSuiteSchema = ExpectationSuiteSchema()
expectationValidationResultSchema = ExpectationValidationResultSchema()
expectationSuiteValidationResultSchema = ExpectationSuiteValidationResultSchema()
runIdentifierSchema = RunIdentifierSchema()
