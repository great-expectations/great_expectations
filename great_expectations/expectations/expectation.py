import re
from abc import ABC, ABCMeta
from collections import Counter
from copy import deepcopy
from inspect import isabstract
from typing import Optional, Union

import pandas as pd
from dateutil.parser import parse

from great_expectations import __version__ as ge_version
from great_expectations.core.expectation_configuration import \
    ExpectationConfiguration
from great_expectations.core.expectation_validation_result import \
    ExpectationValidationResult
from great_expectations.dataset.dataset import DatasetBackendTypes
from great_expectations.exceptions import (
    GreatExpectationsError, InvalidExpectationConfigurationError)
from great_expectations.expectations.registry import register_expectation

from ..core.batch import Batch
from ..data_asset.util import recursively_convert_to_json_serializable
from ..exceptions.expectation_engine import (UnimplementedBackendError,
                                             UnrecognizedDataAssetError)

p1 = re.compile(r"(.)([A-Z][a-z]+)")
p2 = re.compile(r"([a-z0-9])([A-Z])")


def camel_to_snake(name):
    name = p1.sub(r"\1_\2", name)
    return p2.sub(r"\1_\2", name).lower()


def lookup_backend(data):
    if isinstance(data, Batch):
        return data.get_expectation_engine()

    if isinstance(data, pd.DataFrame):
        return DatasetBackendTypes.PandasDataFrame

    raise UnrecognizedDataAssetError(
        "Unable to determine the expectation engine for data."
    )


class MetaExpectation(ABCMeta):
    def __new__(cls, clsname, bases, attrs):
        newclass = super(MetaExpectation, cls).__new__(cls, clsname, bases, attrs)
        if not isabstract(newclass):
            newclass.expectation_type = camel_to_snake(clsname)
            register_expectation(newclass)
        return newclass


class Expectation(ABC, metaclass=MetaExpectation):
    version = ge_version
    default_expectation_args = {
        "include_config": True,
        "catch_exceptions": True,
        "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 20,},
    }
    validation_kwargs = []

    def __init__(self, configuration: Optional[ExpectationConfiguration] = None):
        if configuration is not None:
            self.validate_configuration(configuration)
        self._configuration = configuration

    def __check_validation_kwargs_definition(self):
        """As a convenience to implementers, we verify that validation kwargs are indeed always supersets of their
        parent validation_kwargs"""
        validation_kwargs_set = set(self.validation_kwargs)
        for parent in self.mro():
            assert validation_kwargs_set <= set(
                getattr(parent, "validation_kwargs", set())
            ), ("Invalid Expectation " "definition for : " + self.__class__.__name__)
        return True

    def build_validation_kwargs(
        self, configuration: Optional[ExpectationConfiguration]
    ):
        if configuration is None:
            configuration = self.configuration
        self.validate_configuration(configuration)
        return dict()

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        if configuration is None:
            configuration = self.configuration
        try:
            assert configuration.expectation_type == self.expectation_type, (
                "expectation configuration type does not match " "expectation type"
            )
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    def validate(
        self,
        data,
        configuration: Optional[ExpectationConfiguration] = None,
        runtime_configuration=None,
    ):
        if configuration is None:
            configuration = self.configuration
        result = self._validate(
            data,
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        if isinstance(result, dict):
            result = ExpectationValidationResult(**result)
        return result

    def _validate(
        self, data, configuration: ExpectationConfiguration, runtime_configuration=None
    ):
        raise NotImplementedError

    @property
    def configuration(self):
        if self._configuration is None:
            raise InvalidExpectationConfigurationError(
                "cannot access configuration: expectation has not yet been configured"
            )
        return self._configuration

    @classmethod
    def build_configuration(cls, *args, **kwargs):
        # Combine all arguments into a single new "all_args" dictionary to name positional parameters
        all_args = dict(zip(cls.validation_kwargs, args))
        all_args.update(kwargs)

        # Unpack display parameters; remove them from all_args if appropriate
        if "include_config" in kwargs:
            include_config = kwargs["include_config"]
            del all_args["include_config"]
        else:
            include_config = cls.default_expectation_args["include_config"]

        if "catch_exceptions" in kwargs:
            catch_exceptions = kwargs["catch_exceptions"]
            del all_args["catch_exceptions"]
        else:
            catch_exceptions = cls.default_expectation_args["catch_exceptions"]

        if "result_format" in kwargs:
            result_format = kwargs["result_format"]
        else:
            result_format = cls.default_expectation_args["result_format"]

        # Extract the meta object for use as a top-level expectation_config holder
        if "meta" in kwargs:
            meta = kwargs["meta"]
            del all_args["meta"]
        else:
            meta = None

        # all_args = recursively_convert_to_json_serializable(all_args)
        #
        # # Patch in PARAMETER args, and remove locally-supplied arguments
        # # This will become the stored config
        # expectation_args = copy.deepcopy(all_args)
        #
        # if self._expectation_suite.evaluation_parameters:
        #     evaluation_args = build_evaluation_parameters(
        #         expectation_args,
        #         self._expectation_suite.evaluation_parameters,
        #         self._config.get("interactive_evaluation", True)
        #     )
        # else:
        #     evaluation_args = build_evaluation_parameters(
        #         expectation_args, None, self._config.get("interactive_evaluation", True))

        # Construct the expectation_config object
        return ExpectationConfiguration(
            expectation_type=cls.expectation_type,
            kwargs=recursively_convert_to_json_serializable(deepcopy(all_args)),
            meta=meta,
        )


class DatasetExpectation(Expectation, ABC):
    @staticmethod
    def get_value_set_parser(backend_type: DatasetBackendTypes):
        if backend_type == DatasetBackendTypes.PandasDataFrame:
            return DatasetExpectation._pandas_value_set_parser

        raise GreatExpectationsError(
            "No parser found for backend: " + str(backend_type)
        )

    @staticmethod
    def _pandas_value_set_parser(value_set):
        parsed_value_set = [
            parse(value) if isinstance(value, str) else value for value in value_set
        ]
        return parsed_value_set

    def parse_value_set(
        self, backend_type: DatasetBackendTypes, value_set: Union[list, set]
    ):
        value_set_parser = self.get_value_set_parser(backend_type)
        return value_set_parser(value_set)


class ColumnMapDatasetExpectation(DatasetExpectation, ABC):
    def build_validation_kwargs(
        self, configuration: Optional[ExpectationConfiguration]
    ):
        validation_kwargs = super().build_validation_kwargs(configuration)
        validation_kwargs.update(
            {
                "column": configuration.kwargs.get("column"),
                "mostly": configuration.kwargs.get("mostly", 1),
            }
        )
        return validation_kwargs

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        try:
            assert (
                "column" in configuration.kwargs
            ), "'column' parameter is required for column map expectations"
            if "mostly" in configuration.kwargs:
                mostly = configuration.kwargs["mostly"]
                assert isinstance(
                    mostly, (int, float)
                ), "'mostly' parameter must be an integer or float"
                assert 0 <= mostly <= 1, "'mostly' parameter must be between 0 and 1"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    def format_map_output(
        self,
        result_format,
        success,
        element_count,
        nonnull_count,
        unexpected_count,
        unexpected_list,
        unexpected_index_list,
    ):
        """Delegate to the helper method. But we maintain this method so that subclassers can choose to modify if
        desired."""
        return _format_map_output(
            result_format,
            success,
            element_count,
            nonnull_count,
            unexpected_count,
            unexpected_list,
            unexpected_index_list,
        )

    def calculate_map_expectation_success(self, success_count, nonnull_count, mostly):
        return _calc_map_expectation_success(success_count, nonnull_count, mostly)

    def _build_runtime_kwargs(self, configuration):
        # TODO: add support for configuration defaults
        return {
            # TODO: use configuration default
            "result_format": configuration.kwargs.get("result_format", "SUMMARY")
        }

    def get_validator(self, data: "Dataset"):
        if isinstance(data, Batch):
            data = data.data
        backend_type = lookup_backend(data)
        if backend_type == DatasetBackendTypes.PandasDataFrame:
            return self._validate_pandas
        raise UnimplementedBackendError(
            "Unable to find validator for the data type provided."
        )

    def _validate(
        self,
        data: "Dataset",
        configuration: ExpectationConfiguration,
        runtime_configuration=None,
    ):
        validator = self.get_validator(data)
        result = validator(data, configuration, runtime_configuration)
        return result

    def _validate_pandas(
        self,
        data: "Dataset",
        configuration: ExpectationConfiguration,
        runtime_configuration=None,
    ):
        import numpy as np

        validation_kwargs = self.build_validation_kwargs(configuration)
        runtime_configuration = configuration.build_runtime_configuration(
            runtime_configuration
        )
        result_format = runtime_configuration.result_format

        column = validation_kwargs.pop("column")
        series = data[column]
        element_count = int(len(series))
        if configuration.expectation_type in [
            "expect_column_values_to_not_be_null",
            "expect_column_values_to_be_null",
        ]:
            # Counting the number of unexpected values can be expensive when there is a large
            # number of np.nan values.
            # This only happens on expect_column_values_to_not_be_null expectations.
            # Since there is no reason to look for most common unexpected values in this case,
            # we will instruct the result formatting method to skip this step.
            # FIXME rename to mapped_ignore_values?
            boolean_mapped_null_values = np.full(series.shape, False)
            result_format["partial_unexpected_count"] = 0
        else:
            boolean_mapped_null_values = series.isnull().values

        validated_values = series[boolean_mapped_null_values == False]
        validated_values_count = int((boolean_mapped_null_values == False).sum())

        boolean_mapped_success_values = self._validate_pandas_series(
            validated_values,
            **validation_kwargs,
            runtime_configuration=runtime_configuration
        )
        success_count = np.count_nonzero(boolean_mapped_success_values)

        unexpected_list = list(validated_values[boolean_mapped_success_values == False])
        unexpected_index_list = list(
            validated_values[boolean_mapped_success_values == False].index
        )

        # DEPRECATED / NO LONGER SUPPORTED
        # if "output_strftime_format" in runtime_kwargs:
        #     output_strftime_format = runtime_kwargs["output_strftime_format"]
        #     parsed_unexpected_list = []
        #     for val in unexpected_list:
        #         if val is None:
        #             parsed_unexpected_list.append(val)
        #         else:
        #             if isinstance(val, str):
        #                 val = parse(val)
        #             parsed_unexpected_list.append(datetime.strftime(val, output_strftime_format))
        #     unexpected_list = parsed_unexpected_list

        success, percent_success = self.calculate_map_expectation_success(
            success_count, validated_values_count, validation_kwargs.get("mostly")
        )

        return_obj = self.format_map_output(
            result_format,
            success,
            element_count,
            validated_values_count,
            len(unexpected_list),
            unexpected_list,
            unexpected_index_list,
        )
        return return_obj

    def _validate_pandas_series(self, series: pd.Series, **kwargs):
        raise NotImplementedError


def _calc_map_expectation_success(success_count, nonnull_count, mostly):
    """Calculate success and percent_success for column_map_expectations

    Args:
        success_count (int): \
            The number of successful values in the column
        nonnull_count (int): \
            The number of nonnull values in the column
        mostly (float or None): \
            A value between 0 and 1 (or None), indicating the fraction of successes required to pass the \
            expectation as a whole. If mostly=None, then all values must succeed in order for the expectation as \
            a whole to succeed.

    Returns:
        success (boolean), percent_success (float)
    """

    if nonnull_count > 0:
        # percent_success = float(success_count)/nonnull_count
        percent_success = success_count / nonnull_count

        if mostly is not None:
            success = bool(percent_success >= mostly)

        else:
            success = bool(nonnull_count - success_count == 0)

    else:
        success = True
        percent_success = None

    return success, percent_success


def _format_map_output(
    result_format,
    success,
    element_count,
    nonnull_count,
    unexpected_count,
    unexpected_list,
    unexpected_index_list,
):
    """Helper function to construct expectation result objects for map_expectations (such as column_map_expectation
    and file_lines_map_expectation).

    Expectations support four result_formats: BOOLEAN_ONLY, BASIC, SUMMARY, and COMPLETE.
    In each case, the object returned has a different set of populated fields.
    See :ref:`result_format` for more information.

    This function handles the logic for mapping those fields for column_map_expectations.
    """
    # NB: unexpected_count parameter is explicit some implementing classes may limit the length of unexpected_list
    # Incrementally add to result and return when all values for the specified level are present
    return_obj = {"success": success}

    if result_format["result_format"] == "BOOLEAN_ONLY":
        return return_obj

    missing_count = element_count - nonnull_count

    if element_count > 0:
        unexpected_percent = unexpected_count / element_count * 100
        missing_percent = missing_count / element_count * 100

        if nonnull_count > 0:
            unexpected_percent_nonmissing = unexpected_count / nonnull_count * 100
        else:
            unexpected_percent_nonmissing = None

    else:
        missing_percent = None
        unexpected_percent = None
        unexpected_percent_nonmissing = None

    return_obj["result"] = {
        "element_count": element_count,
        "missing_count": missing_count,
        "missing_percent": missing_percent,
        "unexpected_count": unexpected_count,
        "unexpected_percent": unexpected_percent,
        "unexpected_percent_nonmissing": unexpected_percent_nonmissing,
        "partial_unexpected_list": unexpected_list[
            : result_format["partial_unexpected_count"]
        ],
    }

    if result_format["result_format"] == "BASIC":
        return return_obj

    # Try to return the most common values, if possible.
    if 0 < result_format.get("partial_unexpected_count"):
        try:
            partial_unexpected_counts = [
                {"value": key, "count": value}
                for key, value in sorted(
                    Counter(unexpected_list).most_common(
                        result_format["partial_unexpected_count"]
                    ),
                    key=lambda x: (-x[1], x[0]),
                )
            ]
        except TypeError:
            partial_unexpected_counts = [
                "partial_exception_counts requires a hashable type"
            ]
        finally:
            return_obj["result"].update(
                {
                    "partial_unexpected_index_list": unexpected_index_list[
                        : result_format["partial_unexpected_count"]
                    ]
                    if unexpected_index_list is not None
                    else None,
                    "partial_unexpected_counts": partial_unexpected_counts,
                }
            )

    if result_format["result_format"] == "SUMMARY":
        return return_obj

    return_obj["result"].update(
        {
            "unexpected_list": unexpected_list,
            "unexpected_index_list": unexpected_index_list,
        }
    )

    if result_format["result_format"] == "COMPLETE":
        return return_obj

    raise ValueError("Unknown result_format %s." % (result_format["result_format"],))
