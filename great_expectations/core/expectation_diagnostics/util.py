from __future__ import annotations

import copy
import re
import traceback
from logging import Logger
from typing import TYPE_CHECKING, Any, Dict, Optional

import numpy as np

from great_expectations.exceptions import (
    InvalidExpectationConfigurationError,
    MetricProviderError,
    MetricResolutionError,
)

if TYPE_CHECKING:
    from great_expectations.validator.validator import Validator

RTOL: float = 1.0e-7
ATOL: float = 5.0e-2
RX_FLOAT = re.compile(r".*\d\.\d+.*")


def evaluate_json_test_v3_api(  # noqa: PLR0912, PLR0913
    validator: Validator,
    expectation_type: str,
    test: Dict[str, Any],
    raise_exception: bool = True,
    debug_logger: Optional[Logger] = None,
    pk_column: bool = False,
):
    """
    This method will evaluate the result of a test build using the Great Expectations json test format.

    NOTE: Tests can be suppressed for certain data types if the test contains the Key 'suppress_test_for' with a list
        of DataAsset types to suppress, such as ['SQLAlchemy', 'Pandas'].

    :param validator: (Validator) reference to "Validator" (key object that resolves Metrics and validates Expectations)
    :param expectation_type: (string) the name of the expectation to be run using the test input
    :param test: (dict) a dictionary containing information for the test to be run. The dictionary must include:
        - title: (string) the name of the test
        - exact_match_out: (boolean) If true, match the 'out' dictionary exactly against the result of the expectation
        - in: (dict or list) a dictionary of keyword arguments to use to evaluate the expectation or a list of positional arguments
        - out: (dict) the dictionary keys against which to make assertions. Unless exact_match_out is true, keys must\
            come from the following list:
              - success
              - observed_value
              - unexpected_index_list
              - unexpected_list
              - details
              - traceback_substring (if present, the string value will be expected as a substring of the exception_traceback)
    :param raise_exception: (bool) If False, capture any failed AssertionError from the call to check_json_test_result and return with validation_result
    :param debug_logger: logger instance or None
    :param pk_column: If True, then the primary-key column has been defined in the json test data.
    :return: Tuple(ExpectationValidationResult, error_message, stack_trace). asserts correctness of results.
    """
    from great_expectations.core import ExpectationSuite

    if debug_logger is not None:
        _debug = lambda x: debug_logger.debug(  # noqa: E731
            f"(evaluate_json_test_v3_api) {x}"
        )
    else:
        _debug = lambda x: x  # noqa: E731

    expectation_suite = ExpectationSuite(
        "json_test_suite", data_context=validator._data_context
    )
    # noinspection PyProtectedMember
    validator._initialize_expectations(expectation_suite=expectation_suite)
    # validator.set_default_expectation_argument("result_format", "COMPLETE")

    if "title" not in test:
        raise ValueError("Invalid test configuration detected: 'title' is required.")

    if "exact_match_out" not in test:
        raise ValueError(
            "Invalid test configuration detected: 'exact_match_out' is required."
        )

    if "input" not in test:
        if "in" in test:
            test["input"] = test["in"]
        else:
            raise ValueError(
                "Invalid test configuration detected: 'input' is required."
            )

    if "output" not in test:
        if "out" in test:
            test["output"] = test["out"]
        else:
            raise ValueError(
                "Invalid test configuration detected: 'output' is required."
            )

    kwargs = copy.deepcopy(test["input"])
    error_message = None
    stack_trace = None

    try:
        if isinstance(test["input"], list):
            result = getattr(validator, expectation_type)(*kwargs)
        # As well as keyword arguments
        else:
            if pk_column:
                runtime_kwargs = {
                    "result_format": {
                        "result_format": "COMPLETE",
                        "unexpected_index_column_names": ["pk_index"],
                    },
                }
            else:
                runtime_kwargs = {
                    "result_format": {
                        "result_format": "COMPLETE",
                    },
                }
            runtime_kwargs.update(kwargs)
            result = getattr(validator, expectation_type)(**runtime_kwargs)
    except (
        MetricProviderError,
        MetricResolutionError,
        InvalidExpectationConfigurationError,
    ) as e:
        if raise_exception:
            raise
        error_message = str(e)
        stack_trace = (traceback.format_exc(),)
        result = None
    else:
        try:
            check_json_test_result(
                test=test,
                result=result,
                data_asset=validator.execution_engine.batch_manager.active_batch_data,
                pk_column=pk_column,
            )
        except Exception as e:
            _debug(
                f"RESULT: {result['result']}  |  CONFIG: {result['expectation_config']}"
            )
            if raise_exception:
                raise
            error_message = str(e)
            stack_trace = (traceback.format_exc(),)

    return (result, error_message, stack_trace)


def check_json_test_result(  # noqa: C901, PLR0912, PLR0915
    test, result, data_asset=None, pk_column=False
) -> None:
    from great_expectations.core.expectation_validation_result import (
        ExpectationValidationResultSchema,
    )

    expectationValidationResultSchema = ExpectationValidationResultSchema()

    # check for id_pk results in cases where pk_column is true and unexpected_index_list already exists
    # this will work for testing since result_format is COMPLETE
    if pk_column:
        if not result["success"]:
            if "unexpected_index_list" in result["result"]:
                assert "unexpected_index_query" in result["result"]

    if "unexpected_list" in result["result"]:
        if ("result" in test["output"]) and (
            "unexpected_list" in test["output"]["result"]
        ):
            (
                test["output"]["result"]["unexpected_list"],
                result["result"]["unexpected_list"],
            ) = sort_unexpected_values(
                test["output"]["result"]["unexpected_list"],
                result["result"]["unexpected_list"],
            )
        elif "unexpected_list" in test["output"]:
            (
                test["output"]["unexpected_list"],
                result["result"]["unexpected_list"],
            ) = sort_unexpected_values(
                test["output"]["unexpected_list"],
                result["result"]["unexpected_list"],
            )

    if "partial_unexpected_list" in result["result"]:
        if ("result" in test["output"]) and (
            "partial_unexpected_list" in test["output"]["result"]
        ):
            (
                test["output"]["result"]["partial_unexpected_list"],
                result["result"]["partial_unexpected_list"],
            ) = sort_unexpected_values(
                test["output"]["result"]["partial_unexpected_list"],
                result["result"]["partial_unexpected_list"],
            )
        elif "partial_unexpected_list" in test["output"]:
            (
                test["output"]["partial_unexpected_list"],
                result["result"]["partial_unexpected_list"],
            ) = sort_unexpected_values(
                test["output"]["partial_unexpected_list"],
                result["result"]["partial_unexpected_list"],
            )

    # Determine if np.allclose(..) might be needed for float comparison
    try_allclose = False
    if "observed_value" in test["output"]:
        if RX_FLOAT.match(repr(test["output"]["observed_value"])):
            try_allclose = True

    # Check results
    if test["exact_match_out"] is True:
        if "result" in result and "observed_value" in result["result"]:
            if isinstance(result["result"]["observed_value"], (np.floating, float)):
                assert np.allclose(
                    result["result"]["observed_value"],
                    expectationValidationResultSchema.load(test["output"])["result"][
                        "observed_value"
                    ],
                    rtol=RTOL,
                    atol=ATOL,
                ), f"(RTOL={RTOL}, ATOL={ATOL}) {result['result']['observed_value']} not np.allclose to {expectationValidationResultSchema.load(test['output'])['result']['observed_value']}"
            else:
                assert result == expectationValidationResultSchema.load(
                    test["output"]
                ), f"{result} != {expectationValidationResultSchema.load(test['output'])}"
        else:
            assert result == expectationValidationResultSchema.load(
                test["output"]
            ), f"{result} != {expectationValidationResultSchema.load(test['output'])}"
    else:
        # Convert result to json since our tests are reading from json so cannot easily contain richer types (e.g. NaN)
        # NOTE - 20191031 - JPC - we may eventually want to change these tests as we update our view on how
        # representations, serializations, and objects should interact and how much of that is shown to the user.
        result = result.to_json_dict()
        for key, value in test["output"].items():
            if key == "success":
                if isinstance(value, (np.floating, float)):
                    try:
                        assert np.allclose(
                            result["success"],
                            value,
                            rtol=RTOL,
                            atol=ATOL,
                        ), f"(RTOL={RTOL}, ATOL={ATOL}) {result['success']} not np.allclose to {value}"
                    except TypeError:
                        assert (
                            result["success"] == value
                        ), f"{result['success']} != {value}"
                else:
                    assert result["success"] == value, f"{result['success']} != {value}"

            elif key == "observed_value":
                if "tolerance" in test:
                    if isinstance(value, dict):
                        assert set(result["result"]["observed_value"].keys()) == set(
                            value.keys()
                        ), f"{set(result['result']['observed_value'].keys())} != {set(value.keys())}"
                        for k, v in value.items():
                            assert np.allclose(
                                result["result"]["observed_value"][k],
                                v,
                                rtol=test["tolerance"],
                            )
                    else:
                        assert np.allclose(
                            result["result"]["observed_value"],
                            value,
                            rtol=test["tolerance"],
                        )
                else:  # noqa: PLR5501
                    if isinstance(value, dict) and "values" in value:
                        try:
                            assert np.allclose(
                                result["result"]["observed_value"]["values"],
                                value["values"],
                                rtol=RTOL,
                                atol=ATOL,
                            ), f"(RTOL={RTOL}, ATOL={ATOL}) {result['result']['observed_value']['values']} not np.allclose to {value['values']}"
                        except TypeError as e:
                            print(e)
                            assert (
                                result["result"]["observed_value"] == value
                            ), f"{result['result']['observed_value']} != {value}"
                    elif try_allclose:
                        assert np.allclose(
                            result["result"]["observed_value"],
                            value,
                            rtol=RTOL,
                            atol=ATOL,
                        ), f"(RTOL={RTOL}, ATOL={ATOL}) {result['result']['observed_value']} not np.allclose to {value}"
                    else:
                        assert (
                            result["result"]["observed_value"] == value
                        ), f"{result['result']['observed_value']} != {value}"

            # NOTE: This is a key used ONLY for testing cases where an expectation is legitimately allowed to return
            # any of multiple possible observed_values. expect_column_values_to_be_of_type is one such expectation.
            elif key == "observed_value_list":
                assert result["result"]["observed_value"] in value

            elif key == "unexpected_index_list":
                unexpected_list = result["result"].get("unexpected_index_list")
                if pk_column and unexpected_list:
                    # Note that consistent ordering of unexpected_list is not a guarantee by ID/PK
                    assert (
                        sorted(unexpected_list, key=lambda d: d["pk_index"]) == value
                    ), f"{unexpected_list} != {value}"

            elif key == "unexpected_list":
                try:
                    assert result["result"]["unexpected_list"] == value, (
                        "expected "
                        + str(value)
                        + " but got "
                        + str(result["result"]["unexpected_list"])
                    )
                except AssertionError:
                    if result["result"]["unexpected_list"]:
                        if isinstance(result["result"]["unexpected_list"][0], list):
                            unexpected_list_tup = [
                                tuple(x) for x in result["result"]["unexpected_list"]
                            ]
                            assert (
                                unexpected_list_tup == value
                            ), f"{unexpected_list_tup} != {value}"
                        else:
                            raise
                    else:
                        raise

            elif key == "partial_unexpected_list":
                assert result["result"]["partial_unexpected_list"] == value, (
                    "expected "
                    + str(value)
                    + " but got "
                    + str(result["result"]["partial_unexpected_list"])
                )

            elif key == "unexpected_count":
                pass

            elif key == "details":
                assert result["result"]["details"] == value

            elif key == "value_counts":
                for val_count in value:
                    assert val_count in result["result"]["details"]["value_counts"]

            elif key.startswith("observed_cdf"):
                if "x_-1" in key:
                    if key.endswith("gt"):
                        assert (
                            result["result"]["details"]["observed_cdf"]["x"][-1] > value
                        )
                    else:
                        assert (
                            result["result"]["details"]["observed_cdf"]["x"][-1]
                            == value
                        )
                elif "x_0" in key:
                    if key.endswith("lt"):
                        assert (
                            result["result"]["details"]["observed_cdf"]["x"][0] < value
                        )
                    else:
                        assert (
                            result["result"]["details"]["observed_cdf"]["x"][0] == value
                        )
                else:
                    raise ValueError(
                        f"Invalid test specification: unknown key {key} in 'out'"
                    )

            elif key == "traceback_substring":
                assert result["exception_info"][
                    "raised_exception"
                ], f"{result['exception_info']['raised_exception']}"
                assert value in result["exception_info"]["exception_traceback"], (
                    "expected to find "
                    + value
                    + " in "
                    + result["exception_info"]["exception_traceback"]
                )

            elif key == "expected_partition":
                assert np.allclose(
                    result["result"]["details"]["expected_partition"]["bins"],
                    value["bins"],
                )
                assert np.allclose(
                    result["result"]["details"]["expected_partition"]["weights"],
                    value["weights"],
                )
                if "tail_weights" in result["result"]["details"]["expected_partition"]:
                    assert np.allclose(
                        result["result"]["details"]["expected_partition"][
                            "tail_weights"
                        ],
                        value["tail_weights"],
                    )

            elif key == "observed_partition":
                assert np.allclose(
                    result["result"]["details"]["observed_partition"]["bins"],
                    value["bins"],
                )
                assert np.allclose(
                    result["result"]["details"]["observed_partition"]["weights"],
                    value["weights"],
                )
                if "tail_weights" in result["result"]["details"]["observed_partition"]:
                    assert np.allclose(
                        result["result"]["details"]["observed_partition"][
                            "tail_weights"
                        ],
                        value["tail_weights"],
                    )

            else:
                raise ValueError(
                    f"Invalid test specification: unknown key {key} in 'out'"
                )


def sort_unexpected_values(test_value_list, result_value_list):
    # check if value can be sorted; if so, sort so arbitrary ordering of results does not cause failure
    if (isinstance(test_value_list, list)) & (len(test_value_list) >= 1):
        # __lt__ is not implemented for python dictionaries making sorting trickier
        # in our case, we will sort on the values for each key sequentially
        if isinstance(test_value_list[0], dict):
            test_value_list = sorted(
                test_value_list,
                key=lambda x: tuple(x[k] for k in list(test_value_list[0].keys())),
            )
            result_value_list = sorted(
                result_value_list,
                key=lambda x: tuple(x[k] for k in list(test_value_list[0].keys())),
            )
        # if python built-in class has __lt__ then sorting can always work this way
        elif type(test_value_list[0].__lt__(test_value_list[0])) != type(
            NotImplemented
        ):
            test_value_list = sorted(test_value_list, key=lambda x: str(x))
            result_value_list = sorted(result_value_list, key=lambda x: str(x))

    return test_value_list, result_value_list
