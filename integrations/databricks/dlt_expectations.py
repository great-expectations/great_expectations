# This file contains several decorators used in Databricks Delta Live Tables
# To use these decorators, import this module and then use the decorators in place of the
# decorators provided by delta live tables.
import functools
import sys
from types import ModuleType
from typing import Optional

from ruamel.yaml import YAML

from great_expectations.core import ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from integrations.databricks.dlt_expectation import (
    DLTExpectation,
    DLTExpectationFactory,
)
from integrations.databricks.dlt_expectation_translator import (
    translate_dlt_expectation_to_expectation_config,
)
from integrations.databricks.dlt_ge_utils import (
    run_ge_checkpoint_on_dataframe_from_suite,
)
from integrations.databricks.exceptions import UnsupportedExpectationConfiguration

try:
    from integrations.databricks import dlt_mock_library
except:
    # TODO: Make this a real error message & place error messages as appropriate - but potentially allow non DLT workflows (GE validations) to continue with a warning:
    print(
        "could not import dlt_mock_library - please install or run in the DLT environment to enable this package. TODO: make this a real error message"
    )
    dlt_mock_library = None


yaml = YAML()


def _get_dlt_library(dlt_library: Optional[ModuleType] = None) -> ModuleType:
    """
    Check if dlt library is installed, if not then use the one passed in
    Args:
        dlt_library: dlt library to be used in dependency injection if dlt library
            is not imported into the current environment

    Returns:
        dlt library if already loaded or passed in
    """
    try:
        print(
            "\n\nDIAGNOSTICS START =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
        )
        print("dir(dlt)", dir(dlt))
        print(
            "\n\nDIAGNOSTICS END =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
        )
    except NameError:
        dlt = dlt_library

    if dlt is None:
        raise ModuleNotFoundError("dlt library was not found")
    else:
        return dlt


# Preparation outside of decorator:
# 1. Set up GE (metadata & data docs stores, runtime data connector)
# 2. Create Expectation Configuration (or Expectation Suite for `_all` type decorators e.g. expect_all())
# 3. Pass DLT or GE expectation using the appropriate keyword arguments in the decorator


def expect(
    _func=None,
    *,
    dlt_expectation_name: str = None,
    dlt_expectation_condition: str = None,
    data_context: BaseDataContext = None,
    ge_expectation_configuration: ExpectationConfiguration = None,
    dlt=dlt_mock_library,
):
    """
    Run a single expectation on a Delta Live Table
    Please provide either a dlt_expectation_condition OR a ge_expectation_configuration, not both.
    """

    def decorator_expect(func):
        @functools.wraps(func)
        def wrapper_expect(*args, **kwargs):

            # dlt = _get_dlt_library(dlt_library=dlt_library)

            print(
                "\n\nDIAGNOSTICS START =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
            )
            print("dir(dlt)", dir(dlt))
            print(
                "\n\nDIAGNOSTICS END =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
            )

            _validate_dlt_decorator_arguments(
                dlt_expectation_condition=dlt_expectation_condition,
                ge_expectation_configuration=ge_expectation_configuration,
            )

            # Create DLT expectation object
            ge_expectation_from_dlt: Optional[ExpectationConfiguration] = None
            if dlt_expectation_condition is not None:
                dlt_expectation: DLTExpectation = DLTExpectation(
                    name=dlt_expectation_name, condition=dlt_expectation_condition
                )

                # Translate DLT expectation to GE ExpectationConfiguration
                ge_expectation_from_dlt = (
                    translate_dlt_expectation_to_expectation_config(
                        dlt_expectations=[
                            (dlt_expectation.name, dlt_expectation.condition)
                        ],
                        ge_expectation_type="expect_column_values_to_not_be_null",
                    )
                )
            elif ge_expectation_configuration is not None:
                # Translate GE ExpectationConfiguration to DLT expectation
                dlt_expectation_factory: DLTExpectationFactory = DLTExpectationFactory()
                dlt_expectation: DLTExpectation = (
                    dlt_expectation_factory.from_great_expectations_expectation(
                        ge_expectation_configuration=ge_expectation_configuration,
                        dlt_expectation_name=dlt_expectation_name,
                    )
                )

            # Great Expectations evaluated first on the full dataset before any rows are dropped
            #   via `expect_or_drop` Delta Live Tables expectations
            if data_context is not None:
                # TODO: Get rid of this ugly mess:
                if ge_expectation_configuration is not None:
                    ge_expectation_configuration_to_run = ge_expectation_configuration
                elif ge_expectation_from_dlt is not None:
                    ge_expectation_configuration_to_run = ge_expectation_from_dlt
                else:
                    ge_expectation_configuration_to_run = None
                run_ge_checkpoint_on_dataframe_from_suite(
                    data_context=data_context,
                    df=args[0],
                    expectation_configuration=ge_expectation_configuration_to_run,
                )

                # TODO: getting the df from args[0] is throwing an in-pipeline error, how do we get access to the dataframe?

            if dlt_expectation is not None:
                print(
                    f'Here we would normally apply: @dlt.expect("{dlt_expectation.name}", "{dlt_expectation.condition}")'
                )
                print(
                    f"Here we are instead calling @dlt_mock_library_injected.expect() with appropriate parameters, in the real system the `dlt_mock_library_injected` will be replaced with the main `dlt` library that we pass into the decorator via the `dlt` parameter."
                )
                dlt_expect_return_value = dlt.expect(
                    dlt_expectation.name, dlt_expectation.condition
                )
                print("\n")
                print(
                    "\n\ndlt DIAGNOSTICS START =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
                )
                print("dir(dlt)", dir(dlt))
                dlt_attributes = [
                    "DLT_DECORATOR_RETURN",
                    "DataFrame",
                    "Dataset",
                    "Expectation",
                    "FlowFunction",
                    "SQLContext",
                    "ViolationAction",
                    "api",
                    "create_table",
                    "create_view",
                    "dataset",
                    "expect",
                    "expect_all",
                    "expect_all_or_drop",
                    "expect_all_or_fail",
                    "expect_or_drop",
                    "expect_or_fail",
                    "helpers",
                    "pipeline",
                    "read",
                    "read_stream",
                    "table",
                    "view",
                ]
                for dlt_attribute in dlt_attributes:
                    print(f"dir({dlt_attribute})", dir(getattr(dlt, dlt_attribute)))
                    print(f"type({dlt_attribute})", type(getattr(dlt, dlt_attribute)))
                    print(
                        f"get_size({dlt_attribute})",
                        get_size(getattr(dlt, dlt_attribute)),
                    )

                print(
                    "\n\ndlt DIAGNOSTICS END =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
                )

            func_result = func(*args, **kwargs)
            print(
                "\n\nreturn values DIAGNOSTICS START =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
            )
            import sys

            print("type(func_result):", type(func_result))
            print("sys.getsizeof(func_result)", sys.getsizeof(func_result))
            print("dir(func_result)", dir(func_result))
            print("get_size(func_result)", get_size(func_result))
            print("type(func_result.name):", type(func_result.name))
            print("type(func_result.name_finalized):", type(func_result.name_finalized))
            print("func_result.name", func_result.name)
            print("func_result.name_finalized", func_result.name_finalized)
            print("type(func_result.func):", type(func_result.func))
            print("type(func_result.expectations):", type(func_result.expectations))
            print("func_result.expectations", func_result.expectations)
            print("type(func_result.builder):", type(func_result.builder))
            try:
                print("type(dlt_expect_return_value):", type(dlt_expect_return_value))
                print(
                    "sys.getsizeof(dlt_expect_return_value)",
                    sys.getsizeof(dlt_expect_return_value),
                )
                print("dir(dlt_expect_return_value)", dir(dlt_expect_return_value))
                print(
                    "get_size(dlt_expect_return_value)",
                    get_size(dlt_expect_return_value),
                )
            except:
                pass
            # df = dlt.read(func_result.name)
            # print("type(df)", type(df))
            # print("df.head()", df.head())
            print(
                "\n\nreturn values DIAGNOSTICS END =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
            )
            return func_result

        return wrapper_expect

    # return decorator_expect
    if _func is None:
        return decorator_expect
    else:
        return decorator_expect(_func)


def _validate_dlt_decorator_arguments(
    dlt_expectation_condition: str,
    ge_expectation_configuration: ExpectationConfiguration,
) -> None:
    if (
        dlt_expectation_condition is not None
        and ge_expectation_configuration is not None
    ):
        raise UnsupportedExpectationConfiguration(
            f"Please provide only one of dlt_expectation_condition OR ge_expectation_configuration, not both."
        )
    elif dlt_expectation_condition is None and ge_expectation_configuration is None:
        raise UnsupportedExpectationConfiguration(
            "Please provide at least one type of expectation configuration"
        )


def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, "__dict__"):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size
