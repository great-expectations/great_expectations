# This file contains several decorators used in Databricks Delta Live Tables
# To use these decorators, import this module and then use the decorators in place of the
# decorators provided by delta live tables.
import functools
import sys
from types import ModuleType
from typing import Optional, Tuple

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
        import dlt

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
    dlt_expectation_name: Optional[str] = None,
    dlt_expectation_condition: Optional[str] = None,
    data_context: Optional[BaseDataContext] = None,
    ge_expectation_configuration: Optional[ExpectationConfiguration] = None,
    dlt_library=dlt_mock_library,
):
    """
    Run a single expectation on a Delta Live Table
    Please provide either a dlt_expectation_condition OR a ge_expectation_configuration, not both.
    """

    def decorator_expect(func):
        @functools.wraps(func)
        def wrapper_expect(*args, **kwargs):

            dlt = _get_dlt_library(dlt_library=dlt_library)

            _validate_dlt_decorator_arguments(
                dlt_expectation_condition=dlt_expectation_condition,
                ge_expectation_configuration=ge_expectation_configuration,
            )

            (
                dlt_expectation,
                ge_expectation_configuration_to_run,
            ) = _translate_expectations(
                dlt_expectation_name=dlt_expectation_name,
                dlt_expectation_condition=dlt_expectation_condition,
                ge_expectation_configuration=ge_expectation_configuration,
            )

            # Great Expectations evaluated first on the full dataset before any rows are dropped
            #   via `expect_or_drop` Delta Live Tables expectations
            if data_context is not None:

                # checkpoint_result: CheckpointResult = run_ge_checkpoint_on_dataframe_from_suite(
                #     data_context=data_context,
                #     df=args[0],
                #     expectation_configuration=ge_expectation_configuration_to_run,
                # )
                pass
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

                # TODO: Remove, these diagnostics are for development work only:
                _dlt_expect_return_value_diagnostics(dlt_expect_return_value)

            # TODO: Remove, these diagnostics are for development work only:
            _dlt_diagnostics(dlt)

            func_result = func(*args, **kwargs)

            # TODO: Remove, these diagnostics are for development work only:
            _func_result_diagnostics(func_result)

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


def _translate_expectations(
    dlt_expectation_name: Optional[str] = None,
    dlt_expectation_condition: Optional[str] = None,
    ge_expectation_configuration: Optional[ExpectationConfiguration] = None,
) -> Tuple[DLTExpectation, ExpectationConfiguration]:

    # Initialize return values
    ge_expectation_from_dlt: Optional[ExpectationConfiguration] = None
    dlt_expectation: Optional[DLTExpectation] = None

    # Create DLT expectation object
    if dlt_expectation_condition is not None:
        dlt_expectation: DLTExpectation = DLTExpectation(
            name=dlt_expectation_name, condition=dlt_expectation_condition
        )

        # Translate DLT expectation to GE ExpectationConfiguration
        ge_expectation_from_dlt = translate_dlt_expectation_to_expectation_config(
            dlt_expectations=[(dlt_expectation.name, dlt_expectation.condition)],
            ge_expectation_type="expect_column_values_to_not_be_null",
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

    # TODO: Get rid of this ugly mess:
    if ge_expectation_configuration is not None:
        ge_expectation_configuration_to_run = ge_expectation_configuration
    elif ge_expectation_from_dlt is not None:
        ge_expectation_configuration_to_run = ge_expectation_from_dlt
    else:
        ge_expectation_configuration_to_run = None

    return dlt_expectation, ge_expectation_configuration_to_run


def _dlt_diagnostics(dlt):
    """TODO: For use in development, remove before release"""
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
        print(f"type({dlt_attribute})", type(getattr(dlt, dlt_attribute)))
        # print(
        #     f"get_size({dlt_attribute})",
        #     get_size(getattr(dlt, dlt_attribute)),
        # )
        print(
            f"sys.getsizeof({dlt_attribute})",
            sys.getsizeof(getattr(dlt, dlt_attribute)),
        )
        print(f"dir({dlt_attribute})", dir(getattr(dlt, dlt_attribute)))

    api_attributes = [
        "DLT_DECORATOR_RETURN",
        "DataFrame",
        "Dataset",
        "Expectation",
        "FlowFunction",
        "SQLContext",
        "ViolationAction",
        "_add_expectations",
        "_register_dataset",
        "_register_table",
        "_register_view",
        "_sql",
        "expect",
        "expect_all",
        "expect_all_or_drop",
        "expect_all_or_fail",
        "expect_or_drop",
        "expect_or_fail",
        "pipeline",
        "read",
        "read_stream",
        "table",
        "view",
    ]

    for api_attribute in api_attributes:
        print(f"type({api_attribute})", type(getattr(dlt.api, api_attribute)))
        print(
            f"sys.getsizeof({api_attribute})",
            sys.getsizeof(getattr(dlt.api, api_attribute)),
        )
        print(f"dir({api_attribute})", dir(getattr(dlt.api, api_attribute)))

    print(type(dlt.dataset.Dataset))
    import inspect

    print(
        "inspect.getfullargspec(dlt.create_table)",
        inspect.getfullargspec(dlt.create_table),
    )
    print(
        "inspect.getfullargspec(dlt.api.table)",
        inspect.getfullargspec(dlt.api.table),
    )
    print(
        "inspect.getfullargspec(dlt.api.view)",
        inspect.getfullargspec(dlt.api.view),
    )
    print(
        "inspect.getfullargspec(dlt.api.expect)",
        inspect.getfullargspec(dlt.api.expect),
    )

    # dir(DataFrame)[
    #     '__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattr__', '__getattribute__', '__getitem__', '__gt__', '__hash__', '__init__', '__init_subclass__', '__le__', '__lt__', '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', '_collect_as_arrow', '_jcols', '_jmap', '_jseq', '_repr_html_', '_sort_cols', '_to_corrected_pandas_type', 'agg', 'alias', 'approxQuantile', 'cache', 'checkpoint', 'coalesce', 'colRegex', 'collect', 'columns', 'corr', 'count', 'cov', 'createGlobalTempView', 'createOrReplaceGlobalTempView', 'createOrReplaceTempView', 'createTempView', 'crossJoin', 'crosstab', 'cube', 'describe', 'display', 'distinct', 'drop', 'dropDuplicates', 'drop_duplicates', 'dropna', 'dtypes', 'exceptAll', 'explain', 'fillna', 'filter', 'first', 'foreach', 'foreachPartition', 'freqItems', 'groupBy', 'groupby', 'head', 'hint', 'inputFiles', 'intersect', 'intersectAll', 'isLocal', 'isStreaming', 'join', 'limit', 'localCheckpoint', 'mapInPandas', 'na', 'orderBy', 'persist', 'printSchema', 'randomSplit', 'rdd', 'registerTempTable', 'repartition', 'repartitionByRange', 'replace', 'rollup', 'sameSemantics', 'sample', 'sampleBy', 'schema', 'select', 'selectExpr', 'semanticHash', 'show', 'sort', 'sortWithinPartitions', 'stat', 'storageLevel', 'subtract', 'summary', 'tail', 'take', 'toDF', 'toJSON', 'toLocalIterator', 'toPandas', 'to_koalas', 'transform', 'union', 'unionAll', 'unionByName', 'unpersist', 'where', 'withColumn', 'withColumnRenamed', 'withWatermark', 'write', 'writeStream', 'writeTo']

    # This DataFrame is not an object with data, it is a type.
    df = getattr(dlt, "DataFrame")
    print("df.columns", df.columns)
    # print("df.count()", df.count())

    # clickstream_raw_df = dlt.read("clickstream_raw")
    # print("type(clickstream_raw_df)", type(clickstream_raw_df))
    # print("dir(clickstream_raw_df)", dir(clickstream_raw_df))

    # clickstream_prepared_df = dlt.read("clickstream_prepared")
    # print("type(clickstream_prepared_df)", type(clickstream_prepared_df))
    # print("dir(clickstream_prepared_df)", dir(clickstream_prepared_df))

    print(
        "\n\ndlt DIAGNOSTICS END =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
    )


def _func_result_diagnostics(func_result):
    """TODO: For use in development, remove before release"""
    print(
        "\n\nfunc_result DIAGNOSTICS START =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
    )

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
    print(
        "\n\nfunc_result DIAGNOSTICS END =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
    )


def _dlt_expect_return_value_diagnostics(dlt_expect_return_value):
    """TODO: For use in development, remove before release"""
    print(
        "\n\ndlt_expect_return_value DIAGNOSTICS START =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
    )
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
        "\n\ndlt_expect_return_value DIAGNOSTICS END =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
    )
