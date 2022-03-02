import copy
import cProfile
import importlib
import io
import json
import logging
import os
import pstats
import re
import time
import uuid
from collections import OrderedDict
from datetime import datetime
from functools import wraps
from gc import get_referrers
from inspect import (
    ArgInfo,
    BoundArguments,
    Parameter,
    Signature,
    currentframe,
    getargvalues,
    getclosurevars,
    getmodule,
    signature,
)
from pathlib import Path
from types import CodeType, FrameType, ModuleType
from typing import Any, Callable, List, Optional, Set, Tuple, Union

from dateutil.parser import parse
from packaging import version
from pkg_resources import Distribution

from great_expectations.core.expectation_suite import (
    ExpectationSuite,
    expectationSuiteSchema,
)
from great_expectations.exceptions import (
    GreatExpectationsError,
    PluginClassNotFoundError,
    PluginModuleNotFoundError,
)
from great_expectations.expectations.registry import _registered_expectations

try:
    import black
except ImportError:
    black = None

try:
    # This library moved in python 3.8
    import importlib.metadata as importlib_metadata
except ModuleNotFoundError:
    # Fallback for python < 3.8
    import importlib_metadata

logger = logging.getLogger(__name__)

try:
    import sqlalchemy as sa
    from sqlalchemy import Table
    from sqlalchemy.engine import reflection
    from sqlalchemy.sql import Select
except ImportError:
    logger.debug(
        "Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support"
    )
    sa = None
    reflection = None
    Table = None
    Select = None

SINGULAR_TO_PLURAL_LOOKUP_DICT: dict = {
    "batch": "batches",
    "checkpoint": "checkpoints",
    "data_asset": "data_assets",
    "expectation": "expectations",
    "expectation_suite": "expectation_suites",
    "suite_validation_result": "suite_validation_results",
    "expectation_validation_result": "expectation_validation_results",
    "contract": "contracts",
    "rendered_data_doc": "rendered_data_docs",
}

PLURAL_TO_SINGULAR_LOOKUP_DICT: dict = {
    "batches": "batch",
    "checkpoints": "checkpoint",
    "data_assets": "data_asset",
    "expectations": "expectation",
    "expectation_suites": "expectation_suite",
    "suite_validation_results": "suite_validation_result",
    "expectation_validation_results": "expectation_validation_result",
    "contracts": "contract",
    "rendered_data_docs": "rendered_data_doc",
}

MAX_PROBABILISTIC_TEST_ASSERTION_RETRIES: int = 3


def pluralize(singular_ge_noun):
    """
    Pluralizes a Great Expectations singular noun
    """
    try:
        return SINGULAR_TO_PLURAL_LOOKUP_DICT[singular_ge_noun.lower()]
    except KeyError:
        raise GreatExpectationsError(
            f"Unable to pluralize '{singular_ge_noun}'. Please update "
            f"great_expectations.util.SINGULAR_TO_PLURAL_LOOKUP_DICT"
        )


def singularize(plural_ge_noun):
    """
    Singularizes a Great Expectations plural noun
    """
    try:
        return PLURAL_TO_SINGULAR_LOOKUP_DICT[plural_ge_noun.lower()]
    except KeyError:
        raise GreatExpectationsError(
            f"Unable to singularize '{plural_ge_noun}'. Please update "
            f"great_expectations.util.PLURAL_TO_SINGULAR_LOOKUP_DICT."
        )


def underscore(word: str) -> str:
    """
    **Borrowed from inflection.underscore**
    Make an underscored, lowercase form from the expression in the string.

    Example::

        >>> underscore("DeviceType")
        'device_type'

    As a rule of thumb you can think of :func:`underscore` as the inverse of
    :func:`camelize`, though there are cases where that does not hold::

        >>> camelize(underscore("IOError"))
        'IoError'

    """
    word = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", word)
    word = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", word)
    word = word.replace("-", "_")
    return word.lower()


def hyphen(txt: str):
    return txt.replace("_", "-")


def profile(func: Callable = None) -> Callable:
    @wraps(func)
    def profile_function_call(*args, **kwargs) -> Any:
        pr: cProfile.Profile = cProfile.Profile()
        pr.enable()
        retval: Any = func(*args, **kwargs)
        pr.disable()
        s: io.StringIO = io.StringIO()
        sortby: str = pstats.SortKey.CUMULATIVE  # "cumulative"
        ps: pstats.Stats = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())
        return retval

    return profile_function_call


def measure_execution_time(
    pretty_print: bool = False,
) -> Callable:
    def execution_time_decorator(func: Callable) -> Callable:
        func.execution_duration_milliseconds = 0

        @wraps(func)
        def compute_delta_t(*args, **kwargs) -> Any:
            time_begin: int = int(round(time.time() * 1000))
            try:
                return func(*args, **kwargs)
            finally:
                time_end: int = int(round(time.time() * 1000))
                delta_t: int = time_end - time_begin
                func.execution_duration_milliseconds = delta_t

                if pretty_print:
                    bound_args: BoundArguments = signature(func).bind(*args, **kwargs)
                    call_args: OrderedDict = bound_args.arguments
                    print(
                        f"Total execution time of function {func.__name__}({str(dict(call_args))}): {delta_t} ms."
                    )

        return compute_delta_t

    return execution_time_decorator


# noinspection SpellCheckingInspection
def get_project_distribution() -> Optional[Distribution]:
    ditr: Distribution
    for distr in importlib_metadata.distributions():
        relative_path: Path
        try:
            relative_path = Path(__file__).relative_to(distr.locate_file(""))
        except ValueError:
            pass
        else:
            if relative_path in distr.files:
                return distr
    return None


# Returns the object reference to the currently running function (i.e., the immediate function under execution).
def get_currently_executing_function() -> Callable:
    cf: FrameType = currentframe()
    fb: FrameType = cf.f_back
    fc: CodeType = fb.f_code
    func_obj: Callable = [
        referer
        for referer in get_referrers(fc)
        if getattr(referer, "__code__", None) is fc
        and getclosurevars(referer).nonlocals.items() <= fb.f_locals.items()
    ][0]
    return func_obj


# noinspection SpellCheckingInspection
def get_currently_executing_function_call_arguments(
    include_module_name: bool = False, include_caller_names: bool = False, **kwargs
) -> dict:
    """
    :param include_module_name: bool If True, module name will be determined and included in output dictionary (default is False)
    :param include_caller_names: bool If True, arguments, such as "self" and "cls", if present, will be included in output dictionary (default is False)
    :param kwargs:
    :return: dict Output dictionary, consisting of call arguments as attribute "name: value" pairs.

    Example usage:
    # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter
    # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.
    self._config = get_currently_executing_function_call_arguments(
        include_module_name=True,
        **{
            "class_name": self.__class__.__name__,
        },
    )
    filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)
    """
    cf: FrameType = currentframe()
    fb: FrameType = cf.f_back
    argvs: ArgInfo = getargvalues(fb)
    fc: CodeType = fb.f_code
    cur_func_obj: Callable = [
        referer
        for referer in get_referrers(fc)
        if getattr(referer, "__code__", None) is fc
        and getclosurevars(referer).nonlocals.items() <= fb.f_locals.items()
    ][0]
    cur_mod = getmodule(cur_func_obj)
    sig: Signature = signature(cur_func_obj)
    params: dict = {}
    var_positional: dict = {}
    var_keyword: dict = {}
    for key, param in sig.parameters.items():
        val: Any = argvs.locals[key]
        params[key] = val
        if param.kind == Parameter.VAR_POSITIONAL:
            var_positional[key] = val
        elif param.kind == Parameter.VAR_KEYWORD:
            var_keyword[key] = val
    bound_args: BoundArguments = sig.bind(**params)
    call_args: OrderedDict = bound_args.arguments

    call_args_dict: dict = dict(call_args)

    for key, value in var_positional.items():
        call_args_dict[key] = value

    for key, value in var_keyword.items():
        call_args_dict.pop(key)
        call_args_dict.update(value)

    if include_module_name:
        call_args_dict.update({"module_name": cur_mod.__name__})

    if not include_caller_names:
        if call_args.get("cls"):
            call_args_dict.pop("cls", None)
        if call_args.get("self"):
            call_args_dict.pop("self", None)

    call_args_dict.update(**kwargs)

    return call_args_dict


def verify_dynamic_loading_support(module_name: str, package_name: str = None) -> None:
    """
    :param module_name: a possibly-relative name of a module
    :param package_name: the name of a package, to which the given module belongs
    """
    try:
        # noinspection PyUnresolvedReferences
        module_spec: importlib.machinery.ModuleSpec = importlib.util.find_spec(
            module_name, package=package_name
        )
    except ModuleNotFoundError:
        module_spec = None
    if not module_spec:
        if not package_name:
            package_name = ""
        message: str = f"""No module named "{package_name + module_name}" could be found in the repository. Please \
make sure that the file, corresponding to this package and module, exists and that dynamic loading of code modules, \
templates, and assets is supported in your execution environment.  This error is unrecoverable.
        """
        raise FileNotFoundError(message)


def import_library_module(module_name: str) -> Optional[ModuleType]:
    """
    :param module_name: a fully-qualified name of a module (e.g., "great_expectations.dataset.sqlalchemy_dataset")
    :return: raw source code of the module (if can be retrieved)
    """
    module_obj: Optional[ModuleType]

    try:
        module_obj = importlib.import_module(module_name)
    except ImportError:
        module_obj = None

    return module_obj


def is_library_loadable(library_name: str) -> bool:
    module_obj: Optional[ModuleType] = import_library_module(module_name=library_name)
    return module_obj is not None


def load_class(class_name: str, module_name: str):
    if class_name is None:
        raise TypeError("class_name must not be None")
    if not isinstance(class_name, str):
        raise TypeError("class_name must be a string")
    if module_name is None:
        raise TypeError("module_name must not be None")
    if not isinstance(module_name, str):
        raise TypeError("module_name must be a string")
    try:
        verify_dynamic_loading_support(module_name=module_name)
    except FileNotFoundError:
        raise PluginModuleNotFoundError(module_name)

    module_obj: Optional[ModuleType] = import_library_module(module_name=module_name)

    if module_obj is None:
        raise PluginModuleNotFoundError(module_name)
    try:
        klass_ = getattr(module_obj, class_name)
    except AttributeError:
        raise PluginClassNotFoundError(module_name=module_name, class_name=class_name)

    return klass_


def _convert_to_dataset_class(df, dataset_class, expectation_suite=None, profiler=None):
    """
    Convert a (pandas) dataframe to a great_expectations dataset, with (optional) expectation_suite

    Args:
        df: the DataFrame object to convert
        dataset_class: the class to which to convert the existing DataFrame
        expectation_suite: the expectation suite that should be attached to the resulting dataset
        profiler: the profiler to use to generate baseline expectations, if any

    Returns:
        A new Dataset object
    """

    if expectation_suite is not None:
        # Create a dataset of the new class type, and manually initialize expectations according to
        # the provided expectation suite
        new_df = dataset_class.from_dataset(df)
        new_df._initialize_expectations(expectation_suite)
    else:
        # Instantiate the new Dataset with default expectations
        new_df = dataset_class.from_dataset(df)
        if profiler is not None:
            new_df.profile(profiler)

    return new_df


def _load_and_convert_to_dataset_class(
    df, class_name, module_name, expectation_suite=None, profiler=None
):
    """
    Convert a (pandas) dataframe to a great_expectations dataset, with (optional) expectation_suite

    Args:
        df: the DataFrame object to convert
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        expectation_suite: the expectation suite that should be attached to the resulting dataset
        profiler: the profiler to use to generate baseline expectations, if any

    Returns:
        A new Dataset object
    """
    verify_dynamic_loading_support(module_name=module_name)
    dataset_class = load_class(class_name, module_name)
    return _convert_to_dataset_class(df, dataset_class, expectation_suite, profiler)


def read_csv(
    filename,
    class_name="PandasDataset",
    module_name="great_expectations.dataset",
    dataset_class=None,
    expectation_suite=None,
    profiler=None,
    *args,
    **kwargs,
):
    """Read a file using Pandas read_csv and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        dataset_class (Dataset): If specified, the class to which to convert the resulting Dataset object;
            if not specified, try to load the class named via the class_name and module_name parameters
        expectation_suite (string): path to great_expectations expectation suite file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset
    """
    import pandas as pd

    df = pd.read_csv(filename, *args, **kwargs)
    if dataset_class is not None:
        return _convert_to_dataset_class(
            df=df,
            dataset_class=dataset_class,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )
    else:
        return _load_and_convert_to_dataset_class(
            df=df,
            class_name=class_name,
            module_name=module_name,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )


def read_json(
    filename,
    class_name="PandasDataset",
    module_name="great_expectations.dataset",
    dataset_class=None,
    expectation_suite=None,
    accessor_func=None,
    profiler=None,
    *args,
    **kwargs,
):
    """Read a file using Pandas read_json and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        dataset_class (Dataset): If specified, the class to which to convert the resulting Dataset object;
            if not specified, try to load the class named via the class_name and module_name parameters
        expectation_suite (string): path to great_expectations expectation suite file
        accessor_func (Callable): functions to transform the json object in the file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset
    """
    import pandas as pd

    if accessor_func is not None:
        json_obj = json.load(open(filename, "rb"))
        json_obj = accessor_func(json_obj)
        df = pd.read_json(json.dumps(json_obj), *args, **kwargs)

    else:
        df = pd.read_json(filename, *args, **kwargs)

    if dataset_class is not None:
        return _convert_to_dataset_class(
            df=df,
            dataset_class=dataset_class,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )
    else:
        return _load_and_convert_to_dataset_class(
            df=df,
            class_name=class_name,
            module_name=module_name,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )


def read_excel(
    filename,
    class_name="PandasDataset",
    module_name="great_expectations.dataset",
    dataset_class=None,
    expectation_suite=None,
    profiler=None,
    *args,
    **kwargs,
):
    """Read a file using Pandas read_excel and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        dataset_class (Dataset): If specified, the class to which to convert the resulting Dataset object;
            if not specified, try to load the class named via the class_name and module_name parameters
        expectation_suite (string): path to great_expectations expectation suite file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset or ordered dict of great_expectations datasets,
        if multiple worksheets are imported
    """
    import pandas as pd

    try:
        df = pd.read_excel(filename, *args, **kwargs)
    except ImportError:
        raise ImportError(
            "Pandas now requires 'openpyxl' as an optional-dependency to read Excel files. Please use pip or conda to install openpyxl and try again"
        )

    if dataset_class is None:
        verify_dynamic_loading_support(module_name=module_name)
        dataset_class = load_class(class_name=class_name, module_name=module_name)
    if isinstance(df, dict):
        for key in df:
            df[key] = _convert_to_dataset_class(
                df=df[key],
                dataset_class=dataset_class,
                expectation_suite=expectation_suite,
                profiler=profiler,
            )
    else:
        df = _convert_to_dataset_class(
            df=df,
            dataset_class=dataset_class,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )
    return df


def read_table(
    filename,
    class_name="PandasDataset",
    module_name="great_expectations.dataset",
    dataset_class=None,
    expectation_suite=None,
    profiler=None,
    *args,
    **kwargs,
):
    """Read a file using Pandas read_table and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        dataset_class (Dataset): If specified, the class to which to convert the resulting Dataset object;
            if not specified, try to load the class named via the class_name and module_name parameters
        expectation_suite (string): path to great_expectations expectation suite file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset
    """
    import pandas as pd

    df = pd.read_table(filename, *args, **kwargs)
    if dataset_class is not None:
        return _convert_to_dataset_class(
            df=df,
            dataset_class=dataset_class,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )
    else:
        return _load_and_convert_to_dataset_class(
            df=df,
            class_name=class_name,
            module_name=module_name,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )


def read_feather(
    filename,
    class_name="PandasDataset",
    module_name="great_expectations.dataset",
    dataset_class=None,
    expectation_suite=None,
    profiler=None,
    *args,
    **kwargs,
):
    """Read a file using Pandas read_feather and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        dataset_class (Dataset): If specified, the class to which to convert the resulting Dataset object;
            if not specified, try to load the class named via the class_name and module_name parameters
        expectation_suite (string): path to great_expectations expectation suite file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset
    """
    import pandas as pd

    df = pd.read_feather(filename, *args, **kwargs)
    if dataset_class is not None:
        return _convert_to_dataset_class(
            df=df,
            dataset_class=dataset_class,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )
    else:
        return _load_and_convert_to_dataset_class(
            df=df,
            class_name=class_name,
            module_name=module_name,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )


def read_parquet(
    filename,
    class_name="PandasDataset",
    module_name="great_expectations.dataset",
    dataset_class=None,
    expectation_suite=None,
    profiler=None,
    *args,
    **kwargs,
):
    """Read a file using Pandas read_parquet and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        dataset_class (Dataset): If specified, the class to which to convert the resulting Dataset object;
            if not specified, try to load the class named via the class_name and module_name parameters
        expectation_suite (string): path to great_expectations expectation suite file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset
    """
    import pandas as pd

    df = pd.read_parquet(filename, *args, **kwargs)
    if dataset_class is not None:
        return _convert_to_dataset_class(
            df=df,
            dataset_class=dataset_class,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )
    else:
        return _load_and_convert_to_dataset_class(
            df=df,
            class_name=class_name,
            module_name=module_name,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )


def from_pandas(
    pandas_df,
    class_name="PandasDataset",
    module_name="great_expectations.dataset",
    dataset_class=None,
    expectation_suite=None,
    profiler=None,
):
    """Read a Pandas data frame and return a great_expectations dataset.

    Args:
        pandas_df (Pandas df): Pandas data frame
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        dataset_class (Dataset): If specified, the class to which to convert the resulting Dataset object;
            if not specified, try to load the class named via the class_name and module_name parameters
        expectation_suite (string) = None: path to great_expectations expectation suite file
        profiler (profiler class) = None: The profiler that should
            be run on the dataset to establish a baseline expectation suite.

    Returns:
        great_expectations dataset
    """
    if dataset_class is not None:
        return _convert_to_dataset_class(
            df=pandas_df,
            dataset_class=dataset_class,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )
    else:
        return _load_and_convert_to_dataset_class(
            df=pandas_df,
            class_name=class_name,
            module_name=module_name,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )


def read_pickle(
    filename,
    class_name="PandasDataset",
    module_name="great_expectations.dataset",
    dataset_class=None,
    expectation_suite=None,
    profiler=None,
    *args,
    **kwargs,
):
    """Read a file using Pandas read_pickle and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        dataset_class (Dataset): If specified, the class to which to convert the resulting Dataset object;
            if not specified, try to load the class named via the class_name and module_name parameters
        expectation_suite (string): path to great_expectations expectation suite file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset
    """
    import pandas as pd

    df = pd.read_pickle(filename, *args, **kwargs)
    if dataset_class is not None:
        return _convert_to_dataset_class(
            df=df,
            dataset_class=dataset_class,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )
    else:
        return _load_and_convert_to_dataset_class(
            df=df,
            class_name=class_name,
            module_name=module_name,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )


def read_sas(
    filename,
    class_name="PandasDataset",
    module_name="great_expectations.dataset",
    dataset_class=None,
    expectation_suite=None,
    profiler=None,
    *args,
    **kwargs,
):
    """Read a file using Pandas read_sas and return a great_expectations dataset.

    Args:
        filename (string): path to file to read
        class_name (str): class to which to convert resulting Pandas df
        module_name (str): dataset module from which to try to dynamically load the relevant module
        dataset_class (Dataset): If specified, the class to which to convert the resulting Dataset object;
            if not specified, try to load the class named via the class_name and module_name parameters
        expectation_suite (string): path to great_expectations expectation suite file
        profiler (Profiler class): profiler to use when creating the dataset (default is None)

    Returns:
        great_expectations dataset
    """
    import pandas as pd

    df = pd.read_sas(filename, *args, **kwargs)
    if dataset_class is not None:
        return _convert_to_dataset_class(
            df=df,
            dataset_class=dataset_class,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )
    else:
        return _load_and_convert_to_dataset_class(
            df=df,
            class_name=class_name,
            module_name=module_name,
            expectation_suite=expectation_suite,
            profiler=profiler,
        )


def validate(
    data_asset,
    expectation_suite=None,
    data_asset_name=None,
    expectation_suite_name=None,
    data_context=None,
    data_asset_class_name=None,
    data_asset_module_name="great_expectations.dataset",
    data_asset_class=None,
    *args,
    **kwargs,
):
    """Validate the provided data asset. Validate can accept an optional data_asset_name to apply, data_context to use
    to fetch an expectation_suite if one is not provided, and data_asset_class_name/data_asset_module_name or
    data_asset_class to use to provide custom expectations.

    Args:
        data_asset: the asset to validate
        expectation_suite: the suite to use, or None to fetch one using a DataContext
        data_asset_name: the name of the data asset to use
        expectation_suite_name: the name of the expectation_suite to use
        data_context: data context to use to fetch an an expectation suite, or the path from which to obtain one
        data_asset_class_name: the name of a class to dynamically load a DataAsset class
        data_asset_module_name: the name of the module to dynamically load a DataAsset class
        data_asset_class: a class to use. overrides data_asset_class_name/ data_asset_module_name if provided
        *args:
        **kwargs:

    Returns:

    """
    # Get an expectation suite if not provided
    if expectation_suite is None and data_context is None:
        raise ValueError(
            "Either an expectation suite or a DataContext is required for validation."
        )

    if expectation_suite is None:
        logger.info("Using expectation suite from DataContext.")
        # Allow data_context to be a string, and try loading it from path in that case
        if isinstance(data_context, str):
            from great_expectations.data_context import DataContext

            data_context = DataContext(data_context)
        expectation_suite = data_context.get_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
    else:
        if isinstance(expectation_suite, dict):
            expectation_suite_dict: dict = expectationSuiteSchema.load(
                expectation_suite
            )
            expectation_suite: ExpectationSuite = ExpectationSuite(
                **expectation_suite_dict, data_context=data_context
            )
        if data_asset_name is not None:
            raise ValueError(
                "When providing an expectation suite, data_asset_name cannot also be provided."
            )
        if expectation_suite_name is not None:
            raise ValueError(
                "When providing an expectation suite, expectation_suite_name cannot also be provided."
            )
        logger.info(
            "Validating data_asset_name %s with expectation_suite_name %s"
            % (data_asset_name, expectation_suite.expectation_suite_name)
        )

    # If the object is already a DataAsset type, then this is purely a convenience method
    # and no conversion is needed; try to run validate on the given object
    if data_asset_class_name is None and data_asset_class is None:
        return data_asset.validate(
            expectation_suite=expectation_suite,
            data_context=data_context,
            *args,
            **kwargs,
        )

    # Otherwise, try to convert and validate the dataset
    if data_asset_class is None:
        verify_dynamic_loading_support(module_name=data_asset_module_name)
        data_asset_class = load_class(data_asset_class_name, data_asset_module_name)

    import pandas as pd

    from great_expectations.dataset import Dataset, PandasDataset

    if data_asset_class is None:
        # Guess the GE data_asset_type based on the type of the data_asset
        if isinstance(data_asset, pd.DataFrame):
            data_asset_class = PandasDataset
        # Add other data_asset_type conditions here as needed

    # Otherwise, we will convert for the user to a subclass of the
    # existing class to enable new expectations, but only for datasets
    if not isinstance(data_asset, (Dataset, pd.DataFrame)):
        raise ValueError(
            "The validate util method only supports dataset validations, including custom subclasses. For other data "
            "asset types, use the object's own validate method."
        )

    if not issubclass(type(data_asset), data_asset_class):
        if isinstance(data_asset, pd.DataFrame) and issubclass(
            data_asset_class, PandasDataset
        ):
            pass  # This is a special type of allowed coercion
        else:
            raise ValueError(
                "The validate util method only supports validation for subtypes of the provided data_asset_type."
            )

    data_asset_ = _convert_to_dataset_class(
        data_asset, dataset_class=data_asset_class, expectation_suite=expectation_suite
    )
    return data_asset_.validate(*args, data_context=data_context, **kwargs)


# https://stackoverflow.com/questions/9727673/list-directory-tree-structure-in-python
def gen_directory_tree_str(startpath):
    """Print the structure of directory as a tree:

    Ex:
    project_dir0/
        AAA/
        BBB/
            aaa.txt
            bbb.txt

    #Note: files and directories are sorted alphabetically, so that this method can be used for testing.
    """

    output_str = ""

    tuples = list(os.walk(startpath))
    tuples.sort()

    for root, dirs, files in tuples:
        level = root.replace(startpath, "").count(os.sep)
        indent = " " * 4 * level
        output_str += f"{indent}{os.path.basename(root)}/\n"
        subindent = " " * 4 * (level + 1)

        files.sort()
        for f in files:
            output_str += f"{subindent}{f}\n"

    return output_str


def lint_code(code: str) -> str:
    """Lint strings of code passed in.  Optional dependency "black" must be installed."""

    # NOTE: Chetan 20211111 - This import was failing in Azure with 20.8b1 so we bumped up the version to 21.8b0
    # While this seems to resolve the issue, the root cause is yet to be determined.

    if black is None:
        logger.warning(
            "Please install the optional dependency 'black' to enable linting. Returning input with no changes."
        )
        return code

    black_file_mode = black.FileMode()
    if not isinstance(code, str):
        raise TypeError
    try:
        linted_code = black.format_file_contents(code, fast=True, mode=black_file_mode)
        return linted_code
    except (black.NothingChanged, RuntimeError):
        return code


def convert_json_string_to_be_python_compliant(code: str) -> str:
    """Cleans JSON-formatted string to adhere to Python syntax

    Substitute instances of 'null' with 'None' in string representations of Python dictionaries.
    Additionally, substitutes instances of 'true' or 'false' with their Python equivalents.

    Args:
        code: JSON string to update

    Returns:
        Clean, Python-compliant string

    """
    code = _convert_nulls_to_None(code)
    code = _convert_json_bools_to_python_bools(code)
    return code


def _convert_nulls_to_None(code: str) -> str:
    pattern = r'"([a-zA-Z0-9_]+)": null'
    result = re.findall(pattern, code)
    for match in result:
        code = code.replace(f'"{match}": null', f'"{match}": None')
        logger.info(
            f"Replaced '{match}: null' with '{match}: None' before writing to file"
        )
    return code


def _convert_json_bools_to_python_bools(code: str) -> str:
    pattern = r'"([a-zA-Z0-9_]+)": (true|false)'
    result = re.findall(pattern, code)
    for match in result:
        identifier, boolean = match
        curr = f'"{identifier}": {boolean}'
        updated = f'"{identifier}": {boolean.title()}'  # true -> True | false -> False
        code = code.replace(curr, updated)
        logger.info(f"Replaced '{curr}' with '{updated}' before writing to file")
    return code


def filter_properties_dict(
    properties: Optional[dict] = None,
    keep_fields: Optional[Set[str]] = None,
    delete_fields: Optional[Set[str]] = None,
    clean_nulls: bool = True,
    clean_falsy: bool = False,
    keep_falsy_numerics: bool = True,
    inplace: bool = False,
) -> Optional[dict]:
    """Filter the entries of the source dictionary according to directives concerning the existing keys and values.

    Args:
        properties: source dictionary to be filtered according to the supplied filtering directives
        keep_fields: list of keys that must be retained, with the understanding that all other entries will be deleted
        delete_fields: list of keys that must be deleted, with the understanding that all other entries will be retained
        clean_nulls: If True, then in addition to other filtering directives, delete entries, whose values are None
        clean_falsy: If True, then in addition to other filtering directives, delete entries, whose values are Falsy
        (If the "clean_falsy" argument is specified at "True", then "clean_nulls" is assumed to be "True" as well.)
        inplace: If True, then modify the source properties dictionary; otherwise, make a copy for filtering purposes
        keep_falsy_numerics: If True, then in addition to other filtering directives, do not delete zero-valued numerics

    Returns:
        The (possibly) filtered properties dictionary (or None if no entries remain after filtering is performed)
    """
    if keep_fields is None:
        keep_fields = set()

    if delete_fields is None:
        delete_fields = set()

    if keep_fields & delete_fields:
        raise ValueError(
            "Common keys between sets of keep_fields and delete_fields filtering directives are illegal."
        )

    if clean_falsy:
        clean_nulls = True

    if properties is None:
        properties = {}

    if not isinstance(properties, dict):
        raise ValueError(
            f'Source "properties" must be a dictionary (illegal type "{str(type(properties))}" detected).'
        )

    if not inplace:
        properties = copy.deepcopy(properties)

    keys_for_deletion: list = []

    key: str
    value: Any

    if keep_fields:
        keys_for_deletion.extend(
            [key for key, value in properties.items() if key not in keep_fields]
        )

    if delete_fields:
        keys_for_deletion.extend(
            [key for key, value in properties.items() if key in delete_fields]
        )

    if clean_nulls:
        keys_for_deletion.extend(
            [
                key
                for key, value in properties.items()
                if not (
                    (keep_fields and key in keep_fields)
                    or (delete_fields and key in delete_fields)
                    or value is not None
                )
            ]
        )

    if clean_falsy:
        if keep_falsy_numerics:
            keys_for_deletion.extend(
                [
                    key
                    for key, value in properties.items()
                    if not (
                        (keep_fields and key in keep_fields)
                        or (delete_fields and key in delete_fields)
                        or is_truthy(value=value)
                        or is_numeric(value=value)
                    )
                ]
            )
        else:
            keys_for_deletion.extend(
                [
                    key
                    for key, value in properties.items()
                    if not (
                        (keep_fields and key in keep_fields)
                        or (delete_fields and key in delete_fields)
                        or is_truthy(value=value)
                    )
                ]
            )

    keys_for_deletion = list(set(keys_for_deletion))

    for key in keys_for_deletion:
        del properties[key]

    if inplace:
        return None

    return properties


def deep_filter_properties_iterable(
    properties: Optional[Union[dict, list, set, tuple]] = None,
    keep_fields: Optional[Set[str]] = None,
    delete_fields: Optional[Set[str]] = None,
    clean_nulls: bool = True,
    clean_falsy: bool = False,
    keep_falsy_numerics: bool = True,
    inplace: bool = False,
) -> Optional[Union[dict, list, set]]:
    if isinstance(properties, dict):
        if not inplace:
            properties = copy.deepcopy(properties)

        filter_properties_dict(
            properties=properties,
            keep_fields=keep_fields,
            delete_fields=delete_fields,
            clean_nulls=clean_nulls,
            clean_falsy=clean_falsy,
            keep_falsy_numerics=keep_falsy_numerics,
            inplace=True,
        )

        key: str
        value: Any
        for key, value in properties.items():
            deep_filter_properties_iterable(
                properties=value,
                keep_fields=keep_fields,
                delete_fields=delete_fields,
                clean_nulls=clean_nulls,
                clean_falsy=clean_falsy,
                keep_falsy_numerics=keep_falsy_numerics,
                inplace=True,
            )

        # Upon unwinding the call stack, do a sanity check to ensure cleaned properties
        keys_to_delete: List[str] = list(
            filter(
                lambda k: _is_to_be_removed_from_deep_filter_properties_iterable(
                    value=properties[k],
                    clean_nulls=clean_nulls,
                    clean_falsy=clean_falsy,
                    keep_falsy_numerics=keep_falsy_numerics,
                ),
                properties.keys(),
            )
        )
        for key in keys_to_delete:
            properties.pop(key)

    elif isinstance(properties, (list, set, tuple)):
        if not inplace:
            properties = copy.deepcopy(properties)

        value: Any
        for value in properties:
            deep_filter_properties_iterable(
                properties=value,
                keep_fields=keep_fields,
                delete_fields=delete_fields,
                clean_nulls=clean_nulls,
                clean_falsy=clean_falsy,
                keep_falsy_numerics=keep_falsy_numerics,
                inplace=True,
            )

        # Upon unwinding the call stack, do a sanity check to ensure cleaned properties
        properties = list(
            filter(
                lambda v: not _is_to_be_removed_from_deep_filter_properties_iterable(
                    value=v,
                    clean_nulls=clean_nulls,
                    clean_falsy=clean_falsy,
                    keep_falsy_numerics=keep_falsy_numerics,
                ),
                properties,
            )
        )

    if inplace:
        return None

    return properties


def _is_to_be_removed_from_deep_filter_properties_iterable(
    value: Any, clean_nulls: bool, clean_falsy: bool, keep_falsy_numerics: bool
) -> bool:
    conditions: Tuple[bool, ...] = (
        clean_nulls and value is None,
        not keep_falsy_numerics and is_numeric(value) and value == 0,
        clean_falsy and not is_numeric(value) and not value,
    )
    return any(condition for condition in conditions)


def is_truthy(value: Any) -> bool:
    try:
        if value:
            return True
        else:
            return False
    except ValueError:
        return False


def is_numeric(value: Any) -> bool:
    return value is not None and (is_int(value=value) or is_float(value=value))


def is_int(value: Any) -> bool:
    try:
        # noinspection PyUnusedLocal
        num: int = int(value)
    except (TypeError, ValueError):
        return False
    return True


def is_float(value: Any) -> bool:
    try:
        # noinspection PyUnusedLocal
        num: float = float(value)
    except (TypeError, ValueError):
        return False
    return True


def is_nan(value: Any) -> bool:
    """
    If value is an array, test element-wise for NaN and return result as a boolean array.
    If value is a scalar, return boolean.
    Args:
        value: The value to test

    Returns:
        The results of the test
    """
    import numpy as np

    try:
        return np.isnan(value)
    except TypeError:
        return True


def is_parseable_date(value: Any, fuzzy: bool = False) -> bool:
    try:
        # noinspection PyUnusedLocal
        parsed_date: datetime = parse(value, fuzzy=fuzzy)
    except (TypeError, ValueError):
        return False
    return True


def get_context():
    from great_expectations.data_context.data_context import DataContext

    return DataContext()


def is_sane_slack_webhook(url: str) -> bool:
    """Really basic sanity checking."""
    if url is None:
        return False

    return url.strip().startswith("https://hooks.slack.com/")


def is_list_of_strings(_list) -> bool:
    return isinstance(_list, list) and all([isinstance(site, str) for site in _list])


def generate_library_json_from_registered_expectations():
    """Generate the JSON object used to populate the public gallery"""
    library_json = {}

    for expectation_name, expectation in _registered_expectations.items():
        report_object = expectation().run_diagnostics()
        library_json[expectation_name] = report_object

    return library_json


def delete_blank_lines(text: str) -> str:
    return re.sub(r"\n\s*\n", "\n", text, flags=re.MULTILINE)


def generate_temporary_table_name(
    default_table_name_prefix: str = "ge_temp_",
    num_digits: int = 8,
) -> str:
    table_name: str = f"{default_table_name_prefix}{str(uuid.uuid4())[:num_digits]}"
    return table_name


def get_sqlalchemy_inspector(engine):
    if version.parse(sa.__version__) < version.parse("1.4"):
        # Inspector.from_engine deprecated since 1.4, sa.inspect() should be used instead
        insp = reflection.Inspector.from_engine(engine)
    else:
        insp = sa.inspect(engine)
    return insp


def get_sqlalchemy_url(drivername, **credentials):
    if version.parse(sa.__version__) < version.parse("1.4"):
        # Calling URL() deprecated since 1.4, URL.create() should be used instead
        url = sa.engine.url.URL(drivername, **credentials)
    else:
        url = sa.engine.url.URL.create(drivername, **credentials)
    return url


def get_sqlalchemy_selectable(selectable: Union[Table, Select]) -> Union[Table, Select]:
    """
    Beginning from SQLAlchemy 1.4, a select() can no longer be embedded inside of another select() directly,
    without explicitly turning the inner select() into a subquery first. This helper method ensures that this
    conversion takes place.

    https://docs.sqlalchemy.org/en/14/changelog/migration_14.html#change-4617
    """
    if version.parse(sa.__version__) >= version.parse("1.4"):
        if isinstance(selectable, Select):
            selectable = selectable.subquery()
    return selectable


def get_sqlalchemy_domain_data(domain_data):
    if version.parse(sa.__version__) < version.parse("1.4"):
        # Implicit coercion of SELECT and SELECT constructs is deprecated since 1.4
        # select(query).subquery() should be used instead
        domain_data = sa.select(["*"]).select_from(domain_data)
    # engine.get_domain_records returns a valid select object;
    # calling fetchall at execution is equivalent to a SELECT *
    return domain_data


def import_make_url():
    """
    Beginning from SQLAlchemy 1.4, make_url is accessed from sqlalchemy.engine; earlier versions must
    still be accessed from sqlalchemy.engine.url to avoid import errors.
    """
    if version.parse(sa.__version__) < version.parse("1.4"):
        from sqlalchemy.engine.url import make_url
    else:
        from sqlalchemy.engine import make_url
    return make_url


def probabilistic_test(
    func: Callable = None,
    max_num_retries: int = MAX_PROBABILISTIC_TEST_ASSERTION_RETRIES,
) -> Callable:
    @wraps(func)
    def run_pytest_method(*args, **kwargs) -> None:
        assertion_error: Optional[AssertionError] = None
        error_message: Optional[str] = None

        all_assertions_passed: bool = False

        idx: int = 0
        while idx < max_num_retries:
            try:
                func(*args, **kwargs)
                all_assertions_passed = True
            except AssertionError as e:
                error_message = re.sub(r"\W+", " ", str(e)).strip()
                logger.warning(
                    f"""Attempt {idx + 1} to execute "{func}" failed with error "{error_message}".  Retrying."""
                )
                all_assertions_passed = False
                assertion_error = e

            if all_assertions_passed:
                break

            idx += 1

        if not all_assertions_passed:
            logger.error(
                f"""Aborting trying to execute "{func}" (exceeded maximum allowed \
{MAX_PROBABILISTIC_TEST_ASSERTION_RETRIES} attempts).  Error "{error_message}" is being raised.
"""
            )
            raise assertion_error

    return run_pytest_method
