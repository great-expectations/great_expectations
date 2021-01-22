import copy
import importlib
import json
import logging
import os
import time
from collections import OrderedDict
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
from typing import Any, Callable, Optional, Union

from pkg_resources import Distribution

from great_expectations.core.expectation_suite import expectationSuiteSchema
from great_expectations.exceptions import (
    PluginClassNotFoundError,
    PluginModuleNotFoundError,
)
from great_expectations.expectations.registry import _registered_expectations

try:
    # This library moved in python 3.8
    import importlib.metadata as importlib_metadata
except ModuleNotFoundError:
    # Fallback for python < 3.8
    import importlib_metadata


logger = logging.getLogger(__name__)


def measure_execution_time(func: Callable = None) -> Callable:
    @wraps(func)
    def compute_delta_t(*args, **kwargs) -> Callable:
        time_begin: int = int(round(time.time() * 1000))
        try:
            return func(*args, **kwargs)
        finally:
            time_end: int = int(round(time.time() * 1000))
            delta_t: int = time_end - time_begin
            bound_args: BoundArguments = signature(func).bind(*args, **kwargs)
            call_args: OrderedDict = bound_args.arguments
            print(
                f"Total execution time of function {func.__name__}({str(dict(call_args))}): {delta_t} ms."
            )

    return compute_delta_t


# noinspection SpellCheckingInspection
def get_project_distribution() -> Union[Distribution, None]:
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


def import_library_module(module_name: str) -> Union[ModuleType, None]:
    """
    :param module_name: a fully-qualified name of a module (e.g., "great_expectations.dataset.sqlalchemy_dataset")
    :return: raw source code of the module (if can be retrieved)
    """
    module_obj: Union[ModuleType, None]

    try:
        module_obj = importlib.import_module(module_name)
    except ImportError:
        module_obj = None

    return module_obj


def is_library_loadable(library_name: str) -> bool:
    module_obj: Union[ModuleType, None] = import_library_module(
        module_name=library_name
    )
    return module_obj is not None


def load_class(class_name, module_name):
    try:
        verify_dynamic_loading_support(module_name=module_name)
    except FileNotFoundError:
        raise PluginModuleNotFoundError(module_name)

    module_obj: Union[ModuleType, None] = import_library_module(module_name=module_name)

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

    df = pd.read_excel(filename, *args, **kwargs)
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
            expectation_suite = expectationSuiteSchema.load(expectation_suite)
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
        output_str += "{}{}/\n".format(indent, os.path.basename(root))
        subindent = " " * 4 * (level + 1)

        files.sort()
        for f in files:
            output_str += "{}{}\n".format(subindent, f)

    return output_str


def lint_code(code):
    """Lint strings of code passed in. Optional dependency "black" must be installed."""
    try:
        import black

        black_file_mode = black.FileMode()
        if not isinstance(code, str):
            raise TypeError
        try:
            linted_code = black.format_file_contents(
                code, fast=True, mode=black_file_mode
            )
            return linted_code
        except (black.NothingChanged, RuntimeError):
            return code
    except ImportError:
        logger.warning(
            "Please install the optional dependency 'black' to enable linting. Returning input with no changes."
        )
        return code


def filter_properties_dict(
    properties: dict,
    keep_fields: Optional[list] = None,
    delete_fields: Optional[list] = None,
    clean_empty: Optional[bool] = True,
    inplace: Optional[bool] = False,
) -> Optional[dict]:
    """Filter the entries of the source dictionary according to directives concerning the existing keys and values.

    Args:
        properties: source dictionary to be filtered according to the supplied filtering directives
        keep_fields: list of keys that must be retained, with the understanding that all other entries will be deleted
        delete_fields: list of keys that must be deleted, with the understanding that all other entries will be retained
        clean_empty: If True, then in addition to other filtering directives, delete entries, whose values are Falsy
        inplace: If True, then modify the source properties dictionary; otherwise, make a copy for filtering purposes

    Returns:
        The (possibly) filtered properties dictionary (or None if no entries remain after filtering is performed)
    """
    if keep_fields and delete_fields:
        raise ValueError(
            "Only one of keep_fields and delete_fields filtering directives can be specified."
        )

    if not inplace:
        properties = copy.deepcopy(properties)

    keys_for_deletion: list = []

    if keep_fields:
        keys_for_deletion.extend(
            [key for key, value in properties.items() if key not in keep_fields]
        )

    if delete_fields:
        keys_for_deletion.extend(
            [key for key, value in properties.items() if key in delete_fields]
        )

    if clean_empty:
        keys_for_deletion.extend(
            [
                key
                for key, value in properties.items()
                if not (
                    (keep_fields and key in keep_fields)
                    or is_numeric(value=value)
                    or value
                )
            ]
        )

    keys_for_deletion = list(set(keys_for_deletion))

    for key in keys_for_deletion:
        del properties[key]

    if inplace:
        return None

    return properties


def is_numeric(value: Any) -> bool:
    return value is not None and (is_int(value) or is_float(value))


def is_int(value: Any) -> bool:
    try:
        num: int = int(value)
    except (TypeError, ValueError):
        return False
    return True


def is_float(value: Any) -> bool:
    try:
        num: float = float(value)
    except (TypeError, ValueError):
        return False
    return True


def get_context():
    from great_expectations.data_context.data_context import DataContext

    return DataContext()


def generate_library_json_from_registered_expectations():
    """Generate the JSON object used to populate the public gallery"""
    library_json = {}

    for expectation_name, expectation in _registered_expectations.items():
        report_object = expectation().run_diagnostics()
        library_json[expectation_name] = report_object

    return library_json
