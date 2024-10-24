from __future__ import annotations

import copy
import cProfile
import datetime
import decimal
import importlib
import inspect
import io
import json
import logging
import os
import pathlib
import pstats
import re
import sys
import time
import uuid
from collections import OrderedDict
from functools import wraps
from inspect import (
    BoundArguments,
    signature,
    stack,
)
from numbers import Number
from pathlib import Path
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    SupportsFloat,
    Tuple,
    Union,
    cast,
    overload,
)

import numpy as np
import pandas as pd
from dateutil.parser import parse
from packaging import version

from great_expectations.compatibility import pydantic, pyspark, sqlalchemy
from great_expectations.compatibility.sqlalchemy import LegacyRow, Row
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.typing_extensions import override

# import of private class will be removed when deprecated methods are removed from this module
from great_expectations.exceptions import (
    InvalidExpectationConfigurationError,
    PluginClassNotFoundError,
    PluginModuleNotFoundError,
)
from great_expectations.render import RenderedContent
from great_expectations.types import SerializableDictDot
from great_expectations.types.base import SerializableDotDict

if TYPE_CHECKING:
    # needed until numpy min version 1.20
    import numpy.typing as npt
    from ruamel.yaml.comments import CommentedMap
    from typing_extensions import TypeAlias, TypeGuard

    from great_expectations.alias_types import JSONValues, PathStr
    from great_expectations.data_context import (
        AbstractDataContext,
    )
    from great_expectations.data_context.types.base import DataContextConfig


try:
    from shapely.geometry import LineString, MultiPolygon, Point, Polygon
except ImportError:
    Point = None
    Polygon = None
    MultiPolygon = None
    LineString = None


logger = logging.getLogger(__name__)


p1 = re.compile(r"(.)([A-Z][a-z]+)")
p2 = re.compile(r"([a-z0-9])([A-Z])")


class bidict(dict):
    """
    Bi-directional hashmap: https://stackoverflow.com/a/21894086
    """

    def __init__(self, *args: List[Any], **kwargs: Dict[str, Any]) -> None:
        super().__init__(*args, **kwargs)
        self.inverse: Dict = {}
        for key, value in self.items():
            self.inverse.setdefault(value, []).append(key)

    @override
    def __setitem__(self, key: str, value: Any) -> None:
        if key in self:
            self.inverse[self[key]].remove(key)
        super().__setitem__(key, value)
        self.inverse.setdefault(value, []).append(key)

    @override
    def __delitem__(self, key: str):
        self.inverse.setdefault(self[key], []).remove(key)
        if self[key] in self.inverse and not self.inverse[self[key]]:
            del self.inverse[self[key]]
        super().__delitem__(key)


def camel_to_snake(name: str) -> str:
    name = p1.sub(r"\1_\2", name)
    return p2.sub(r"\1_\2", name).lower()


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


def profile(func: Callable) -> Callable:
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
    execution_time_holder_object_reference_name: str = "execution_time_holder",
    execution_time_property_name: str = "execution_time",
    method: str = "process_time",
    pretty_print: bool = True,
    include_arguments: bool = True,
) -> Callable:
    """
    Parameterizes template "execution_time_decorator" function with options, supplied as arguments.

    Args:
        execution_time_holder_object_reference_name: Handle, provided in "kwargs", holds execution time property setter.
        execution_time_property_name: Property attribute name, provided in "kwargs", sets execution time value.
        method: Name of method in "time" module (default: "process_time") to be used for recording timestamps.
        pretty_print: If True (default), prints execution time summary to standard output; if False, "silent" mode.
        include_arguments: If True (default), prints arguments of function, whose execution time is measured.

    Note: Method "time.perf_counter()" keeps going during sleep, while method "time.process_time()" does not.
    Using "time.process_time()" is the better suited method for measuring code computational efficiency.

    Returns:
        Callable -- configured "execution_time_decorator" function.
    """  # noqa: E501

    def execution_time_decorator(func: Callable) -> Callable:
        @wraps(func)
        def compute_delta_t(*args, **kwargs) -> Any:
            """
            Computes return value of decorated function, calls back "execution_time_holder_object_reference_name", and
            saves execution time (in seconds) into specified "execution_time_property_name" of passed object reference.
            Settable "{execution_time_holder_object_reference_name}.{execution_time_property_name}" property must exist.

            Args:
                args: Positional arguments of original function being decorated.
                kwargs: Keyword arguments of original function being decorated.

            Returns:
                Any (output value of original function being decorated).
            """  # noqa: E501
            time_begin: float = (getattr(time, method))()
            try:
                return func(*args, **kwargs)
            finally:
                time_end: float = (getattr(time, method))()
                delta_t: float = time_end - time_begin
                if kwargs is None:
                    kwargs = {}

                execution_time_holder: type = kwargs.get(  # type: ignore[assignment]
                    execution_time_holder_object_reference_name
                )
                if execution_time_holder is not None and hasattr(
                    execution_time_holder, execution_time_property_name
                ):
                    setattr(execution_time_holder, execution_time_property_name, delta_t)

                if pretty_print:
                    if include_arguments:
                        bound_args: BoundArguments = signature(func).bind(*args, **kwargs)
                        call_args: OrderedDict = bound_args.arguments
                        print(
                            f"""Total execution time of function {func.__name__}({dict(call_args)!s}): {delta_t} \
seconds."""  # noqa: E501
                        )
                    else:
                        print(
                            f"Total execution time of function {func.__name__}(): {delta_t} seconds."  # noqa: E501
                        )

        return compute_delta_t

    return execution_time_decorator


def verify_dynamic_loading_support(module_name: str, package_name: Optional[str] = None) -> None:
    """
    :param module_name: a possibly-relative name of a module
    :param package_name: the name of a package, to which the given module belongs
    """
    # noinspection PyUnresolvedReferences
    module_spec: Optional[importlib.machinery.ModuleSpec]
    try:
        # noinspection PyUnresolvedReferences
        module_spec = importlib.util.find_spec(module_name, package=package_name)
    except ModuleNotFoundError:
        module_spec = None

    if not module_spec:
        if not package_name:
            package_name = ""

        message: str = f"""No module named "{package_name + module_name}" could be found in the repository. Please \
make sure that the file, corresponding to this package and module, exists and that dynamic loading of code modules, \
templates, and assets is supported in your execution environment.  This error is unrecoverable.
        """  # noqa: E501
        raise FileNotFoundError(message)


def import_library_module(module_name: str) -> Optional[ModuleType]:
    """
    :param module_name: a fully-qualified name of a module (e.g., "great_expectations.dataset.sqlalchemy_dataset")
    :return: raw source code of the module (if can be retrieved)
    """  # noqa: E501
    module_obj: Optional[ModuleType]

    try:
        module_obj = importlib.import_module(module_name)
    except ImportError:
        module_obj = None

    return module_obj


def is_library_loadable(library_name: str) -> bool:
    module_obj: Optional[ModuleType] = import_library_module(module_name=library_name)
    return module_obj is not None


def load_class(class_name: str, module_name: str) -> type:
    if class_name is None:
        raise TypeError("class_name must not be None")  # noqa: TRY003
    if not isinstance(class_name, str):
        raise TypeError("class_name must be a string")  # noqa: TRY003
    if module_name is None:
        raise TypeError("module_name must not be None")  # noqa: TRY003
    if not isinstance(module_name, str):
        raise TypeError("module_name must be a string")  # noqa: TRY003
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


def build_in_memory_runtime_context() -> AbstractDataContext:
    """
    Create generic in-memory "BaseDataContext" context for manipulations as required by tests.

    Args:
        include_pandas (bool): If True, include pandas datasource
        include_spark (bool): If True, include spark datasource
    """
    from great_expectations.data_context.types.base import (
        DataContextConfig,
        InMemoryStoreBackendDefaults,
    )

    data_context_config: DataContextConfig = DataContextConfig(
        expectations_store_name="expectations_store",
        validation_results_store_name="validation_results_store",
        checkpoint_store_name="checkpoint_store",
        store_backend_defaults=InMemoryStoreBackendDefaults(),
    )

    from great_expectations.data_context.data_context.context_factory import (
        get_context as context_factory,
    )

    context = context_factory(project_config=data_context_config, mode="ephemeral")

    return context


# https://stackoverflow.com/questions/9727673/list-directory-tree-structure-in-python
def gen_directory_tree_str(startpath: PathStr):
    """Print the structure of directory as a tree:

    Ex:
    project_dir0/
        AAA/
        BBB/
            aaa.txt
            bbb.txt

    #Note: files and directories are sorted alphabetically, so that this method can be used for testing.
    """  # noqa: E501

    output_str = ""

    tuples = list(os.walk(startpath))
    tuples.sort()

    for root, dirs, files in tuples:
        level = root.replace(str(startpath), "").count(os.sep)
        indent = " " * 4 * level
        output_str += f"{indent}{os.path.basename(root)}/\n"  # noqa: PTH119
        subindent = " " * 4 * (level + 1)

        files.sort()
        for f in files:
            output_str += f"{subindent}{f}\n"

    return output_str


# NOTE: Can delete once CLI is removed
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


# NOTE: Can delete once CLI is removed
def _convert_nulls_to_None(code: str) -> str:
    pattern = r'"([a-zA-Z0-9_]+)": null'
    result = re.findall(pattern, code)
    for match in result:
        code = code.replace(f'"{match}": null', f'"{match}": None')
        logger.info(f"Replaced '{match}: null' with '{match}: None' before writing to file")
    return code


# NOTE: Can delete once CLI is removed
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


def filter_properties_dict(  # noqa: C901, PLR0912, PLR0913
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
        (If the "clean_falsy" argument is specified as "True", then "clean_nulls" is assumed to be "True" as well.)
        inplace: If True, then modify the source properties dictionary; otherwise, make a copy for filtering purposes
        keep_falsy_numerics: If True, then in addition to other filtering directives, do not delete zero-valued numerics

    Returns:
        The (possibly) filtered properties dictionary (or None if no entries remain after filtering is performed)
    """  # noqa: E501
    if keep_fields is None:
        keep_fields = set()

    if delete_fields is None:
        delete_fields = set()

    if keep_fields & delete_fields:
        raise ValueError(  # noqa: TRY003
            "Common keys between sets of keep_fields and delete_fields filtering directives are illegal."  # noqa: E501
        )

    if clean_falsy:
        clean_nulls = True

    if properties is None:
        properties = {}

    if not isinstance(properties, dict):
        raise ValueError(  # noqa: TRY003, TRY004
            f'Source "properties" must be a dictionary (illegal type "{type(properties)!s}" detected).'  # noqa: E501
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
        keys_for_deletion.extend([key for key, value in properties.items() if key in delete_fields])

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


@overload
def deep_filter_properties_iterable(
    properties: dict,
    keep_fields: Optional[Set[str]] = ...,
    delete_fields: Optional[Set[str]] = ...,
    clean_nulls: bool = ...,
    clean_falsy: bool = ...,
    keep_falsy_numerics: bool = ...,
    inplace: bool = ...,
) -> dict: ...


@overload
def deep_filter_properties_iterable(
    properties: list,
    keep_fields: Optional[Set[str]] = ...,
    delete_fields: Optional[Set[str]] = ...,
    clean_nulls: bool = ...,
    clean_falsy: bool = ...,
    keep_falsy_numerics: bool = ...,
    inplace: bool = ...,
) -> list: ...


@overload
def deep_filter_properties_iterable(
    properties: set,
    keep_fields: Optional[Set[str]] = ...,
    delete_fields: Optional[Set[str]] = ...,
    clean_nulls: bool = ...,
    clean_falsy: bool = ...,
    keep_falsy_numerics: bool = ...,
    inplace: bool = ...,
) -> set: ...


@overload
def deep_filter_properties_iterable(
    properties: tuple,
    keep_fields: Optional[Set[str]] = ...,
    delete_fields: Optional[Set[str]] = ...,
    clean_nulls: bool = ...,
    clean_falsy: bool = ...,
    keep_falsy_numerics: bool = ...,
    inplace: bool = ...,
) -> tuple: ...


@overload
def deep_filter_properties_iterable(
    properties: None,
    keep_fields: Optional[Set[str]] = ...,
    delete_fields: Optional[Set[str]] = ...,
    clean_nulls: bool = ...,
    clean_falsy: bool = ...,
    keep_falsy_numerics: bool = ...,
    inplace: bool = ...,
) -> None: ...


def deep_filter_properties_iterable(  # noqa: C901, PLR0913
    properties: Union[dict, list, set, tuple, None] = None,
    keep_fields: Optional[Set[str]] = None,
    delete_fields: Optional[Set[str]] = None,
    clean_nulls: bool = True,
    clean_falsy: bool = False,
    keep_falsy_numerics: bool = True,
    inplace: bool = False,
) -> Union[dict, list, set, tuple, None]:
    if keep_fields is None:
        keep_fields = set()

    if delete_fields is None:
        delete_fields = set()

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

        # Upon unwinding the call stack, do a sanity check to ensure cleaned properties.
        keys_to_delete: List[str] = list(
            filter(
                lambda k: k not in keep_fields  # type: ignore[arg-type]
                and _is_to_be_removed_from_deep_filter_properties_iterable(
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

        # Upon unwinding the call stack, do a sanity check to ensure cleaned properties.
        properties_type: type = type(properties)
        properties = properties_type(
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
        clean_falsy and not (is_numeric(value) or value),
    )
    return any(condition for condition in conditions)


def is_truthy(value: Any) -> bool:
    try:
        return bool(value)
    except ValueError:
        return False


def is_numeric(value: Any) -> bool:
    return value is not None and (is_int(value=value) or is_float(value=value))


def is_int(value: Any) -> bool:
    try:
        int(value)
    except (TypeError, ValueError):
        return False
    return True


def is_float(value: Any) -> bool:
    try:
        float(value)
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


def convert_decimal_to_float(d: SupportsFloat) -> float:
    """
    This method convers "decimal.Decimal" to standard "float" type.
    """
    rule_based_profiler_call: bool = (
        len(
            list(
                filter(
                    lambda frame_info: Path(frame_info.filename).name == "parameter_builder.py"
                    and frame_info.function == "get_metrics",
                    stack(),
                )
            )
        )
        > 0
    )
    if (
        not rule_based_profiler_call
        and isinstance(d, decimal.Decimal)
        and requires_lossy_conversion(d=d)
    ):
        logger.warning(
            f"Using lossy conversion for decimal {d} to float object to support serialization."
        )

    # noinspection PyTypeChecker
    return float(d)


def requires_lossy_conversion(d: decimal.Decimal) -> bool:
    """
    This method determines whether or not conversion from "decimal.Decimal" to standard "float" type cannot be lossless.
    """  # noqa: E501
    return d - decimal.Context(prec=sys.float_info.dig).create_decimal(d) != 0


def isclose(
    operand_a: Union[datetime.datetime, datetime.timedelta, Number],
    operand_b: Union[datetime.datetime, datetime.timedelta, Number],
    rtol: float = 1.0e-5,  # controls relative weight of "operand_b" (when its magnitude is large)
    atol: float = 1.0e-8,  # controls absolute accuracy (based on floating point machine precision)
    equal_nan: bool = False,
) -> bool:
    """
    Checks whether or not two numbers (or timestamps) are approximately close to one another.

    According to "https://numpy.org/doc/stable/reference/generated/numpy.isclose.html",
        For finite values, isclose uses the following equation to test whether two floating point values are equivalent:
        "absolute(a - b) <= (atol + rtol * absolute(b))".

    This translates to:
        "absolute(operand_a - operand_b) <= (atol + rtol * absolute(operand_b))", where "operand_a" is "target" quantity
    under evaluation for being close to a "control" value, and "operand_b" serves as the "control" ("reference") value.

    The values of the absolute tolerance ("atol") parameter is chosen as a sufficiently small constant for most floating
    point machine representations (e.g., 1.0e-8), so that even if the "control" value is small in magnitude and "target"
    and "control" are close in absolute value, then the accuracy of the assessment can still be high up to the precision
    of the "atol" value (here, 8 digits as the default).  However, when the "control" value is large in magnitude, the
    relative tolerance ("rtol") parameter carries a greater weight in the comparison assessment, because the acceptable
    deviation between the two quantities can be relatively larger for them to be deemed as "close enough" in this case.
    """  # noqa: E501
    if isinstance(operand_a, str) and isinstance(operand_b, str):
        return operand_a == operand_b

    if isinstance(operand_a, datetime.datetime) and isinstance(operand_b, datetime.datetime):
        operand_a = operand_a.timestamp()  # type: ignore[assignment]
        operand_b = operand_b.timestamp()  # type: ignore[assignment]
    elif isinstance(operand_a, datetime.timedelta) and isinstance(operand_b, datetime.timedelta):
        operand_a = operand_a.total_seconds()  # type: ignore[assignment]
        operand_b = operand_b.total_seconds()  # type: ignore[assignment]

    return cast(
        bool,
        np.isclose(
            a=np.float64(operand_a),  # type: ignore[arg-type]
            b=np.float64(operand_b),  # type: ignore[arg-type]
            rtol=rtol,
            atol=atol,
            equal_nan=equal_nan,
        ),
    )


def is_candidate_subset_of_target(candidate: Any, target: Any) -> bool:
    """
    This method checks whether or not candidate object is subset of target object.
    """
    if isinstance(candidate, dict):
        key: Any  # must be "hashable"
        value: Any
        return all(
            key in target and is_candidate_subset_of_target(candidate=val, target=target[key])
            for key, val in candidate.items()
        )

    if isinstance(candidate, (list, set, tuple)):
        subitem: Any
        superitem: Any
        return all(
            any(is_candidate_subset_of_target(subitem, superitem) for superitem in target)
            for subitem in candidate
        )

    return candidate == target


def is_parseable_date(value: Any, fuzzy: bool = False) -> bool:
    try:
        _ = parse(value, fuzzy=fuzzy)
        return True
    except (TypeError, ValueError):
        try:
            _ = datetime.datetime.fromisoformat(value)
            return True
        except (TypeError, ValueError):
            return False


def is_ndarray_datetime_dtype(
    data: npt.NDArray, parse_strings_as_datetimes: bool = False, fuzzy: bool = False
) -> bool:
    """
    Determine whether or not all elements of 1-D "np.ndarray" argument are "datetime.datetime" type objects.
    """  # noqa: E501
    value: Any
    result: bool = all(isinstance(value, datetime.datetime) for value in data)
    return result or (
        parse_strings_as_datetimes
        and all(is_parseable_date(value=value, fuzzy=fuzzy) for value in data)
    )


def convert_ndarray_to_datetime_dtype_best_effort(
    data: npt.NDArray,
    datetime_detected: bool = False,
    parse_strings_as_datetimes: bool = False,
    fuzzy: bool = False,
) -> Tuple[bool, bool, npt.NDArray]:
    """
    Attempt to parse all elements of 1-D "np.ndarray" argument into "datetime.datetime" type objects.

    Returns:
        Boolean flag -- True if all elements of original "data" were "datetime.datetime" type objects; False, otherwise.
        Boolean flag -- True, if conversion was performed; False, otherwise.
        Output "np.ndarray" (converted, if necessary).
    """  # noqa: E501
    if is_ndarray_datetime_dtype(data=data, parse_strings_as_datetimes=False, fuzzy=fuzzy):
        return True, False, data

    value: Any
    if datetime_detected or is_ndarray_datetime_dtype(
        data=data, parse_strings_as_datetimes=parse_strings_as_datetimes, fuzzy=fuzzy
    ):
        try:
            return (
                False,
                True,
                np.asarray([parse(value, fuzzy=fuzzy) for value in data]),
            )
        except (TypeError, ValueError):
            pass

    return False, False, data


def convert_ndarray_datetime_to_float_dtype_utc_timezone(
    data: np.ndarray,
) -> np.ndarray:
    """
    Convert all elements of 1-D "np.ndarray" argument from "datetime.datetime" type to "timestamp" "float" type objects.

    Note: Conversion of "datetime.datetime" to "float" uses "UTC" TimeZone to normalize all "datetime.datetime" values.
    """  # noqa: E501
    value: Any
    return np.asarray([value.replace(tzinfo=datetime.timezone.utc).timestamp() for value in data])


def convert_ndarray_float_to_datetime_dtype(data: np.ndarray) -> np.ndarray:
    """
    Convert all elements of 1-D "np.ndarray" argument from "float" type to "datetime.datetime" type objects.

    Note: Converts to "naive" "datetime.datetime" values (assumes "UTC" TimeZone based floating point timestamps).
    """  # noqa: E501
    value: Any
    return np.asarray(
        [datetime.datetime.utcfromtimestamp(value) for value in data]  # noqa: DTZ004
    )


def convert_ndarray_float_to_datetime_tuple(
    data: np.ndarray,
) -> Tuple[datetime.datetime, ...]:
    """
    Convert all elements of 1-D "np.ndarray" argument from "float" type to "datetime.datetime" type tuple elements.

    Note: Converts to "naive" "datetime.datetime" values (assumes "UTC" TimeZone based floating point timestamps).
    """  # noqa: E501
    return tuple(convert_ndarray_float_to_datetime_dtype(data=data).tolist())


def does_ndarray_contain_decimal_dtype(
    data: npt.NDArray,
) -> TypeGuard[npt.NDArray]:
    """
    Determine whether or not all elements of 1-D "np.ndarray" argument are "decimal.Decimal" type objects.
    """  # noqa: E501
    value: Any
    result: bool = any(isinstance(value, decimal.Decimal) for value in data)
    return result


def convert_ndarray_decimal_to_float_dtype(data: np.ndarray) -> np.ndarray:
    """
    Convert all elements of N-D "np.ndarray" argument from "decimal.Decimal" type to "float" type objects.
    """  # noqa: E501
    convert_decimal_to_float_vectorized: Callable[[np.ndarray], np.ndarray] = np.vectorize(
        pyfunc=convert_decimal_to_float
    )
    return convert_decimal_to_float_vectorized(data)


def convert_pandas_series_decimal_to_float_dtype(
    data: pd.Series, inplace: bool = False
) -> pd.Series | None:
    """
    Convert all elements of "pd.Series" argument from "decimal.Decimal" type to "float" type objects "pd.Series" result.
    """  # noqa: E501
    series_data: np.ndarray = data.to_numpy()
    series_data_has_decimal: bool = does_ndarray_contain_decimal_dtype(data=series_data)
    if series_data_has_decimal:
        series_data = convert_ndarray_decimal_to_float_dtype(data=series_data)
        if inplace:
            data.update(pd.Series(series_data))
            return None

        return pd.Series(series_data)

    if inplace:
        return None

    return data


def is_sane_slack_webhook(url: str) -> bool:
    """Really basic sanity checking."""
    if url is None:
        return False

    return url.strip().startswith("https://hooks.slack.com/")


def is_list_of_strings(_list) -> TypeGuard[List[str]]:
    return isinstance(_list, list) and all(isinstance(site, str) for site in _list)


def generate_temporary_table_name(
    default_table_name_prefix: str = "gx_temp_",
    num_digits: int = 8,
) -> str:
    table_name: str = f"{default_table_name_prefix}{str(uuid.uuid4())[:num_digits]}"
    return table_name


def get_sqlalchemy_url(drivername, **credentials):
    if version.parse(sa.__version__) < version.parse("1.4"):
        # Calling URL() deprecated since 1.4, URL.create() should be used instead
        url = sa.engine.url.URL(drivername, **credentials)
    else:
        url = sa.engine.url.URL.create(drivername, **credentials)
    return url


def get_sqlalchemy_selectable(
    selectable: Union[sa.Table, sqlalchemy.Select],
) -> Union[sa.Table, sqlalchemy.Select]:
    """
    Beginning from SQLAlchemy 1.4, a select() can no longer be embedded inside of another select() directly,
    without explicitly turning the inner select() into a subquery first. This helper method ensures that this
    conversion takes place.

    For versions of SQLAlchemy < 1.4 the implicit conversion to a subquery may not always work, so that
    also needs to be handled here, using the old equivalent method.

    https://docs.sqlalchemy.org/en/14/changelog/migration_14.html#change-4617
    """  # noqa: E501
    if sqlalchemy.Select and isinstance(selectable, sqlalchemy.Select):  # type: ignore[truthy-function]
        if version.parse(sa.__version__) >= version.parse("1.4"):
            selectable = selectable.subquery()  # type: ignore[assignment]
        else:
            selectable = selectable.alias()  # type: ignore[assignment]
    return selectable


def get_sqlalchemy_subquery_type():
    """
    Beginning from SQLAlchemy 1.4, `sqlalchemy.sql.Alias` has been deprecated in favor of `sqlalchemy.sql.Subquery`.
    This helper method ensures that the appropriate type is returned.

    https://docs.sqlalchemy.org/en/14/changelog/migration_14.html#change-4617
    """  # noqa: E501
    try:
        return sa.sql.Subquery
    except AttributeError:
        return sa.sql.Alias


def get_sqlalchemy_domain_data(domain_data):
    if version.parse(sa.__version__) < version.parse("1.4"):
        # Implicit coercion of SELECT and SELECT constructs is deprecated since 1.4
        # select(query).subquery() should be used instead
        domain_data = sa.select(sa.text("*")).select_from(domain_data)
    # engine.get_domain_records returns a valid select object;
    # calling fetchall at execution is equivalent to a SELECT *
    return domain_data


def import_make_url():
    """
    Beginning from SQLAlchemy 1.4, make_url is accessed from sqlalchemy.engine; earlier versions must
    still be accessed from sqlalchemy.engine.url to avoid import errors.
    """  # noqa: E501
    if version.parse(sa.__version__) < version.parse("1.4"):
        make_url = sqlalchemy.url.make_url
    else:
        make_url = sqlalchemy.engine.make_url

    return make_url


def get_clickhouse_sqlalchemy_potential_type(type_module, type_) -> Any:
    ch_type = type_
    if type(ch_type) is str:
        if type_.lower() in ("decimal", "decimaltype()"):
            ch_type = type_module.types.Decimal
        elif type_.lower() in ("fixedstring"):
            ch_type = type_module.types.String
        else:
            ch_type = type_module.ClickHouseDialect()._get_column_type("", ch_type)

    if hasattr(ch_type, "nested_type"):
        ch_type = type(ch_type.nested_type)
    if not inspect.isclass(ch_type):
        ch_type = type(ch_type)
    return ch_type


def get_pyathena_potential_type(type_module, type_) -> str:
    if version.parse(type_module.pyathena.__version__) >= version.parse("2.5.0"):
        # introduction of new column type mapping in 2.5
        potential_type = type_module.AthenaDialect()._get_column_type(type_)
    else:
        if type_ == "string":
            type_ = "varchar"
        # < 2.5 column type mapping
        potential_type = type_module._TYPE_MAPPINGS.get(type_)

    return potential_type


def get_trino_potential_type(type_module: ModuleType, type_: str) -> object:
    """
    Leverage on Trino Package to return sqlalchemy sql type
    """
    # noinspection PyUnresolvedReferences
    potential_type = type_module.parse_sqltype(type_)
    return potential_type


def pandas_series_between_inclusive(series: pd.Series, min_value: int, max_value: int) -> pd.Series:
    """
    As of Pandas 1.3.0, the 'inclusive' arg in between() is an enum: {"left", "right", "neither", "both"}
    """  # noqa: E501
    metric_series: pd.Series
    if version.parse(pd.__version__) >= version.parse("1.3.0"):
        metric_series = series.between(min_value, max_value, inclusive="both")
    else:
        metric_series = series.between(min_value, max_value)

    return metric_series


ToBool: TypeAlias = bool
ToFloat: TypeAlias = Union[float, np.floating]
ToInt: TypeAlias = Union[int, np.integer]
ToStr: TypeAlias = Union[
    str, bytes, slice, uuid.UUID, datetime.date, datetime.datetime, np.datetime64
]

ToList: TypeAlias = Union[list, set, tuple, "npt.NDArray", pd.Index, pd.Series]
ToDict: TypeAlias = Union[
    dict,
    "CommentedMap",
    pd.DataFrame,
    SerializableDictDot,
    SerializableDotDict,
    pydantic.BaseModel,
]

JSONConvertable: TypeAlias = Union[
    ToDict, ToList, ToStr, ToInt, ToFloat, ToBool, ToBool, None  # noqa: PYI016
]


@overload
def convert_to_json_serializable(
    data: ToDict,
) -> dict: ...


@overload
def convert_to_json_serializable(
    data: ToList,
) -> list: ...


@overload
def convert_to_json_serializable(
    data: ToBool,
) -> bool: ...


@overload
def convert_to_json_serializable(
    data: ToFloat,
) -> float: ...


@overload
def convert_to_json_serializable(
    data: ToInt,
) -> int: ...


@overload
def convert_to_json_serializable(
    data: ToStr,
) -> str: ...


@overload
def convert_to_json_serializable(
    data: None,
) -> None: ...


def convert_to_json_serializable(  # noqa: C901, PLR0911, PLR0912
    data: JSONConvertable,
) -> JSONValues:
    """Converts an object to one that is JSON-serializable.

    WARNING, data may be converted in place.

    Args:
        data: an object to convert to a JSON-serializable object

    Returns:
        A JSON-serializable object. For example:

        >>> convert_to_json_serializable(1)
        1

        >>> convert_to_json_serializable("hello")
        "hello"

        >>> convert_to_json_serializable(Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]))
        "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"

    Raises:
        TypeError: A non-JSON-serializable field was found.
    """
    if isinstance(data, pydantic.BaseModel):
        return json.loads(data.json())

    if isinstance(data, (SerializableDictDot, SerializableDotDict)):
        return data.to_json_dict()

    # Handling "float(nan)" separately is required by Python-3.6 and Pandas-0.23 versions.
    if isinstance(data, float) and np.isnan(data):
        return None

    if isinstance(data, (str, int, float, bool)):
        # No problem to encode json
        return data

    if isinstance(data, range):
        return list(data)

    if isinstance(data, dict):
        new_dict = {}
        for key in data:
            # A pandas index can be numeric, and a dict key can be numeric, but a json key must be a string  # noqa: E501
            new_dict[str(key)] = convert_to_json_serializable(data[key])

        return new_dict

    if isinstance(data, (list, tuple, set)):
        new_list: List[JSONValues] = []
        for val in data:
            new_list.append(convert_to_json_serializable(val))

        return new_list

    if isinstance(data, (np.ndarray, pd.Index)):
        # test_obj[key] = test_obj[key].tolist()
        # If we have an array or index, convert it first to a list--causing coercion to float--and then round  # noqa: E501
        # to the number of digits for which the string representation will equal the float representation  # noqa: E501
        return [convert_to_json_serializable(x) for x in data.tolist()]

    if isinstance(data, np.int64):
        return int(data)

    if isinstance(data, np.float64):
        return float(data)

    if isinstance(data, (datetime.datetime, datetime.date)):
        return data.isoformat()

    if isinstance(data, (np.datetime64)):
        return np.datetime_as_string(data)

    if isinstance(data, uuid.UUID):
        return str(data)

    if isinstance(data, bytes):
        return str(data)

    if isinstance(data, slice):
        return str(data)

    if isinstance(data, pathlib.PurePath):
        return str(data)

    # noinspection PyTypeChecker
    if Polygon and isinstance(data, (Point, Polygon, MultiPolygon, LineString)):
        return str(data)

    # Use built in base type from numpy, https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    # https://github.com/numpy/numpy/pull/9505
    if np.issubdtype(type(data), np.bool_):
        return bool(data)

    if np.issubdtype(type(data), np.integer) or np.issubdtype(type(data), np.uint):
        return int(data)  # type: ignore[arg-type] # could be None

    if np.issubdtype(type(data), np.floating):
        # Note: Use np.floating to avoid FutureWarning from numpy
        return float(round(data, sys.float_info.dig))  # type: ignore[arg-type] # could be None

    # Note: This clause has to come after checking for np.ndarray or we get:
    #      `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`  # noqa: E501
    if data is None:
        # No problem to encode json
        return data

    try:
        if not isinstance(data, list) and pd.isna(data):  # type: ignore[arg-type]
            # pd.isna is functionally vectorized, but we only want to apply this to single objects
            # Hence, why we test for `not isinstance(list)`
            return None
    except TypeError:
        pass
    except ValueError:
        pass

    if isinstance(data, pd.Series):
        # Converting a series is tricky since the index may not be a string, but all json
        # keys must be strings. So, we use a very ugly serialization strategy
        index_name = data.index.name or "index"
        value_name = data.name or "value"
        return [
            {
                index_name: convert_to_json_serializable(idx),  # type: ignore[call-overload]
                value_name: convert_to_json_serializable(val),  # type: ignore[dict-item]
            }
            for idx, val in data.items()
        ]

    if isinstance(data, pd.DataFrame):
        return convert_to_json_serializable(data.to_dict(orient="records"))

    if pyspark.DataFrame and isinstance(data, pyspark.DataFrame):  # type: ignore[truthy-function]
        # using StackOverflow suggestion for converting pyspark df into dictionary
        # https://stackoverflow.com/questions/43679880/pyspark-dataframe-to-dictionary-columns-as-keys-and-list-of-column-values-ad-di
        return convert_to_json_serializable(dict(zip(data.schema.names, zip(*data.collect()))))

    # SQLAlchemy serialization
    if LegacyRow and isinstance(data, LegacyRow):
        return dict(data)

    # sqlalchemy text for SqlAlchemy 2 compatibility
    if sqlalchemy.TextClause and isinstance(data, sqlalchemy.TextClause):  # type: ignore[truthy-function]
        return str(data)

    if Row and isinstance(data, Row):  # type: ignore[truthy-function]
        return str(data)

    if isinstance(data, decimal.Decimal):
        return convert_decimal_to_float(d=data)

    from great_expectations.core.run_identifier import RunIdentifier

    if isinstance(data, RunIdentifier):
        return data.to_json_dict()

    # PySpark schema serialization
    if pyspark.types and isinstance(data, pyspark.types.StructType):
        return dict(data.jsonValue())

    if sqlalchemy.Connection and isinstance(data, sqlalchemy.Connection):  # type: ignore[truthy-function]
        # Connection is a module, which is non-serializable. Return module name instead.
        return "sqlalchemy.engine.base.Connection"

    if isinstance(data, RenderedContent):
        return data.to_json_dict()

    if isinstance(data, re.Pattern):
        return data.pattern

    # Unable to serialize (unrecognized data type).
    raise TypeError(f"{data!s} is of type {type(data).__name__} which cannot be serialized.")  # noqa: TRY003


def ensure_json_serializable(data: Any) -> None:  # noqa: C901, PLR0911, PLR0912
    """
    Helper function to convert an object to one that is json serializable
    Args:
        data: an object to attempt to convert a corresponding json-serializable object
    Warning:
        test_obj may also be converted in place.
    """
    if isinstance(data, pydantic.BaseModel):
        return

    if isinstance(data, (SerializableDictDot, SerializableDotDict)):
        return

    if isinstance(data, ((str,), (int,), float, bool)):
        # No problem to encode json
        return

    if isinstance(data, dict):
        for key in data:
            str(key)  # key must be cast-able to string
            ensure_json_serializable(data[key])

        return

    if isinstance(data, (list, tuple, set)):
        for val in data:
            ensure_json_serializable(val)
        return

    if isinstance(data, (np.ndarray, pd.Index)):
        # test_obj[key] = test_obj[key].tolist()
        # If we have an array or index, convert it first to a list--causing coercion to float--and then round  # noqa: E501
        # to the number of digits for which the string representation will equal the float representation  # noqa: E501
        _ = [ensure_json_serializable(x) for x in data.tolist()]  # type: ignore[func-returns-value]
        return

    if isinstance(data, (datetime.datetime, datetime.date)):
        return

    if isinstance(data, pathlib.PurePath):
        return

    # Use built in base type from numpy, https://docs.scipy.org/doc/numpy-1.13.0/user/basics.types.html
    # https://github.com/numpy/numpy/pull/9505
    if np.issubdtype(type(data), np.bool_):
        return

    if np.issubdtype(type(data), np.integer) or np.issubdtype(type(data), np.uint):
        return

    if np.issubdtype(type(data), np.floating):
        # Note: Use np.floating to avoid FutureWarning from numpy
        return

    # Note: This clause has to come after checking for np.ndarray or we get:
    #      `ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`  # noqa: E501
    if data is None:
        # No problem to encode json
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

    if isinstance(data, pd.Series):
        # Converting a series is tricky since the index may not be a string, but all json
        # keys must be strings. So, we use a very ugly serialization strategy
        index_name = data.index.name or "index"
        value_name = data.name or "value"
        _ = [
            {
                index_name: ensure_json_serializable(idx),  # type: ignore[func-returns-value]
                value_name: ensure_json_serializable(val),  # type: ignore[func-returns-value]
            }
            for idx, val in data.items()
        ]
        return

    if pyspark.DataFrame and isinstance(data, pyspark.DataFrame):  # type: ignore[truthy-function] # ensure pyspark is installed
        # using StackOverflow suggestion for converting pyspark df into dictionary
        # https://stackoverflow.com/questions/43679880/pyspark-dataframe-to-dictionary-columns-as-keys-and-list-of-column-values-ad-di
        return ensure_json_serializable(dict(zip(data.schema.names, zip(*data.collect()))))

    if isinstance(data, pd.DataFrame):
        return ensure_json_serializable(data.to_dict(orient="records"))

    if isinstance(data, decimal.Decimal):
        return

    from great_expectations.core.run_identifier import RunIdentifier

    if isinstance(data, RunIdentifier):
        return

    if sqlalchemy.TextClause and isinstance(data, sqlalchemy.TextClause):  # type: ignore[truthy-function]
        # TextClause is handled manually by convert_to_json_serializable()
        return

    if sqlalchemy.Connection and isinstance(data, sqlalchemy.Connection):  # type: ignore[truthy-function]
        # Connection module is handled manually by convert_to_json_serializable()
        return

    raise InvalidExpectationConfigurationError(  # noqa: TRY003
        f"{data!s} is of type {type(data).__name__} which cannot be serialized to json"
    )
