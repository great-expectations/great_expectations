from __future__ import annotations

import enum
import functools
import inspect
import logging
import re
import warnings
from collections import defaultdict
from pprint import pformat as pf
from typing import (
    Any,
    Callable,
    Dict,
    Hashable,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import pandas as pd
import pydantic
from packaging.version import Version
from pydantic import Field, FilePath

# from pydantic.typing import resolve_annotations
from typing_extensions import Final, Literal, TypeAlias

from great_expectations.experimental.datasources.interfaces import (
    DataAsset,  # noqa: TCH001
)

try:
    # https://github.com/pandas-dev/pandas/blob/main/pandas/_typing.py
    from pandas._typing import CompressionOptions, CSVEngine, IndexLabel, StorageOptions
except ImportError:
    # Types may not exist on earlier version of pandas (current min ver is v.1.1.0)
    # https://github.com/pandas-dev/pandas/blob/v1.1.0/pandas/_typing.py
    CompressionDict = Dict[str, Any]
    CompressionOptions = Optional[
        Union[
            Literal["infer", "gzip", "bz2", "zip", "xz", "zstd", "tar"], CompressionDict
        ]
    ]
    CSVEngine = Literal["c", "python", "pyarrow", "python-fwf"]
    IndexLabel = Union[Hashable, Sequence[Hashable]]
    StorageOptions = Optional[Dict[str, Any]]

try:
    from pandas._libs.lib import _NoDefault
except ImportError:

    class _NoDefault(enum.Enum):  # type: ignore[no-redef]
        no_default = "NO_DEFAULT"


logger = logging.getLogger(__file__)

PANDAS_VERSION: float = float(
    f"{Version(pd.__version__).major}.{Version(pd.__version__).minor}"
)

DataFrameFactoryFn: TypeAlias = Callable[..., pd.DataFrame]

# sentinel values
UNSUPPORTED_TYPE: Final = object()

CAN_HANDLE: Final[Set[str]] = {
    # builtins
    "str",
    "int",
    "list",
    "list[str]",
    "set",
    "tuple",
    "dict",
    "dict[str, str]",
    "dict[str, list[str]]",
    "dict[str, Any]",
    "bool",
    "None",
    # typing
    "Hashable",
    "Sequence[Hashable]",
    "Sequence[tuple[int, int]]",
    "Sequence[Hashable]",
    "Sequence[str]",
    "Sequence[int]",
    "Sequence[tuple[int, int]]",
    "Literal['infer']",
    "Literal[False]",
    "Literal[True]",
    "Literal['high', 'legacy']",
    "Literal['frame', 'series']",
    "Literal['xlrd', 'openpyxl', 'odf', 'pyxlsb']",
    "Literal[None, 'header', 'footer', 'body', 'all']",
    "Iterable[object]",
    "Iterable[Hashable]",
    # other
    "Pattern",  # re
    "Path",  # pathlib
    "FilePath",  # pydantic
    # pandas
    "DtypeArg",
    "CSVEngine",
    "IndexLabel",
    "CompressionOptions",
    "StorageOptions",
}

NEED_SPECIAL_HANDLING: Dict[str, Set[str]] = defaultdict(set)
FIELD_SKIPPED_UNSUPPORTED_TYPE: Set[str] = set()
FIELD_SKIPPED_NO_ANNOTATION: Set[str] = set()


class _SignatureTuple(NamedTuple):
    name: str
    signature: inspect.Signature
    docstring: str = ""


class _FieldSpec(NamedTuple):
    # mypy doesn't consider Optional[SOMETHING] or Union[SOMETHING] a type. So what is it?
    type: Type
    default_value: object  # ... for required value


@functools.lru_cache(maxsize=64)
def _replace_builtins(input_: str | type) -> str | type:
    if not isinstance(input_, str):
        return input_
    return input_.replace("list", "List").replace("dict", "Dict")


FIELD_SUBSTITUTIONS: Final[Dict[str, Dict[str, _FieldSpec]]] = {
    # SQLTable
    "schema": {
        "schema_name": _FieldSpec(
            Optional[str],  # type: ignore[arg-type]
            Field(
                None,
                description="'schema_name' on the instance model."
                " Will be passed to pandas reader method as 'schema'",
                alias="schema",
            ),
        )
    },
    # misc
    "dtype": {"dtype": _FieldSpec(Optional[dict], None)},  # type: ignore[arg-type]
    "dialect": {"dialect": _FieldSpec(Optional[str], None)},  # type: ignore[arg-type]
    "usecols": {"usecols": _FieldSpec(Union[int, str, Sequence[int], None], None)},  # type: ignore[arg-type]
    "skiprows": {"skiprows": _FieldSpec(Union[Sequence[int], int, None], None)},  # type: ignore[arg-type]
    "kwargs": {
        "kwargs": _FieldSpec(
            Optional[dict],  # type: ignore[arg-type]
            Field(
                None,
                description="Extra keyword arguments that will be passed to the reader method",
            ),
        )
    },
}

_METHOD_TO_CLASS_NAME_MAPPINGS: Final[Dict[str, str]] = {
    "csv": "CSVAsset",
    "gbq": "GBQAsset",
    "hdf": "HDFAsset",
    "html": "HTMLAsset",
    "json": "JSONAsset",
    "orc": "ORCAsset",
    "sas": "SASAsset",
    "spss": "SPSSAsset",
    "sql_query": "SQLQueryAsset",
    "sql_table": "SQLTableAsset",
    "stata": "STATAAsset",
    "xml": "XMLAsset",
}

_TYPE_REF_LOCALS: Final[Dict[str, Type]] = {
    "Literal": Literal,  # type: ignore[dict-item]
    "Sequence": Sequence,
    "Hashable": Hashable,
    "Iterable": Iterable,
    "FilePath": FilePath,
    "Pattern": re.Pattern,
    "CSVEngine": CSVEngine,
    "IndexLabel": IndexLabel,
    "CompressionOptions": CompressionOptions,
    "StorageOptions": StorageOptions,
}

# TODO: make these functions a generator pipeline


def _extract_io_methods(
    blacklist: Optional[Sequence[str]] = None,
) -> List[Tuple[str, DataFrameFactoryFn]]:
    # suppress pandas future warnings that may be emitted by collecting
    # pandas io methods
    # Once the context manager exits, the warning filter is removed.
    # Do not remove this context-manager.
    # https://docs.python.org/3/library/warnings.html#temporarily-suppressing-warnings
    with warnings.catch_warnings():
        warnings.simplefilter(action="ignore", category=FutureWarning)

        member_functions = inspect.getmembers(pd, predicate=inspect.isfunction)
    # filter removed
    if blacklist:
        return [
            t
            for t in member_functions
            if t[0] not in blacklist and t[0].startswith("read_")
        ]
    return [t for t in member_functions if t[0].startswith("read_")]


def _extract_io_signatures(
    io_methods: List[Tuple[str, DataFrameFactoryFn]]
) -> List[_SignatureTuple]:
    signatures = []
    for name, method in io_methods:
        sig = inspect.signature(method)
        signatures.append(_SignatureTuple(name, sig, method.__doc__ or ""))
    return signatures


def _get_default_value(
    param: inspect.Parameter,
) -> object:
    if param.default is inspect.Parameter.empty:
        default = ...
    # this is the pandas sentinel value for determining if a parameter has been passed
    # we can treat it as `None` because we only pass down kwargs that have been explicitly
    # set by the user
    elif param.default is _NoDefault.no_default:
        default = None
    else:
        default = param.default
    return default


def _get_annotation_type(param: inspect.Parameter) -> Union[Type, str, object]:
    """
    https://docs.python.org/3/howto/annotations.html#manually-un-stringizing-stringized-annotations
    """
    annotation = param.annotation
    # this section is only needed for when user is running our min supported pandas (1.1)
    # pandas now exclusively uses postponed/str annotations
    if not isinstance(annotation, str):
        logger.debug(f"{param.name} has non-string annotations")
        # `__args__` contains the actual members of a `Union[TYPE_1, TYPE_2]` object
        union_types = getattr(annotation, "__args__", None)
        if union_types and PANDAS_VERSION < 1.2:
            # we could examine these types and only kick out certain blacklisted types
            # but once we drop python 3.7 support our min pandas version will make this
            # unneeded
            return UNSUPPORTED_TYPE
        return annotation

    types: list = []

    union_parts = annotation.split("|")
    str_to_eval: str
    for type_str in union_parts:
        type_str = type_str.strip()

        if type_str in CAN_HANDLE:
            types.append(type_str)
        else:
            NEED_SPECIAL_HANDLING[param.name].add(type_str)
            logger.debug(f"skipping {param.name} type - {type_str}")
            continue
    if not types:
        return UNSUPPORTED_TYPE
    if len(types) > 1:
        str_to_eval = f"Union[{', '.join(types)}]"
    else:
        str_to_eval = types[0]
    return str_to_eval


def _to_pydantic_fields(
    sig_tuple: _SignatureTuple, skip_first_param: bool
) -> Dict[str, _FieldSpec]:
    """
    Extract the parameter details in a structure that can be easily unpacked to
    `pydantic.create_model()` as field arguments
    """
    fields_dict: Dict[str, _FieldSpec] = {}
    all_parameters: Iterator[tuple[str, inspect.Parameter]] = iter(
        sig_tuple.signature.parameters.items()
    )
    if skip_first_param:
        # skip the first parameter as this corresponds to the path/buffer/io field
        next(all_parameters)

    for param_name, param in all_parameters:
        no_annotation: bool = param.annotation is inspect._empty
        if no_annotation:
            logger.debug(f"`{param_name}` has no type annotation")
            FIELD_SKIPPED_NO_ANNOTATION.add(param_name)  # TODO: not skipped
            type_ = Any
        else:
            type_ = _get_annotation_type(param)
            if type_ is UNSUPPORTED_TYPE or type_ == "None":
                logger.debug(f"`{param_name}` has no supported types. Field skipped")
                FIELD_SKIPPED_UNSUPPORTED_TYPE.add(param_name)
                continue

        substitution = FIELD_SUBSTITUTIONS.get(param_name)
        if substitution:
            fields_dict.update(substitution)
        else:
            fields_dict[param_name] = _FieldSpec(
                type=_replace_builtins(type_), default_value=_get_default_value(param)  # type: ignore[arg-type]
            )

    return fields_dict


M = TypeVar("M", bound=Type[DataAsset])


def _create_pandas_asset_model(
    model_name: str,
    model_base: M,
    type_field: Tuple[Union[Type, str], str],
    fields_dict: Dict[str, _FieldSpec],
    extra: pydantic.Extra,
    model_docstring: str = "",
) -> M:
    """https://docs.pydantic.dev/usage/models/#dynamic-model-creation"""
    model = pydantic.create_model(  # type: ignore[call-overload] # FieldSpec is a tuple
        model_name,
        __base__=model_base,
        type=type_field,
        **fields_dict,
    )
    # can't set both __base__ & __config__ when dynamically creating model
    model.__config__.extra = extra
    if model_docstring:
        model.__doc__ = model_docstring

    def _get_reader_method(self) -> str:
        return f"read_{self.type}"

    def _get_reader_options_include(self) -> set[str] | None:
        return None

    setattr(model, "_get_reader_method", _get_reader_method)
    setattr(model, "_get_reader_options_include", _get_reader_options_include)

    return model


def _generate_pandas_data_asset_models(
    base_model_class: M,
    blacklist: Optional[Sequence[str]] = None,
    use_docstring_from_method: bool = False,
    skip_first_param: bool = False,
    type_prefix: Optional[str] = None,
) -> Dict[str, M]:
    io_methods = _extract_io_methods(blacklist)
    io_method_sigs = _extract_io_signatures(io_methods)

    data_asset_models: Dict[str, M] = {}
    for signature_tuple in io_method_sigs:

        # skip the first parameter as this corresponds to the path/buffer/io field
        # paths to specific files are provided by the batch building logic
        fields = _to_pydantic_fields(signature_tuple, skip_first_param=skip_first_param)

        type_name = signature_tuple.name.split("read_")[1]
        model_name = _METHOD_TO_CLASS_NAME_MAPPINGS.get(
            type_name, f"{type_name.capitalize()}Asset"
        )

        # TODO: remove this special case once we have namespace type-lookups
        if type_prefix:
            type_name = type_prefix + type_name
            model_name = type_prefix + model_name

        try:
            asset_model = _create_pandas_asset_model(
                model_name=model_name,
                model_base=base_model_class,
                type_field=(f"Literal['{type_name}']", type_name),
                fields_dict=fields,
                extra=pydantic.Extra.forbid,
                model_docstring=signature_tuple.docstring.partition("\n\nParameters")[0]
                if use_docstring_from_method
                else "",
            )
            logger.debug(f"{model_name}\n{pf(fields)}")
        except NameError as err:
            # TODO: sql_table has a `schema` param that is a pydantic reserved attribute.
            # Solution is to use an alias field.
            logger.info(f"{model_name} - {type(err).__name__}:{err}")
            continue
        except TypeError as err:
            logger.info(
                f"pandas {pd.__version__}  {model_name} could not be created normally - {type(err).__name__}:{err} , skipping"
            )
            logger.info(f"{model_name} fields\n{pf(fields)}")
            continue

        data_asset_models[type_name] = asset_model
        asset_model.update_forward_refs(**_TYPE_REF_LOCALS)

    logger.debug(f"Needs extra handling\n{pf(dict(NEED_SPECIAL_HANDLING))}")
    logger.debug(f"No Annotation\n{FIELD_SKIPPED_NO_ANNOTATION}")
    return data_asset_models
