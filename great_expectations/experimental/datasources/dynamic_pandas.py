from __future__ import annotations

import enum
import functools
import inspect
import logging
import pathlib
import re
from collections import defaultdict
from pprint import pformat as pf
from typing import (
    Any,
    Callable,
    Dict,
    Hashable,
    Iterable,
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
from pydantic import FilePath

# from pydantic.typing import resolve_annotations
from typing_extensions import Final, Literal, TypeAlias

from great_expectations.experimental.datasources.interfaces import DataAsset

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


class _FieldSpec(NamedTuple):
    type: Type
    default_value: object  # ... for required value


@functools.lru_cache(maxsize=64)
def _replace_builtins(input_: str | type) -> str | type:
    if not isinstance(input_, str):
        return input_
    return input_.replace("list", "List").replace("dict", "Dict")


FIELD_SUBSTITUTIONS: Final[Dict[str, Dict[str, _FieldSpec]]] = {
    # CSVAsset
    "filepath_or_buffer": {"base_directory": _FieldSpec(pathlib.Path, ...)},
    # JSONAsset
    "path_or_buf": {"base_directory": _FieldSpec(pathlib.Path, ...)},
    # misc
    "filepath": {"base_directory": _FieldSpec(pathlib.Path, ...)},
    "dtype": {"dtype": _FieldSpec(Optional[dict], None)},  # type: ignore[arg-type]
    "dialect": {"dialect": _FieldSpec(Optional[str], None)},  # type: ignore[arg-type]
    "usecols": {"usecols": _FieldSpec(Union[int, str, Sequence[int], None], None)},  # type: ignore[arg-type]
    "skiprows": {"skiprows": _FieldSpec(Union[Sequence[int], int, None], None)},  # type: ignore[arg-type]
}

_METHOD_TO_CLASS_NAME_MAPPINGS: Final[Dict[str, str]] = {
    "csv": "CSVAsset",
    "json": "JSONAsset",
    "html": "HTMLAsset",
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
    whitelist: Optional[Sequence[str]] = None,
) -> List[Tuple[str, DataFrameFactoryFn]]:
    member_functions = inspect.getmembers(pd, predicate=inspect.isfunction)
    if whitelist:
        return [t for t in member_functions if t[0] in whitelist]
    return [t for t in member_functions if t[0].startswith("read_")]


def _extract_io_signatures(
    io_methods: List[Tuple[str, DataFrameFactoryFn]]
) -> List[_SignatureTuple]:
    signatures = []
    for name, method in io_methods:
        sig = inspect.signature(method)
        signatures.append(_SignatureTuple(name, sig))
    return signatures


def _get_default_value(
    param: inspect.Parameter,
) -> object:
    if param.default is inspect.Parameter.empty:
        default = ...
    elif param.default is _NoDefault.no_default:
        default = None
    else:
        default = param.default
    return default


def _get_annotation_type(param: inspect.Parameter) -> Union[Type, str, object]:
    """
    https://docs.python.org/3/howto/annotations.html#manually-un-stringizing-stringized-annotations
    """
    # TODO: parse the annotation string
    annotation = param.annotation
    if not isinstance(annotation, str):
        logger.debug(type(annotation), annotation)
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
    if not types:
        return UNSUPPORTED_TYPE
    if len(types) > 1:
        str_to_eval = f"Union[{', '.join(types)}]"
    else:
        str_to_eval = types[0]
    return str_to_eval


def _to_pydantic_fields(
    sig_tuple: _SignatureTuple,
) -> Dict[str, _FieldSpec]:
    """
    Extract the parameter details in a structure that can be easily unpacked to
    `pydantic.create_model()` as field arguments
    """
    fields_dict: Dict[str, _FieldSpec] = {}
    for param_name, param in sig_tuple.signature.parameters.items():

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
) -> M:
    """https://docs.pydantic.dev/usage/models/#dynamic-model-creation"""
    model = pydantic.create_model(model_name, __base__=model_base, type=type_field, **fields_dict)  # type: ignore[call-overload] # FieldSpec is a tuple
    # can't set both __base__ & __config__ when dynamically creating model
    model.__config__.extra = extra
    return model


def _generate_data_asset_models(
    base_model_class: M, whitelist: Optional[Sequence[str]] = None
) -> Dict[str, M]:
    io_methods = _extract_io_methods(whitelist)
    io_method_sigs = _extract_io_signatures(io_methods)

    data_asset_models: Dict[str, M] = {}
    for signature_tuple in io_method_sigs:

        fields = _to_pydantic_fields(signature_tuple)

        type_name = signature_tuple.name.split("read_")[1]
        model_name = _METHOD_TO_CLASS_NAME_MAPPINGS.get(
            type_name, f"{type_name.capitalize()}Asset"
        )

        try:
            asset_model = _create_pandas_asset_model(
                model_name=model_name,
                model_base=base_model_class,
                type_field=(f"Literal['{type_name}']", type_name),
                fields_dict=fields,
                extra=pydantic.Extra.forbid,
            )
            logger.debug(f"{model_name}\n{pf(fields)}")
        except NameError as err:
            # TODO: sql_table has a `schema` param that is a pydantic reserved attribute.
            # Solution is to use an alias field.
            logger.debug(f"{model_name} - {type(err).__name__}:{err}")
            continue
        except TypeError as err:
            # may fail on python <3.10 due to use of builtin as generic
            logger.warning(
                f"{model_name} could not be created normally - {type(err).__name__}:{err}"
            )
            logger.warning(f"{model_name} fields\n{pf(fields)}")
            raise

        data_asset_models[type_name] = asset_model
        asset_model.update_forward_refs(**_TYPE_REF_LOCALS)

    logger.debug(f"Needs extra handling\n{pf(dict(NEED_SPECIAL_HANDLING))}")
    logger.debug(f"No Annotation\n{FIELD_SKIPPED_NO_ANNOTATION}")
    return data_asset_models
