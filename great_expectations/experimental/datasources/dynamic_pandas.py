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
from pandas._typing import CompressionOptions, CSVEngine, IndexLabel, StorageOptions
from pydantic import FilePath

# from pydantic.typing import resolve_annotations
from typing_extensions import Final, Literal, TypeAlias

from great_expectations.experimental.datasources.interfaces import DataAsset

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


FIELD_SUBSTITUTIONS: Final[Dict[str, Dict[str, _FieldSpec]]] = {
    # CSVAsset
    "filepath_or_buffer": {"path": _FieldSpec(pathlib.Path, ...)},
    # JSONAsset
    "path_or_buf": {"path": _FieldSpec(pathlib.Path, ...)},
    # misc
    "filepath": {"path": _FieldSpec(pathlib.Path, ...)},
    "dtype": {"dtype": _FieldSpec(Optional[dict], None)},
    "dialect": {"dialect": _FieldSpec(Optional[str], None)},
    "usecols": {"usecols": _FieldSpec(Union[int, str, Sequence[int], None], None)},
    "skiprows": {"skiprows": _FieldSpec(Union[Sequence[int], int, None], None)},
}

_METHOD_TO_CLASS_NAME_MAPPINGS: Final[Dict[str, str]] = {
    "csv": "CSVAsset",
    "json": "JSONAsset",
    "html": "HTMLAsset",
}

_TYPE_REF_LOCALS: Final[Dict[str, Type]] = {
    "Literal": Literal,
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

    union_parts = annotation.split(" | ")
    str_to_eval: str
    # TODO: use eval'ed types and use subclass check
    for type_str in union_parts:
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
                type=type_, default_value=_get_default_value(param)  # type: ignore[arg-type]
            )

    return fields_dict


M = TypeVar("M", bound=DataAsset)


def _create_pandas_asset_model(
    model_name: str,
    model_base: Type[M],
    type_field: Tuple[Union[Type, str], str],
    fields_dict: Dict[str, _FieldSpec],
) -> Type[M]:
    """https://docs.pydantic.dev/usage/models/#dynamic-model-creation"""
    model = pydantic.create_model(model_name, __base__=model_base, type=type_field, **fields_dict)  # type: ignore[call-overload] # FieldSpec is a tuple
    return model


def _generate_data_asset_models(
    base_model_class: Type[M], whitelist: Optional[Sequence[str]] = None
) -> Dict[str, Type[M]]:
    io_methods = _extract_io_methods(whitelist)
    io_method_sigs = _extract_io_signatures(io_methods)

    models: Dict[str, Type[M]] = {}
    for signature_tuple in io_method_sigs:

        fields = _to_pydantic_fields(signature_tuple)

        type_name = signature_tuple.name.split("read_")[1]
        model_name = _METHOD_TO_CLASS_NAME_MAPPINGS.get(
            type_name, f"{type_name.capitalize()}Asset"
        )

        try:
            model = _create_pandas_asset_model(
                model_name=model_name,
                model_base=base_model_class,
                type_field=(f"Literal['{type_name}']", type_name),
                fields_dict=fields,
            )
            logger.debug(f"{model_name}\n{pf(fields)}")
        except NameError as err:
            # TODO: sql_table has a `schema` param that is a pydantic reserved attribute.
            # Solution is to use an alias field.
            logger.debug(f"{model_name} - {type(err).__name__}:{err}")
            continue
        models[type_name] = model
        model.update_forward_refs(**_TYPE_REF_LOCALS)

    logger.debug(f"Needs extra handling\n{pf(dict(NEED_SPECIAL_HANDLING))}")
    logger.debug(f"No Annotation\n{FIELD_SKIPPED_NO_ANNOTATION}")
    return models
