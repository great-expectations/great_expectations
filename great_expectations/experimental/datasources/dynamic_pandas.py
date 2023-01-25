import inspect
import logging
import pathlib
import re
import sys
from collections import defaultdict
from pprint import pformat as pf
from typing import (  # type: ignore[attr-defined]
    Callable,
    Dict,
    Hashable,
    List,
    NamedTuple,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    _eval_type,
)

import pandas as pd
import pydantic
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
    "bool",
    "None",
    # typing
    "Hashable",
    "typing.Sequence[Hashable]",
    "typing.Sequence[str]",
    "typing.Sequence[int]",
    "typing.Sequence[tuple[int, int]]",
    "Literal['infer']",
    "Literal[False]",
    "Literal[True]",
    "Literal['high', 'legacy']",
    "Literal['frame', 'series']",
    "Literal['xlrd', 'openpyxl', 'odf', 'pyxlsb']",
    "Literal[None, 'header', 'footer', 'body', 'all']",
    # other
    "Pattern",  # re
    "Path",  # pathlib
    "FilePath",  # pydantic
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
    "filepath": {"path": _FieldSpec(pathlib.Path, ...)},
}

_METHOD_TO_CLASS_NAME_MAPPINGS: Final[Dict[str, str]] = {
    "csv": "CSVAsset",
    "json": "JSONAsset",
    "html": "HTMLAsset",
}

_TYPE_REF_LOCALS: Final[Dict[str, Type]] = {
    "Literal": Literal,
    # "Sequence": Sequence,
    "Hashable": Hashable,
    "FilePath": FilePath,
    "Pattern": re.Pattern,
}


def _public_dir(obj: object) -> List[str]:
    return [x for x in dir(obj) if not x.startswith("_")]


# TODO: make these a generator pipeline


def _extract_io_methods() -> List[Tuple[str, DataFrameFactoryFn]]:
    # TODO: use blacklist/whitelist?
    member_functions = inspect.getmembers(pd, predicate=inspect.isfunction)
    return [t for t in member_functions if t[0].startswith("read_")]


def _extract_io_signatures(
    io_methods: List[Tuple[str, DataFrameFactoryFn]]
) -> List[_SignatureTuple]:
    signatures = []
    for name, method in io_methods:
        sig = inspect.signature(method)
        # print(f"  {name} -> {sig.return_annotation}\n{sig}\n")
        signatures.append(_SignatureTuple(name, sig))
    return signatures


def _get_default_value(
    param: inspect.Parameter,
) -> object:
    # print(param.name, param.default)
    if param.default is inspect.Parameter.empty:
        default = ...
    else:
        default = param.default
    return default


def _evaluate_annotation_str(annotation_str: str) -> Type:
    base_globals = None
    try:
        module = sys.modules["pandas.io.api"]
    except KeyError as err:
        # happens occasionally, see https://github.com/pydantic/pydantic/issues/2363
        logger.error(f"{err.__class__.__name__}:{err}")
    else:
        base_globals = module.__dict__
    type_ = _eval_type(annotation_str, base_globals, None)
    # assert not isinstance(type_, str), type(type_)
    return type_


def _get_annotation_type(param: inspect.Parameter) -> Union[Type, str, object]:
    """
    https://docs.python.org/3/howto/annotations.html#manually-un-stringizing-stringized-annotations
    """
    # TODO: parse the annotation string
    annotation = param.annotation
    # print(type(annotation), annotation)

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
    # print(f"{str_to_eval=}")
    return _evaluate_annotation_str(str_to_eval)


def _to_pydantic_fields(
    sig_tuple: _SignatureTuple,
) -> Dict[str, _FieldSpec]:
    """
    Extract the parameter details in a structure that can be easily unpacked to
    `pydantic.create_model()` as field arguments
    """
    fields_dict: Dict[str, _FieldSpec] = {}
    for param_name, param in sig_tuple.signature.parameters.items():
        # print(type(param), param)

        no_annotation: bool = param.annotation is inspect._empty
        if no_annotation:
            logger.debug(f"`{param_name}` has no type annotation")
            FIELD_SKIPPED_NO_ANNOTATION.add(param_name)
            continue
        type_ = _get_annotation_type(param)
        if type_ is UNSUPPORTED_TYPE:
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
    base_model_class: Type[M],
) -> Dict[str, Type[M]]:
    io_methods = _extract_io_methods()[:]
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
            print(f"{model_name}\n{pf(fields)}")
        except NameError as err:
            logger.exception(err)
            continue
        models[type_name] = model
        model.update_forward_refs(**_TYPE_REF_LOCALS)

    print(pf(dict(NEED_SPECIAL_HANDLING)))
    return models
