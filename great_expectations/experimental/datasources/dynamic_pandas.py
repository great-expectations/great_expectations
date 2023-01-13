import inspect
import logging
import sys
from pprint import pformat as pf
from typing import (
    Callable,
    Dict,
    List,
    NamedTuple,
    Sequence,
    Tuple,
    Type,
    Union,
    _eval_type,
)

import pandas as pd
import pydantic
from pydantic.typing import resolve_annotations
from typing_extensions import TypeAlias, reveal_type

logger = logging.getLogger(__file__)

DataFrameFactoryFn: TypeAlias = Callable[..., pd.DataFrame]


class _FieldSpec(NamedTuple):
    type: Type
    default_value: object  # ... for required value


def _public_dir(obj: object) -> List[str]:
    return [x for x in dir(obj) if not x.startswith("_")]


# TODO: make these a generator pipeline


def _extract_io_methods() -> List[Tuple[str, DataFrameFactoryFn]]:
    # TODO: use blacklist/whitelist?
    member_functions = inspect.getmembers(pd.io.api, predicate=inspect.isfunction)
    return [t for t in member_functions if t[0].startswith("read_")]


def _extract_io_signatures(
    io_methods: List[Tuple[str, DataFrameFactoryFn]]
) -> List[inspect.Signature]:
    signatures = []
    for name, method in io_methods:
        sig = inspect.signature(method)
        # print(f"  {name} -> {sig.return_annotation}\n{sig}\n")
        signatures.append(sig)
    return signatures


def _get_default_value(
    param: inspect.Parameter,
) -> object:
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


def _get_annotation_type(param: inspect.Parameter) -> Type:
    """
    https://docs.python.org/3/howto/annotations.html#manually-un-stringizing-stringized-annotations
    """
    # TODO: parse the annotation string
    annotation = param.annotation
    print(type(annotation), annotation)

    union_parts = annotation.split(" | ")
    str_to_eval: str
    if len(union_parts) > 1:
        str_to_eval = f"Union[{', '.join(union_parts)}]"
    else:
        str_to_eval = union_parts[0]
    # print(f"{str_to_eval=}")
    return _evaluate_annotation_str(str_to_eval)


def _to_pydantic_fields(
    signature: inspect.Signature,
) -> Dict[str, _FieldSpec]:
    """
    Extract the parameter details in a structure that can be easily unpacked to
    `pydantic.create_model()` as field arguments
    """
    fields_dict: Dict[str, _FieldSpec] = {}
    for param_name, param in signature.parameters.items():
        # print(type(param), param)

        no_annotation: bool = param.annotation is inspect._empty
        if no_annotation:
            logger.warning(f"`{param_name}` has no type annotation")
            continue

        fields_dict[param_name] = _FieldSpec(
            type=_get_annotation_type(param), default_value=_get_default_value(param)
        )

    return fields_dict


def _create_pandas_asset_model(
    model_name: str, fields_dict: Dict[str, _FieldSpec]
) -> pydantic.BaseModel:
    model = pydantic.create_model(model_name, **fields_dict)
    return model


if __name__ == "__main__":
    io_methods = _extract_io_methods()[1:2]
    print(f"  IO Methods\n{pf(io_methods)}\n")

    io_method_sigs = _extract_io_signatures(io_methods)
    # print(f"  IO Method Signatures\n{pf(io_method_sigs)}")

    fields = _to_pydantic_fields(io_method_sigs[0])
    print(f"  Pydantic Field Specs\n{pf(fields)}")

    model = _create_pandas_asset_model("POCAssetModel", fields)
    print(model)
    # model.update_forward_refs()
    # print(model(filepath_or_buffer=__file__))
