import inspect
from pprint import pformat as pf
from typing import Callable, List, Sequence, Tuple

import pandas as pd
from typing_extensions import TypeAlias, reveal_type

DataFrameFactoryFn: TypeAlias = Callable[..., pd.DataFrame]


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
        print(f"  {name} -> {sig.return_annotation}\n{sig}\n")
        signatures.append(sig)
    return signatures


def _to_pydantic_fields(signatures: Sequence[inspect.Signature]):
    for sig in signatures:
        for param_name, param in sig.parameters.items():
            print(type(param), param)


if __name__ == "__main__":
    io_methods = _extract_io_methods()[1:2]
    print(f"  IO Methods\n{pf(io_methods)}\n")

    io_method_sigs = _extract_io_signatures(io_methods)
    # print(f"  IO Method Signatures\n{pf(io_method_sigs)}")

    fields = _to_pydantic_fields(io_method_sigs)
