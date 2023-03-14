from __future__ import annotations

from inspect import Parameter, Signature
from typing import Callable, ForwardRef, Type

from typing_extensions import Protocol

from great_expectations.datasource.fluent import Datasource, PandasFilesystemDatasource

# TODO: make this script run the initial mypy stubgen and then layer the dynamic methods
# ontop.


class _Callable(Protocol):
    __signature__: Signature
    __call__: Callable


def _print_method(
    method: _Callable,
    cls: Type,
    method_name: str | None = None,
    default_override: str = "...",
):
    if method_name:
        print(f"def {method_name}(")

    signature: Signature = method.__signature__
    for name, param in signature.parameters.items():
        if param.kind == Parameter.VAR_KEYWORD:
            print(f") -> {cls.__name__}:\n\t...")
            continue

        annotation = param.annotation
        if isinstance(annotation, ForwardRef):
            annotation = annotation.__forward_arg__
        elif getattr(annotation, "__name__", None):
            annotation = annotation.__name__

        if name in ["self"]:
            print(f"\t{name}", end="")
        else:
            print(f"\t{name}: {annotation}", end="")

        if param.kind == Parameter.KEYWORD_ONLY:
            if default_override:
                default = default_override
            elif param.default is Parameter.empty:
                default = "..."
            else:
                default = param.default
                if isinstance(default, str):
                    default = f"'{default}'"
            print(f" = {default}", end="")
        print(",")


def print_add_asset_method_signatures(
    datasource_class: Type[Datasource],
    method_name_template_str: str = "add_{0}_asset",
    default_override: str = "...",
):
    """
    Prints out all of the asset methods for a given datasource in a format that be used
    for defining methods in stub files.
    """
    type_lookup = datasource_class._type_lookup

    for asset_type_name in type_lookup.type_names():
        asset_type = type_lookup[asset_type_name]
        method_name = method_name_template_str.format(asset_type_name)
        method = getattr(datasource_class, method_name)

        _print_method(
            method,
            asset_type,
            method_name=method_name,
            default_override=default_override,
        )


if __name__ == "__main__":
    # replace the provided dataclass as needed
    print_add_asset_method_signatures(PandasFilesystemDatasource)
