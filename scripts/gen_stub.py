from __future__ import annotations

from inspect import Parameter, Signature
from typing import TYPE_CHECKING, Callable, ForwardRef, Protocol, Type

from great_expectations.datasource.fluent import Datasource, PandasFilesystemDatasource
from great_expectations.datasource.fluent.sources import _SourceFactories

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.type_lookup import TypeLookup

# TODO: make this script run the initial mypy stubgen and then layer the dynamic methods
# ontop.


class _Callable(Protocol):
    __signature__: Signature
    __call__: Callable


def _print_method(  # noqa: PLR0912
    method: _Callable,
    method_name: str | None = None,
    default_override: str = "...",
    return_type_override: str = "",
):
    if method_name:
        print(f"def {method_name}(")

    signature: Signature = method.__signature__
    for name, param in signature.parameters.items():
        # ignore kwargs
        if param.kind == Parameter.VAR_KEYWORD:
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

    if return_type_override:
        return_type = return_type_override
    else:
        return_type = getattr(
            signature.return_annotation, "__name__", signature.return_annotation
        )
    print(f") -> {return_type}:\n\t...")


def print_add_asset_method_signatures(
    datasource_class: Type[Datasource],
    method_name_template_str: str = "add_{0}_asset",
    default_override: str = "...",
):
    """
    Prints out all of the asset methods for a given datasource in a format that be used
    for defining methods in stub files.
    """
    type_lookup: TypeLookup = datasource_class._type_lookup

    for asset_type_name in type_lookup.type_names():
        asset_type = type_lookup[asset_type_name]
        method_name = method_name_template_str.format(asset_type_name)
        method = getattr(datasource_class, method_name)

        _print_method(
            method,
            method_name=method_name,
            default_override=default_override,
            return_type_override=asset_type.__name__,
        )


def print_datasource_crud_signatures(
    source_factories: _SourceFactories,
    method_name_templates: tuple[str, ...] = (
        "add_{0}",
        "update_{0}",
        "add_or_update_{0}",
        "delete_{0}",
    ),
    default_override: str = "...",
):
    """
    Prints out all of the CRUD methods for a given datasource in a format that be used
    for defining methods in stub files.
    """
    datasource_type_lookup = source_factories.type_lookup

    for datasource_name in datasource_type_lookup.type_names():
        for method_name_tmplt in method_name_templates:
            method_name = method_name_tmplt.format(datasource_name)

            _print_method(
                getattr(source_factories, method_name),
                method_name=method_name,
                default_override=default_override,
            )


if __name__ == "__main__":
    # replace the provided dataclass as needed
    print_add_asset_method_signatures(PandasFilesystemDatasource)

    print_datasource_crud_signatures(
        source_factories=_SourceFactories("dummy_context"),  # type: ignore[arg-type]
    )
