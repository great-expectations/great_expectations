from inspect import Parameter, Signature
from typing import Type

from great_expectations.datasource.fluent import Datasource, PandasFilesystemDatasource


def print_add_asset_method_signatures(datasource_class: Type[Datasource]):
    type_lookup = datasource_class._type_lookup

    for asset_type_name in type_lookup.type_names():
        asset_type = type_lookup[asset_type_name]
        method_name = f"add_{asset_type_name}_asset"
        method = getattr(PandasFilesystemDatasource, method_name)

        print(f"def add_{asset_type_name}_asset(")

        signature: Signature = method.__signature__
        for name, param in signature.parameters.items():
            if param.kind == Parameter.VAR_KEYWORD:
                print(f") -> {asset_type.__name__}\n\t...")
                continue

            annotation = param.annotation
            if getattr(annotation, "__name__", None):
                annotation = annotation.__name__
            print(f"\t{name}: {annotation}", end="")

            if param.kind == Parameter.KEYWORD_ONLY:
                if param.default is Parameter.empty:
                    default = "..."
                else:
                    default = param.default
                    if isinstance(default, str):
                        default = f"'{default}'"
                print(f" = {default}", end="")
            print(",")


print_add_asset_method_signatures(PandasFilesystemDatasource)
