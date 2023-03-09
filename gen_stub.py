import inspect
from great_expectations.datasource.fluent import PandasFilesystemDatasource

_DEFAULT_ARGS = """
        self,
        name: str,
        batching_regex: Optional[Union[re.Pattern, str]] = ...,
        glob_directive: str = ...,
        order_by: Optional[SortersDefinition] = ...,
"""

type_lookup = PandasFilesystemDatasource._type_lookup

for asset_type_name in type_lookup.type_names():
    asset_type = type_lookup[asset_type_name]
    method_name = f"add_{asset_type_name}_asset"
    method = getattr(PandasFilesystemDatasource, method_name)

    print(f"def add_{asset_type_name}_asset(")

    # print(method.__signature__)

    # asset_sig_args = str(asset_type.__signature__).split(", ")
    # for i, arg in enumerate(asset_sig_args[5:]):
    #     print(f"{i}\t{arg}")
    # print(asset_sig_args)

    for name, param in method.__signature__.parameters.items():
        print(f"\t{param}", end="")
        print(",")
