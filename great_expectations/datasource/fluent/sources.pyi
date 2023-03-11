from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Generator,
    List,
    NamedTuple,
    Type,
)

from typing_extensions import Final, TypeAlias

from great_expectations.data_context import (
    AbstractDataContext as GXDataContext,  # noqa: TCH001
)

if TYPE_CHECKING:
    import pathlib
    from logging import Logger

    import pydantic
    from pydantic.networks import PostgresDsn

    from great_expectations.datasource.fluent import (
        PandasAzureBlobStorageDatasource,
        PandasDatasource,
        PandasFilesystemDatasource,
        PandasGoogleCloudStorageDatasource,
        PandasS3Datasource,
        PostgresDatasource,
        SparkAzureBlobStorageDatasource,
        SparkFilesystemDatasource,
        SparkGoogleCloudStorageDatasource,
        SparkS3Datasource,
        SQLDatasource,
        SqliteDatasource,
    )
    from great_expectations.datasource.fluent.interfaces import (
        DataAsset,
        Datasource,
    )
    from great_expectations.datasource.fluent.sqlite_datasource import SqliteDsn

SourceFactoryFn: TypeAlias = Callable[..., "Datasource"]
logger: Logger
DEFAULT_PANDAS_DATASOURCE_NAME: Final[str]
DEFAULT_PANDAS_DATA_ASSET_NAME: Final[str]

class DefaultPandasDatasourceError(Exception): ...
class TypeRegistrationError(TypeError): ...

class _FieldDetails(NamedTuple):
    default_value: Any
    type_annotation: Type

def _get_field_details(
    model: Type[pydantic.BaseModel], field_name: str
) -> _FieldDetails: ...

class _SourceFactories:
    type_lookup: ClassVar
    def __init__(self, data_context: GXDataContext) -> None: ...
    @classmethod
    def register_datasource(
        cls,
        ds_type: Type[Datasource],
    ) -> None: ...
    @property
    def pandas_default(self) -> PandasDatasource: ...
    @property
    def factories(self) -> List[str]: ...
    def __getattr__(self, attr_name: str): ...
    def __dir__(self) -> List[str]: ...
    def add_pandas(self, name: str) -> PandasDatasource: ...
    def add_pandas_abs(
        self, name: str, *, azure_options: dict[str, Any] | None = None
    ) -> PandasAzureBlobStorageDatasource: ...
    def add_pandas_filesystem(
        self,
        name: str,
        *,
        base_directory: pathlib.Path,
        data_context_root_directory: pathlib.Path | None = None,
    ) -> PandasFilesystemDatasource: ...
    def add_pandas_gcs(
        self,
        name: str,
        *,
        bucket_or_name: str,
        gcs_options: dict[str, Any] | None = None,
    ) -> PandasGoogleCloudStorageDatasource: ...
    def add_pandas_s3(
        self, name: str, *, bucket: str, boto3_options: dict[str, Any] | None = None
    ) -> PandasS3Datasource: ...
    def add_postgres(
        self, name: str, *, connection_string: PostgresDsn | str
    ) -> PostgresDatasource: ...
    def add_spark_abs(
        self, name: str, *, azure_options: dict[str, Any] | None = None
    ) -> SparkAzureBlobStorageDatasource: ...
    def add_spark_filesystem(
        self,
        name: str,
        *,
        base_directory: pathlib.Path,
        data_context_root_directory: pathlib.Path | None = None,
    ) -> SparkFilesystemDatasource: ...
    def add_spark_gcs(
        self,
        name: str,
        *,
        bucket_or_name: str,
        gcs_options: dict[str, Any] | None = None,
    ) -> SparkGoogleCloudStorageDatasource: ...
    def add_spark_s3(
        self, name: str, *, bucket: str, boto3_options: dict[str, Any] | None = None
    ) -> SparkS3Datasource: ...
    def add_sql(self, name: str, *, connection_string: str) -> SQLDatasource: ...
    def add_sqlite(
        self, name: str, *, connection_string: SqliteDsn | str
    ) -> SqliteDatasource: ...

def _iter_all_registered_types() -> Generator[
    tuple[str, Type[Datasource] | Type[DataAsset]], None, None
]: ...
