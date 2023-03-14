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
    Union,
)

from typing_extensions import Final, TypeAlias

from great_expectations.data_context import (
    AbstractDataContext as GXDataContext,  # noqa: TCH001
)

if TYPE_CHECKING:
    import pathlib
    from logging import Logger

    import pydantic

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
    from great_expectations.datasource.fluent.config_str import ConfigStr
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
    def add_pandas(
        self,
        name: str,
    ) -> PandasDatasource: ...
    def update_pandas(
        self,
        name: str,
    ) -> PandasDatasource: ...
    def add_or_update_pandas(
        self,
        name: str,
    ) -> PandasDatasource: ...
    def delete_pandas(
        self,
        name: str,
    ) -> None: ...
    def add_pandas_filesystem(
        self,
        name: str,
        *,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> PandasFilesystemDatasource: ...
    def update_pandas_filesystem(
        self,
        name: str,
        *,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> PandasFilesystemDatasource: ...
    def add_or_update_pandas_filesystem(
        self,
        name: str,
        *,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> PandasFilesystemDatasource: ...
    def delete_pandas_filesystem(
        self,
        name: str,
    ) -> None: ...
    def add_pandas_s3(
        self,
        name: str,
        *,
        bucket: str = ...,
        boto3_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> PandasS3Datasource: ...
    def update_pandas_s3(
        self,
        name: str,
        *,
        bucket: str = ...,
        boto3_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> PandasS3Datasource: ...
    def add_or_update_pandas_s3(
        self,
        name: str,
        *,
        bucket: str = ...,
        boto3_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> PandasS3Datasource: ...
    def delete_pandas_s3(
        self,
        name: str,
    ) -> None: ...
    def add_pandas_gcs(
        self,
        name: str,
        *,
        bucket_or_name: str = ...,
        gcs_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> PandasGoogleCloudStorageDatasource: ...
    def update_pandas_gcs(
        self,
        name: str,
        *,
        bucket_or_name: str = ...,
        gcs_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> PandasGoogleCloudStorageDatasource: ...
    def add_or_update_pandas_gcs(
        self,
        name: str,
        bucket_or_name: str = ...,
        gcs_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> PandasGoogleCloudStorageDatasource: ...
    def delete_pandas_gcs(
        self,
        name: str,
    ) -> None: ...
    def add_pandas_abs(
        self,
        name: str,
        *,
        azure_options: dict[str, Any] = ...,
    ) -> PandasAzureBlobStorageDatasource: ...
    def update_pandas_abs(
        self,
        name: str,
        *,
        azure_options: dict[str, Any] = ...,
    ) -> PandasAzureBlobStorageDatasource: ...
    def add_or_update_pandas_abs(
        self,
        name: str,
        *,
        azure_options: dict[str, Any] = ...,
    ) -> PandasAzureBlobStorageDatasource: ...
    def delete_pandas_abs(
        self,
        name: str,
    ) -> None: ...
    def add_sql(
        self,
        name: str,
        *,
        connection_string: Union[ConfigStr, str] = ...,
    ) -> SQLDatasource: ...
    def update_sql(
        self,
        name: str,
        *,
        connection_string: Union[ConfigStr, str] = ...,
    ) -> SQLDatasource: ...
    def add_or_update_sql(
        self,
        name: str,
        *,
        connection_string: Union[ConfigStr, str] = ...,
    ) -> SQLDatasource: ...
    def delete_sql(
        self,
        name: str,
    ) -> None: ...
    def add_postgres(
        self,
        name: str,
        *,
        connection_string: Union[ConfigStr, pydantic.networks.PostgresDsn, str] = ...,
    ) -> PostgresDatasource: ...
    def update_postgres(
        self,
        name: str,
        *,
        connection_string: Union[ConfigStr, pydantic.networks.PostgresDsn, str] = ...,
    ) -> PostgresDatasource: ...
    def add_or_update_postgres(
        self,
        name: str,
        *,
        connection_string: Union[ConfigStr, pydantic.networks.PostgresDsn, str] = ...,
    ) -> PostgresDatasource: ...
    def delete_postgres(
        self,
        name: str,
    ) -> None: ...
    def add_spark_filesystem(
        self,
        name: str,
        *,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> SparkFilesystemDatasource: ...
    def update_spark_filesystem(
        self,
        name: str,
        *,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> SparkFilesystemDatasource: ...
    def add_or_update_spark_filesystem(
        self,
        name: str,
        *,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> SparkFilesystemDatasource: ...
    def delete_spark_filesystem(
        self,
        name: str,
    ) -> None: ...
    def add_spark_s3(
        self,
        name: str,
        *,
        bucket: str = ...,
        boto3_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> SparkS3Datasource: ...
    def update_spark_s3(
        self,
        name: str,
        *,
        bucket: str = ...,
        boto3_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> SparkS3Datasource: ...
    def add_or_update_spark_s3(
        self,
        name: str,
        *,
        bucket: str = ...,
        boto3_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> SparkS3Datasource: ...
    def delete_spark_s3(
        self,
        name: str,
    ) -> None: ...
    def add_spark_gcs(
        self,
        name: str,
        *,
        bucket_or_name: str = ...,
        gcs_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> SparkGoogleCloudStorageDatasource: ...
    def update_spark_gcs(
        self,
        name: str,
        *,
        bucket_or_name: str = ...,
        gcs_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> SparkGoogleCloudStorageDatasource: ...
    def add_or_update_spark_gcs(
        self,
        name: str,
        *,
        bucket_or_name: str = ...,
        gcs_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> SparkGoogleCloudStorageDatasource: ...
    def delete_spark_gcs(
        self,
        name: str,
    ) -> None: ...
    def add_spark_abs(
        self,
        name: str,
        *,
        azure_options: dict[str, Any] = ...,
    ) -> SparkAzureBlobStorageDatasource: ...
    def update_spark_abs(
        self,
        name: str,
        *,
        azure_options: dict[str, Any] = ...,
    ) -> SparkAzureBlobStorageDatasource: ...
    def add_or_update_spark_abs(
        self,
        name: str,
        *,
        azure_options: dict[str, Any] = ...,
    ) -> SparkAzureBlobStorageDatasource: ...
    def delete_spark_abs(
        self,
        name: str,
    ) -> None: ...
    def add_sqlite(
        self,
        name: str,
        *,
        connection_string: Union[ConfigStr, SqliteDsn, str] = ...,
    ) -> SqliteDatasource: ...
    def update_sqlite(
        self,
        name: str,
        *,
        connection_string: Union[ConfigStr, SqliteDsn, str] = ...,
    ) -> SqliteDatasource: ...
    def add_or_update_sqlite(
        self,
        name: str,
        *,
        connection_string: Union[ConfigStr, SqliteDsn, str] = ...,
    ) -> SqliteDatasource: ...
    def delete_sqlite(
        self,
        name: str,
    ) -> None: ...

def _iter_all_registered_types() -> Generator[
    tuple[str, Type[Datasource] | Type[DataAsset]], None, None
]: ...
