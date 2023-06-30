from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Final,
    Generator,
    List,
    NamedTuple,
    Optional,
    Type,
    Union,
    overload,
)

from typing_extensions import TypeAlias

from great_expectations.data_context import (
    AbstractDataContext as GXDataContext,
)
from great_expectations.datasource.fluent.spark_datasource import SparkConfig

if TYPE_CHECKING:
    import pathlib
    from logging import Logger

    import pydantic

    from great_expectations.datasource.fluent import (
        PandasAzureBlobStorageDatasource,
        PandasDatasource,
        PandasDBFSDatasource,
        PandasFilesystemDatasource,
        PandasGoogleCloudStorageDatasource,
        PandasS3Datasource,
        PostgresDatasource,
        SnowflakeDatasource,
        SparkAzureBlobStorageDatasource,
        SparkDatasource,
        SparkDBFSDatasource,
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
    from great_expectations.datasource.fluent.snowflake_datasource import SnowflakeDsn
    from great_expectations.datasource.fluent.sqlite_datasource import SqliteDsn

SourceFactoryFn: TypeAlias = Callable[..., Datasource]
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
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
    ) -> PandasDatasource: ...
    def update_pandas(
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
    ) -> PandasDatasource: ...
    def add_or_update_pandas(
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
    ) -> PandasDatasource: ...
    def delete_pandas(
        self,
        name: str,
    ) -> None: ...
    def add_pandas_filesystem(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> PandasFilesystemDatasource: ...
    def update_pandas_filesystem(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> PandasFilesystemDatasource: ...
    def add_or_update_pandas_filesystem(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> PandasFilesystemDatasource: ...
    def delete_pandas_filesystem(
        self,
        name: str,
    ) -> None: ...
    def add_pandas_dbfs(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> PandasDBFSDatasource: ...
    def update_pandas_dbfs(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> PandasDBFSDatasource: ...
    def add_or_update_pandas_dbfs(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> PandasDBFSDatasource: ...
    def delete_pandas_dbfs(
        self,
        name: str,
    ) -> None: ...
    def add_pandas_s3(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        bucket: str = ...,
        boto3_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> PandasS3Datasource: ...
    def update_pandas_s3(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        bucket: str = ...,
        boto3_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> PandasS3Datasource: ...
    def add_or_update_pandas_s3(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        bucket: str = ...,
        boto3_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> PandasS3Datasource: ...
    def delete_pandas_s3(
        self,
        name: str,
    ) -> None: ...
    def add_pandas_gcs(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        bucket_or_name: str = ...,
        gcs_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> PandasGoogleCloudStorageDatasource: ...
    def update_pandas_gcs(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        bucket_or_name: str = ...,
        gcs_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> PandasGoogleCloudStorageDatasource: ...
    def add_or_update_pandas_gcs(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        bucket_or_name: str = ...,
        gcs_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> PandasGoogleCloudStorageDatasource: ...
    def delete_pandas_gcs(
        self,
        name: str,
    ) -> None: ...
    def add_pandas_abs(
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        azure_options: dict[str, Any] = ...,
    ) -> PandasAzureBlobStorageDatasource: ...
    def update_pandas_abs(
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        azure_options: dict[str, Any] = ...,
    ) -> PandasAzureBlobStorageDatasource: ...
    def add_or_update_pandas_abs(
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        azure_options: dict[str, Any] = ...,
    ) -> PandasAzureBlobStorageDatasource: ...
    def delete_pandas_abs(
        self,
        name: str,
    ) -> None: ...
    def add_sql(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        connection_string: Union[ConfigStr, str] = ...,
        create_temp_table: bool = True,
    ) -> SQLDatasource: ...
    def update_sql(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        connection_string: Union[ConfigStr, str] = ...,
        create_temp_table: bool = True,
    ) -> SQLDatasource: ...
    def add_or_update_sql(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        connection_string: Union[ConfigStr, str] = ...,
        create_temp_table: bool = True,
    ) -> SQLDatasource: ...
    def delete_sql(
        self,
        name: str,
    ) -> None: ...
    def add_postgres(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        connection_string: Union[ConfigStr, pydantic.networks.PostgresDsn, str] = ...,
        create_temp_table: bool = True,
    ) -> PostgresDatasource: ...
    def update_postgres(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        connection_string: Union[ConfigStr, pydantic.networks.PostgresDsn, str] = ...,
        create_temp_table: bool = True,
    ) -> PostgresDatasource: ...
    def add_or_update_postgres(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        connection_string: Union[ConfigStr, pydantic.networks.PostgresDsn, str] = ...,
        create_temp_table: bool = True,
    ) -> PostgresDatasource: ...
    def delete_postgres(
        self,
        name: str,
    ) -> None: ...
    def add_spark(
        self,
        name: str,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
    ) -> SparkDatasource: ...
    def update_spark(
        self,
        name: str,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
    ) -> SparkDatasource: ...
    def add_or_update_spark(
        self,
        name: str,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
    ) -> SparkDatasource: ...
    def delete_spark(
        self,
        name: str,
    ) -> None: ...
    def add_spark_filesystem(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> SparkFilesystemDatasource: ...
    def update_spark_filesystem(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> SparkFilesystemDatasource: ...
    def add_or_update_spark_filesystem(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> SparkFilesystemDatasource: ...
    def delete_spark_filesystem(
        self,
        name: str,
    ) -> None: ...
    def add_spark_dbfs(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> SparkDBFSDatasource: ...
    def update_spark_dbfs(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> SparkDBFSDatasource: ...
    def add_or_update_spark_dbfs(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
        base_directory: pathlib.Path = ...,
        data_context_root_directory: Union[pathlib.Path, None] = ...,
    ) -> SparkDBFSDatasource: ...
    def delete_spark_dbfs(
        self,
        name: str,
    ) -> None: ...
    def add_spark_s3(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
        bucket: str = ...,
        boto3_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> SparkS3Datasource: ...
    def update_spark_s3(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
        bucket: str = ...,
        boto3_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> SparkS3Datasource: ...
    def add_or_update_spark_s3(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
        bucket: str = ...,
        boto3_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> SparkS3Datasource: ...
    def delete_spark_s3(
        self,
        name: str,
    ) -> None: ...
    def add_spark_gcs(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
        bucket_or_name: str = ...,
        gcs_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> SparkGoogleCloudStorageDatasource: ...
    def update_spark_gcs(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
        bucket_or_name: str = ...,
        gcs_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> SparkGoogleCloudStorageDatasource: ...
    def add_or_update_spark_gcs(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
        bucket_or_name: str = ...,
        gcs_options: dict[str, Union[ConfigStr, Any]] = ...,
    ) -> SparkGoogleCloudStorageDatasource: ...
    def delete_spark_gcs(
        self,
        name: str,
    ) -> None: ...
    def add_spark_abs(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
        azure_options: dict[str, Any] = ...,
    ) -> SparkAzureBlobStorageDatasource: ...
    def update_spark_abs(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
        azure_options: dict[str, Any] = ...,
    ) -> SparkAzureBlobStorageDatasource: ...
    def add_or_update_spark_abs(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        spark_config: SparkConfig | None = None,
        force_reuse_spark_context: bool = True,
        azure_options: dict[str, Any] = ...,
    ) -> SparkAzureBlobStorageDatasource: ...
    def delete_spark_abs(
        self,
        name: str,
    ) -> None: ...
    def add_sqlite(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        connection_string: Union[ConfigStr, SqliteDsn, str] = ...,
        create_temp_table: bool = True,
    ) -> SqliteDatasource: ...
    def update_sqlite(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        connection_string: Union[ConfigStr, SqliteDsn, str] = ...,
        create_temp_table: bool = True,
    ) -> SqliteDatasource: ...
    def add_or_update_sqlite(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        connection_string: Union[ConfigStr, SqliteDsn, str] = ...,
        create_temp_table: bool = True,
    ) -> SqliteDatasource: ...
    def delete_sqlite(
        self,
        name: str,
    ) -> None: ...
    @overload
    def add_snowflake(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = ...,
        name: Optional[str] = ...,
        datasource: Optional[Datasource] = ...,
        *,
        connection_string: Union[ConfigStr, SnowflakeDsn, str] = ...,
        create_temp_table: bool = ...,
        account: None = ...,
        user: None = ...,
        password: None = ...,
        database: None = ...,
        schema: None = ...,
        warehouse: None = ...,
        role: None = ...,
        numpy: bool = ...,
    ) -> SnowflakeDatasource: ...
    @overload
    def add_snowflake(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = ...,
        name: Optional[str] = ...,
        datasource: Optional[Datasource] = ...,
        *,
        connection_string: None = ...,
        create_temp_table: bool = ...,
        account: str = ...,
        user: str = ...,
        password: Union[ConfigStr, str] = ...,
        database: Optional[str] = ...,
        schema: Optional[str] = ...,
        warehouse: Optional[str] = ...,
        role: Optional[str] = ...,
        numpy: bool = ...,
    ) -> SnowflakeDatasource: ...
    def add_snowflake(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        connection_string: Optional[Union[ConfigStr, SnowflakeDsn, str]] = ...,
        create_temp_table: bool = True,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[Union[ConfigStr, str]] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        numpy: bool = False,
    ) -> SnowflakeDatasource: ...
    @overload
    def update_snowflake(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = ...,
        name: Optional[str] = ...,
        datasource: Optional[Datasource] = ...,
        *,
        connection_string: Union[ConfigStr, SnowflakeDsn, str] = ...,
        create_temp_table: bool = ...,
        account: None = ...,
        user: None = ...,
        password: None = ...,
        database: None = ...,
        schema: None = ...,
        warehouse: None = ...,
        role: None = ...,
        numpy: bool = ...,
    ) -> SnowflakeDatasource: ...
    @overload
    def update_snowflake(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = ...,
        name: Optional[str] = ...,
        datasource: Optional[Datasource] = ...,
        *,
        connection_string: None = ...,
        create_temp_table: bool = ...,
        account: str = ...,
        user: str = ...,
        password: Union[ConfigStr, str] = ...,
        database: Optional[str] = ...,
        schema: Optional[str] = ...,
        warehouse: Optional[str] = ...,
        role: Optional[str] = ...,
        numpy: bool = ...,
    ) -> SnowflakeDatasource: ...
    def update_snowflake(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        connection_string: Optional[Union[ConfigStr, SnowflakeDsn, str]] = ...,
        create_temp_table: bool = True,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[Union[ConfigStr, str]] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        numpy: bool = False,
    ) -> SnowflakeDatasource: ...
    @overload
    def add_or_update_snowflake(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = ...,
        name: Optional[str] = ...,
        datasource: Optional[Datasource] = ...,
        *,
        connection_string: Union[ConfigStr, SnowflakeDsn, str] = ...,
        create_temp_table: bool = ...,
        account: None = ...,
        user: None = ...,
        password: None = ...,
        database: None = ...,
        schema: None = ...,
        warehouse: None = ...,
        role: None = ...,
        numpy: bool = ...,
    ) -> SnowflakeDatasource: ...
    @overload
    def add_or_update_snowflake(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = ...,
        name: Optional[str] = ...,
        datasource: Optional[Datasource] = ...,
        *,
        connection_string: None = ...,
        create_temp_table: bool = ...,
        account: str = ...,
        user: str = ...,
        password: Union[ConfigStr, str] = ...,
        database: Optional[str] = ...,
        schema: Optional[str] = ...,
        warehouse: Optional[str] = ...,
        role: Optional[str] = ...,
        numpy: bool = ...,
    ) -> SnowflakeDatasource: ...
    def add_or_update_snowflake(  # noqa: PLR0913
        self,
        name_or_datasource: Optional[Union[str, Datasource]] = None,
        name: Optional[str] = None,
        datasource: Optional[Datasource] = None,
        *,
        connection_string: Optional[Union[ConfigStr, SnowflakeDsn, str]] = ...,
        create_temp_table: bool = True,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[Union[ConfigStr, str]] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        numpy: bool = False,
    ) -> SnowflakeDatasource: ...
    def delete_snowflake(
        self,
        name: str,
    ) -> None: ...

def _iter_all_registered_types(
    include_datasource: bool = True, include_data_asset: bool = True
) -> Generator[tuple[str, Type[Datasource] | Type[DataAsset]], None, None]: ...
