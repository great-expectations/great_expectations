from typing import TYPE_CHECKING, Mapping, Optional

import pandas as pd
import pytest

from great_expectations.compatibility.pydantic import BaseSettings
from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.sql_datasource import TableAsset
from great_expectations.self_check.util import _get_snowflake_connect_args
from tests.integration.sql_session_manager import SessionSQLEngineManager
from tests.integration.test_utils.data_source_config.backend_spec import SqlBackendSpec
from tests.integration.test_utils.data_source_config.base import (
    BatchTestSetup,
)
from tests.integration.test_utils.data_source_config.data_source_spec import (
    CiLaneRef,
    DataSourceProvisioning,
    ExecutionEngineKind,
    SupportTier,
)
from tests.integration.test_utils.data_source_config.registry import register_sql_config
from tests.integration.test_utils.data_source_config.sql import (
    ConnectionDetails,
    SQLBatchTestSetup,
)
from tests.integration.test_utils.data_source_config.sql_config import SqlDatasourceTestConfig

if TYPE_CHECKING:
    from great_expectations.types.connect_args import ConnectArgs


@register_sql_config
class SnowflakeDatasourceTestConfig(SqlDatasourceTestConfig):
    DATA_SOURCE_SPEC = SqlBackendSpec(
        label="snowflake",
        public_name="Snowflake",
        marker="snowflake",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        execution_engine=ExecutionEngineKind.SQL,
        fluent_types=frozenset({"snowflake"}),
        # Snowflake's CI lane is a dedicated job (`marker-tests-snowflake`), not a
        # `marker-tests` matrix entry: it is sharded across `pytest-split` groups rather than
        # selected as one matrix cell among many markers.
        ci_lane=CiLaneRef(workflow_job="marker-tests-snowflake", marker_token="snowflake"),
        uses_schema=True,
        tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
        dev_requirements_file="reqs/requirements-dev-snowflake.txt",
        task_runner_marker="snowflake",
    )

    @override
    def create_batch_setup(
        self,
        request: pytest.FixtureRequest,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> BatchTestSetup:
        return SnowflakeBatchTestSetup(
            data=data,
            config=self,
            extra_data=extra_data,
            table_name=self.table_name,
            context=context,
            engine_manager=engine_manager,
        )


class SnowflakeConnectionConfig(BaseSettings):
    """This class retrieves these values from the environment.
    If you're testing locally, you can use your Snowflake creds
    and test against your own Snowflake account.

    Supports both password and key-pair authentication:
    - For password auth: Set SNOWFLAKE_USER and SNOWFLAKE_PW
    - For key-pair auth: Set SNOWFLAKE_USER and SNOWFLAKE_PRIVATE_KEY (base64-encoded)
    """

    SNOWFLAKE_USER: str
    SNOWFLAKE_PW: Optional[str] = None  # Optional when using key-pair auth
    SNOWFLAKE_ACCOUNT: str
    SNOWFLAKE_DATABASE: str
    SNOWFLAKE_WAREHOUSE: str
    SNOWFLAKE_ROLE: str
    SNOWFLAKE_PRIVATE_KEY: Optional[str] = None  # base64-encoded private key

    def build_connection_string(self) -> str:
        # Note: we don't specify the schema here because it will be created dynamically, and we pass
        # it into the `data_sources.add_snowflake` call.

        # When using private key auth, include colon with empty password
        # The private key will be passed separately via connect_args
        # Format: snowflake://user:@account (colon indicates password field, even if empty)
        if self.SNOWFLAKE_PRIVATE_KEY:
            user_auth = f"{self.SNOWFLAKE_USER}:"
        elif self.SNOWFLAKE_PW:
            user_auth = f"{self.SNOWFLAKE_USER}:{self.SNOWFLAKE_PW}"
        else:
            raise ValueError("Either SNOWFLAKE_PW or SNOWFLAKE_PRIVATE_KEY must be set")

        return (
            f"snowflake://{user_auth}"
            f"@{self.SNOWFLAKE_ACCOUNT}/{self.SNOWFLAKE_DATABASE}"
            f"?warehouse={self.SNOWFLAKE_WAREHOUSE}&role={self.SNOWFLAKE_ROLE}"
        )

    @property
    def private_key(self) -> Optional[str]:
        """Return the private key if using key-pair auth.

        The private key should be base64-encoded in the SNOWFLAKE_PRIVATE_KEY env var.
        """
        return self.SNOWFLAKE_PRIVATE_KEY


class SnowflakeBatchTestSetup(SQLBatchTestSetup[SnowflakeDatasourceTestConfig]):
    @override
    def build_connection_string(self, schema: str | None = None) -> str:
        return self.snowflake_connection_config.build_connection_string()

    @property
    def private_key(self) -> Optional[str]:
        return self.snowflake_connection_config.private_key

    def __init__(
        self,
        config: SnowflakeDatasourceTestConfig,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        table_name: Optional[str] = None,
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> None:
        self.snowflake_connection_config = SnowflakeConnectionConfig()  # type: ignore[call-arg]  # retrieves env vars
        super().__init__(
            config=config,
            data=data,
            extra_data=extra_data,
            table_name=table_name,
            engine_manager=engine_manager,
            context=context,
        )

    @override
    def _get_engine(self) -> tuple:
        """Override to handle key-pair authentication for Snowflake."""

        from sqlalchemy import create_engine

        if self.engine_manager:
            connection_details = ConnectionDetails(
                connection_string=self.build_connection_string(),
            )
            connect_args: ConnectArgs = {"private_key": self.private_key}
            engine = self.engine_manager.get_engine(connection_details, connect_args=connect_args)
            return engine, lambda: None
        else:
            # For key-pair auth, pass private key via connect_args
            if private_key := self.snowflake_connection_config.private_key:
                connect_args = _get_snowflake_connect_args(private_key)
                engine = create_engine(
                    url=self.build_connection_string(), connect_args=connect_args
                )
            else:
                engine = create_engine(url=self.build_connection_string())
            return engine, engine.dispose

    @override
    def make_asset(self) -> TableAsset:
        schema = self.schema
        assert schema

        # Use key-pair auth if private key is present, otherwise password auth
        if private_key := self.snowflake_connection_config.private_key:
            return self.context.data_sources.add_snowflake(
                name=self._random_resource_name(),
                account=self.snowflake_connection_config.SNOWFLAKE_ACCOUNT,
                user=self.snowflake_connection_config.SNOWFLAKE_USER,
                private_key=private_key,
                database=self.snowflake_connection_config.SNOWFLAKE_DATABASE,
                schema=schema,
                warehouse=self.snowflake_connection_config.SNOWFLAKE_WAREHOUSE,
                role=self.snowflake_connection_config.SNOWFLAKE_ROLE,
            ).add_table_asset(
                name=self._random_resource_name(),
                table_name=self.table_name,
            )
        else:
            # Password auth - ensure password is set
            password = self.snowflake_connection_config.SNOWFLAKE_PW
            assert password is not None, "SNOWFLAKE_PW must be set when not using key-pair auth"

            return self.context.data_sources.add_snowflake(
                name=self._random_resource_name(),
                account=self.snowflake_connection_config.SNOWFLAKE_ACCOUNT,
                user=self.snowflake_connection_config.SNOWFLAKE_USER,
                password=password,
                database=self.snowflake_connection_config.SNOWFLAKE_DATABASE,
                schema=schema,
                warehouse=self.snowflake_connection_config.SNOWFLAKE_WAREHOUSE,
                role=self.snowflake_connection_config.SNOWFLAKE_ROLE,
            ).add_table_asset(
                name=self._random_resource_name(),
                table_name=self.table_name,
            )
