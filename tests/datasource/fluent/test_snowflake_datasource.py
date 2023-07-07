from __future__ import annotations

import pydantic
import pytest

from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.snowflake_datasource import (
    SnowflakeDatasource,
    SnowflakeDsn,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    "connection_string, connect_args",
    [
        pytest.param(
            "snowflake://<user_login_name>:<password>@<account_identifier>",
            {"account": "my_account", "user": "my_user", "password": "123456"},
            id="both connection_string and connect_args",
        ),
        pytest.param(None, {}, id="neither connection_string nor connect_args"),
        pytest.param(
            None,
            {"account": "my_account", "user": "my_user"},
            id="incomplete connect_args",
        ),
    ],
)
def test_conflicting_connection_string_and_args_raises_error(
    connection_string: ConfigStr | SnowflakeDsn | None, connect_args: dict
):
    with pytest.raises(ValueError):
        _ = SnowflakeDatasource(connection_string=connection_string, **connect_args)


@pytest.mark.unit
@pytest.mark.parametrize(
    "connection_string, expected_errors",
    [
        pytest.param(
            "user_login_name:password@account_identifier",
            [
                {
                    "loc": ("connection_string",),
                    "msg": "ConfigStr - contains no config template strings in the format '${MY_CONFIG_VAR}' or '$MY_CONFIG_VAR'",
                    "type": "value_error",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "invalid or missing URL scheme",
                    "type": "value_error.url.scheme",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, user, and password.",
                    "type": "value_error",
                },
            ],
            id="missing scheme",
        ),
        pytest.param(
            "snowflake://user_login_name@account_identifier",
            [
                {
                    "loc": ("connection_string",),
                    "msg": "ConfigStr - contains no config template strings in the format '${MY_CONFIG_VAR}' or '$MY_CONFIG_VAR'",
                    "type": "value_error",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "URL password invalid",
                    "type": "value_error.url.password",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, user, and password.",
                    "type": "value_error",
                },
            ],
            id="bad password",
        ),
        pytest.param(
            "snowflake://user_login_name:password@",
            [
                {
                    "loc": ("connection_string",),
                    "msg": "ConfigStr - contains no config template strings in the format '${MY_CONFIG_VAR}' or '$MY_CONFIG_VAR'",
                    "type": "value_error",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "URL domain invalid",
                    "type": "value_error.url.domain",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, user, and password.",
                    "type": "value_error",
                },
            ],
            id="bad domain",
        ),
    ],
)
def test_invalid_connection_string_raises_dsn_error(
    connection_string: str, expected_errors: list[dict]
):
    with pytest.raises(pydantic.ValidationError) as exc_info:
        _ = SnowflakeDatasource(name="my_snowflake", connection_string=connection_string)  # type: ignore[arg-type] # Pydantic coerces connection_string to SnowflakeDsn

    assert expected_errors == exc_info.value.errors()
