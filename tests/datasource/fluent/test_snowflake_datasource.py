from __future__ import annotations

import pytest
import sqlalchemy as sa
from pytest import param

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.snowflake import snowflake
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.snowflake_datasource import (
    SnowflakeDatasource,
    SnowflakeDsn,
)
from great_expectations.execution_engine import SqlAlchemyExecutionEngine


@pytest.fixture
def seed_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_CONN_STR", "snowflake://my_user:password@my_account")
    monkeypatch.setenv("MY_PASSWORD", "my_password")


@pytest.mark.snowflake  # TODO: make this a unit test
@pytest.mark.parametrize(
    "config_kwargs",
    [
        param(
            {"connection_string": "snowflake://my_user:password@my_account?numpy=True"},
            id="connection_string str",
        ),
        param(
            {"connection_string": "${MY_CONN_STR}"}, id="connection_string ConfigStr"
        ),
        param(
            {
                "connection_string": {
                    "user": "my_user",
                    "password": "password",
                    "account": "my_account",
                }
            },
            id="connection_string dict",
        ),
        param(
            {
                "connection_string": {
                    "user": "my_user",
                    "password": "${MY_PASSWORD}",
                    "account": "my_account",
                }
            },
            id="connection_string dict with password ConfigStr",
        ),
        param(
            {"user": "my_user", "password": "password", "account": "my_account"},
            id="old config format - top level keys",
        ),
    ],
)
def test_valid_config(
    empty_file_context: AbstractDataContext, seed_env_vars: None, config_kwargs: dict
):
    my_sf_ds_1 = SnowflakeDatasource(name="my_sf_ds_1", **config_kwargs)
    assert my_sf_ds_1

    my_sf_ds_1._data_context = (
        empty_file_context  # attach to enable config substitution
    )
    sql_engine = my_sf_ds_1.get_engine()
    assert isinstance(sql_engine, sa.engine.Engine)

    exec_engine = my_sf_ds_1.get_execution_engine()
    assert isinstance(exec_engine, SqlAlchemyExecutionEngine)


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
        pytest.param(
            {"connection_string": {"account": "my_account", "user": "my_user"}},
            {},
            id="incomplete connection_string dict connect_args",
        ),
    ],
)
def test_conflicting_connection_string_and_args_raises_error(
    connection_string: ConfigStr | SnowflakeDsn | None | dict, connect_args: dict
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
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
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
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
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
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
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
        _ = SnowflakeDatasource(
            name="my_snowflake", connection_string=connection_string
        )

    assert expected_errors == exc_info.value.errors()


# TODO: Cleanup how we install test dependencies and remove this skipif
@pytest.mark.skipif(
    True if not snowflake else False, reason="snowflake is not installed"
)
@pytest.mark.unit
def test_get_execution_engine_succeeds():
    connection_string = "snowflake://my_user:password@my_account"
    datasource = SnowflakeDatasource(
        name="my_snowflake", connection_string=connection_string
    )
    # testing that this doesn't raise an exception
    datasource.get_execution_engine()


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
