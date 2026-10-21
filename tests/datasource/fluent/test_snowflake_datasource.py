from __future__ import annotations

import base64
import logging
from pprint import pformat as pf
from typing import TYPE_CHECKING, Final, Sequence
from unittest.mock import ANY

import pytest
import sqlalchemy as sa
from pytest import param

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.snowflake import URL as SnowflakeURL
from great_expectations.compatibility.snowflake import snowflake
from great_expectations.datasource.fluent import (
    GxContextWarning,
    SQLDatasource,
    TestConnectionError,
)
from great_expectations.datasource.fluent.config_str import ConfigStr, ConfigUri
from great_expectations.datasource.fluent.snowflake_datasource import (
    AccountIdentifier,
    SnowflakeDatasource,
    SnowflakeDsn,
)
from great_expectations.execution_engine import SqlAlchemyExecutionEngine

if TYPE_CHECKING:
    from pytest.mark.structures import ParameterSet  # type: ignore[import-not-found] # FIXME CoP
    from pytest_mock import MockerFixture

    from great_expectations.data_context import AbstractDataContext

TEST_LOGGER: Final = logging.getLogger(__name__)

_EXAMPLE_PRIVATE_KEY: Final[bytes] = b"""-----BEGIN RSA PRIVATE KEY-----
MIICXAIBAAKBgHpFdCdKOGLaiMH9t1th1lKqJVcDwfnlP2lpneANbbsgHb6/4U2U
ua085zlNYhZ5xJsnSdqIAfragzuVYNk2OCpoN1Qkq4oWad0a4cEB2QBtP9js0dVW
xQObJM8t1ZLHB3Lw1NCqB6OefkP7XlE0w6aXRZ5IWwvVC86cBXVBmBXzAgMBAAEC
gYABR6TVnHNGpZ702OEIdde2ec12QbXQFdQ6GD7sz3cslEN7caq8Eyh2ZcLN2L+E
GLY0IY8mWHIc3BivkPq4i1a/JyRUzEToJvjVd8J1slrzz8ryMOAiPbxt33IpgGL3
/8KgOLYxjdg5bpn6sCZlOXy7WYjl1H8TBw8CzZF41Ha24QJBAM7U+8m0hyknbnBD
gKXGb0eHIBx0zlPaNJwDHUcJXujxbVfwVjKWLy07JoXRiAgPuVszIMhu0r+Xa87L
W2WLdTsCQQCXVm0He7SaytnrlAFck5/L4EjtWaAQGfmV4eawI2HemWMjj0tukdFt
wAWHDuKYMb+bg21OU2XQxollYYJfk/apAkBaSe10WuNZ2sXCKiWBuIMhZWJmKbNc
NXgb1tw0A2o0JBhIeDkYsij8BMNHTXWllz+iCUq5VG+ZhX9hcbJ/PIa7AkEAjfgd
v+9ktfGmDUGDJX23YmK9BywU5AX6BYkuB/6pSVFLl4hNkyRn+zUv+ksUdwH0Zccd
O2UxFnGpYtnenBsKQQJBAMX2tgFcg//t1Li4+dxlTvZZ/clZCLpWXp4HQgBwzxMN
wpDoF40OzNYrrKIboU4BJFOMOBWAS4DFDYGdfLVS99g=
-----END RSA PRIVATE KEY-----"""

_EXAMPLE_B64_ENCODED_PRIVATE_KEY: Final[bytes] = base64.standard_b64encode(_EXAMPLE_PRIVATE_KEY)

KEY_PAIR_CONNECTION_ARGS = {
    "user": "my_user",
    "private_key": (
        "MIICXAIBAAKBgHpFdCdKOGLaiMH9t1th1lKqJVcDwfnlP2lpneANbbsgHb6/4U2U"
        "ua085zlNYhZ5xJsnSdqIAfragzuVYNk2OCpoN1Qkq4oWad0a4cEB2QBtP9js0dVW"
        "xQObJM8t1ZLHB3Lw1NCqB6OefkP7XlE0w6aXRZ5IWwvVC86cBXVBmBXzAgMBAAEC"
        "gYABR6TVnHNGpZ702OEIdde2ec12QbXQFdQ6GD7sz3cslEN7caq8Eyh2ZcLN2L+E"
        "GLY0IY8mWHIc3BivkPq4i1a/JyRUzEToJvjVd8J1slrzz8ryMOAiPbxt33IpgGL3"
        "/8KgOLYxjdg5bpn6sCZlOXy7WYjl1H8TBw8CzZF41Ha24QJBAM7U+8m0hyknbnBD"
        "gKXGb0eHIBx0zlPaNJwDHUcJXujxbVfwVjKWLy07JoXRiAgPuVszIMhu0r+Xa87L"
        "W2WLdTsCQQCXVm0He7SaytnrlAFck5/L4EjtWaAQGfmV4eawI2HemWMjj0tukdFt"
        "wAWHDuKYMb+bg21OU2XQxollYYJfk/apAkBaSe10WuNZ2sXCKiWBuIMhZWJmKbNc"
        "NXgb1tw0A2o0JBhIeDkYsij8BMNHTXWllz+iCUq5VG+ZhX9hcbJ/PIa7AkEAjfgd"
        "v+9ktfGmDUGDJX23YmK9BywU5AX6BYkuB/6pSVFLl4hNkyRn+zUv+ksUdwH0Zccd"
        "O2UxFnGpYtnenBsKQQJBAMX2tgFcg//t1Li4+dxlTvZZ/clZCLpWXp4HQgBwzxMN"
        "wpDoF40OzNYrrKIboU4BJFOMOBWAS4DFDYGdfLVS99g="
    ),
    "account": "my_account",
    "schema": "S_PUBLIC",
    "database": "D_PUBLIC",
    "role": "my_role",
    "warehouse": "my_wh",
}


VALID_DS_CONFIG_PARAMS: Final[Sequence[ParameterSet]] = [
    param(
        {
            "connection_string": "snowflake://my_user:password@my_account/d_public/s_public?numpy=True&role=my_role&warehouse=my_wh"
        },
        id="connection_string str",
    ),
    param(
        {
            "connection_string": "snowflake://my_user:${MY_PASSWORD}@my_account/d_public/s_public?role=my_role&warehouse=my_wh"
        },
        id="connection_string ConfigStr - password sub",
    ),
    param(
        {
            "connection_string": "snowflake://${MY_USER}:${MY_PASSWORD}@my_account/d_public/s_public?role=my_role&warehouse=my_wh"
        },
        id="connection_string ConfigStr - user + password sub",
    ),
    param(
        {
            "connection_string": {
                "user": "my_user",
                "password": "password",
                "account": "my_account",
                "schema": "S_PUBLIC",
                "database": "D_PUBLIC",
                "role": "my_role",
                "warehouse": "my_wh",
            }
        },
        id="connection_string dict",
    ),
    param(
        {
            "connection_string": {
                "user": "my_user",
                "private_key": (
                    "MIICXAIBAAKBgHpFdCdKOGLaiMH9t1th1lKqJVcDwfnlP2lpneANbbsgHb6/4U2U"
                    "ua085zlNYhZ5xJsnSdqIAfragzuVYNk2OCpoN1Qkq4oWad0a4cEB2QBtP9js0dVW"
                    "xQObJM8t1ZLHB3Lw1NCqB6OefkP7XlE0w6aXRZ5IWwvVC86cBXVBmBXzAgMBAAEC"
                    "gYABR6TVnHNGpZ702OEIdde2ec12QbXQFdQ6GD7sz3cslEN7caq8Eyh2ZcLN2L+E"
                    "GLY0IY8mWHIc3BivkPq4i1a/JyRUzEToJvjVd8J1slrzz8ryMOAiPbxt33IpgGL3"
                    "/8KgOLYxjdg5bpn6sCZlOXy7WYjl1H8TBw8CzZF41Ha24QJBAM7U+8m0hyknbnBD"
                    "gKXGb0eHIBx0zlPaNJwDHUcJXujxbVfwVjKWLy07JoXRiAgPuVszIMhu0r+Xa87L"
                    "W2WLdTsCQQCXVm0He7SaytnrlAFck5/L4EjtWaAQGfmV4eawI2HemWMjj0tukdFt"
                    "wAWHDuKYMb+bg21OU2XQxollYYJfk/apAkBaSe10WuNZ2sXCKiWBuIMhZWJmKbNc"
                    "NXgb1tw0A2o0JBhIeDkYsij8BMNHTXWllz+iCUq5VG+ZhX9hcbJ/PIa7AkEAjfgd"
                    "v+9ktfGmDUGDJX23YmK9BywU5AX6BYkuB/6pSVFLl4hNkyRn+zUv+ksUdwH0Zccd"
                    "O2UxFnGpYtnenBsKQQJBAMX2tgFcg//t1Li4+dxlTvZZ/clZCLpWXp4HQgBwzxMN"
                    "wpDoF40OzNYrrKIboU4BJFOMOBWAS4DFDYGdfLVS99g="
                ),
                "account": "my_account",
                "schema": "S_PUBLIC",
                "database": "D_PUBLIC",
                "role": "my_role",
                "warehouse": "my_wh",
            }
        },
        id="key pair dict",
    ),
    param(
        {
            "connection_string": {
                "user": "my_user",
                "password": "${MY_PASSWORD}",
                "account": "my_account",
                "schema": "s_public",
                "database": "d_public",
                "role": "my_role",
                "warehouse": "my_wh",
            }
        },
        id="connection_string dict with password ConfigStr",
    ),
    param(
        {
            "connection_string": {
                "user": "my_user",
                "password": "DUMMY_VALUE",
                "account": "my_account",
                "database": "d_public",
                "schema": "s_public",
                "warehouse": "my_wh",
                "role": "my_role",
            },
            "kwargs": {"connect_args": {"private": _EXAMPLE_PRIVATE_KEY}},
        },
        id="private_key auth",
    ),
    param(
        {
            "connection_string": {
                "user": "my_user",
                "password": "DUMMY_VALUE",
                "account": "my_account",
                "database": "d_public",
                "schema": "s_public",
                "warehouse": "my_wh",
                "role": "my_role",
            },
            "kwargs": {"connect_args": {"private": _EXAMPLE_B64_ENCODED_PRIVATE_KEY}},
        },
        id="private_key auth b64 encoded",
    ),
]


@pytest.fixture
def seed_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_USER", "my_user")
    monkeypatch.setenv("MY_PASSWORD", "my_password")


@pytest.fixture
def sf_test_connection_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    TEST_LOGGER.warning("Monkeypatching SnowflakeDatasource.test_connection() to a noop")

    def noop(self):
        TEST_LOGGER.info(".test_connection noop")

    monkeypatch.setattr(SnowflakeDatasource, "test_connection", noop)


@pytest.fixture
def sql_ds_test_connection_always_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Monkey patch the parent class' test_connection() method to always fail.
    Useful for testing the extra handling that SnowflakeDatasource.test_connection() provides.
    """
    TEST_LOGGER.warning("Monkeypatching SQLDatasource.test_connection() to a always fail")

    def fail(self, test_assets: bool = True):
        TEST_LOGGER.info("SQLDatasource.test_connection() always fail")
        raise TestConnectionError("always fail")

    monkeypatch.setattr(SQLDatasource, "test_connection", fail)


@pytest.mark.unit
class TestAccountIdentifier:
    @pytest.mark.parametrize(
        "value",
        [
            "orgname.account_name",
            "orgname-account_name",
            "abc12345.us-east-1.aws",
            "xy12345.us-gov-west-1.aws",
            "xy12345.europe-west4.gcp",
            "xy12345.central-us.azure",
        ],
    )
    def test_str_and_repr_methods(self, value: str):
        account_identifier: AccountIdentifier = pydantic.parse_obj_as(AccountIdentifier, value)
        assert str(account_identifier) == value
        assert repr(account_identifier) == f"AccountIdentifier({value!r})"

    @pytest.mark.parametrize("account_name", ["account_name", "account-name"])
    def test_fmt1_parse(self, account_name: str):
        orgname = "orgname"
        value = f"{orgname}-{account_name}"
        print(f"{value=}")

        account_identifier = pydantic.parse_obj_as(AccountIdentifier, value)
        assert account_identifier.match

        assert account_identifier.account_name == account_name
        assert account_identifier.orgname == orgname
        assert account_identifier.as_tuple() == (orgname, account_name)

    @pytest.mark.parametrize(
        "value",
        [
            "abc12345.us-east-1.aws",
            "xy12345.us-gov-west-1.aws",
            "xy12345.europe-west4.gcp",
            "xy12345.central-us.azure",
        ],
    )
    def test_fmt2_parse(self, value: str):
        """
        The cloud portion is technically optional if the the provider is AWS, but expecting greatly
        simplifies our parsing logic.
        """
        print(f"{value=}")
        locator, _, _remainder = value.partition(".")
        cloud_region_id, _, cloud_service = _remainder.partition(".")

        account_identifier = pydantic.parse_obj_as(AccountIdentifier, value)
        assert account_identifier.match

        assert account_identifier.account_locator == locator
        assert account_identifier.region == (cloud_region_id or None)
        assert account_identifier.cloud == (cloud_service or None)

        assert account_identifier.as_tuple() == (
            locator,
            cloud_region_id,
            cloud_service,
        )

    @pytest.mark.parametrize(
        "value",
        [
            "foobar",
            "orgname.account-name",
            "orgname.account_name",
            "my_account.us-east-1",
            "xy12345.us-gov-west-1.aws.",
            "xy12345.europe-west4.gcp.bar",
            "xy12345.us-east-1.nope",
            "xy12345.",
            "xy12345.central-us.bazar",
            "xy12345_us-gov-west-1_aws",
            "xy12345_europe-west4_gcp",
            "xy12345_central-us_azure",
        ],
    )
    def test_invalid_formats(self, value: str):
        """
        Test that an invalid format that does not match but can still be stringified as
        the original value.
        """
        print(f"{value=}")
        account_identifier = pydantic.parse_obj_as(AccountIdentifier, value)
        assert not account_identifier.match
        assert str(account_identifier) == value


@pytest.mark.unit
def test_snowflake_dsn():
    dsn = pydantic.parse_obj_as(
        SnowflakeDsn,
        "snowflake://my_user:password@my_account/my_db/my_schema?role=my_role&warehouse=my_wh",
    )
    assert dsn.user == "my_user"
    assert dsn.password == "password"
    assert dsn.account_identifier == "my_account"
    assert dsn.database == "my_db"
    assert dsn.schema_ == "my_schema"
    assert dsn.role == "my_role"
    assert dsn.warehouse == "my_wh"


@pytest.mark.snowflake  # TODO: make this a unit test
@pytest.mark.parametrize(
    "config_kwargs",
    [
        *VALID_DS_CONFIG_PARAMS,
        param(
            {
                "user": "my_user",
                "password": "password",
                "account": "my_account",
                "schema": "s_public",
                "database": "d_public",
                "role": "my_role",
                "warehouse": "my_wh",
            },
            id="old config format - top level keys",
        ),
    ],
)
def test_valid_config(
    empty_file_context: AbstractDataContext,
    seed_env_vars: None,
    config_kwargs: dict,
    param_id: str,
):
    my_sf_ds_1 = SnowflakeDatasource(name=f"my_sf {param_id}", **config_kwargs)
    assert my_sf_ds_1

    my_sf_ds_1._data_context = empty_file_context  # attach to enable config substitution
    sql_engine = my_sf_ds_1.get_engine()
    assert isinstance(sql_engine, sa.engine.Engine)

    exec_engine = my_sf_ds_1.get_execution_engine()
    assert isinstance(exec_engine, SqlAlchemyExecutionEngine)


@pytest.mark.unit
@pytest.mark.parametrize(
    ["connection_string", "expected_errors"],
    [
        pytest.param(
            "${MY_CONFIG_VAR}",
            [
                {
                    "loc": ("connection_string", "__root__"),
                    "msg": "Only password, user may use config substitution;"
                    " 'domain' substitution not allowed",
                    "type": "value_error",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, password, database, schema, warehouse, role as keyword args or "
                    "a combination of account, user, database, schema, warehouse, role, "
                    "private_key as keyword args.",
                    "type": "value_error",
                },
            ],
            id="illegal config substitution - full connection string",
        ),
        pytest.param(
            "snowflake://my_user:password@${MY_CONFIG_VAR}/db/schema",
            [
                {
                    "loc": ("connection_string", "__root__"),
                    "msg": "Only password, user may use config substitution;"
                    " 'domain' substitution not allowed",
                    "type": "value_error",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, password, database, schema, warehouse, role as keyword args or "
                    "a combination of account, user, database, schema, warehouse, role, "
                    "private_key as keyword args.",
                    "type": "value_error",
                },
            ],
            id="illegal config substitution - account (domain)",
        ),
        pytest.param(
            "snowflake://my_user:password@account/${MY_CONFIG_VAR}/schema",
            [
                {
                    "loc": ("connection_string", "__root__"),
                    "msg": "Only password, user may use config substitution;"
                    " 'path' substitution not allowed",
                    "type": "value_error",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, password, database, schema, warehouse, role as keyword args or "
                    "a combination of account, user, database, schema, warehouse, role, "
                    "private_key as keyword args.",
                    "type": "value_error",
                },
            ],
            id="illegal config substitution - database (path)",
        ),
        pytest.param(
            "snowflake://my_user:password@account/db/${MY_CONFIG_VAR}",
            [
                {
                    "loc": ("connection_string", "__root__"),
                    "msg": "Only password, user may use config substitution;"
                    " 'path' substitution not allowed",
                    "type": "value_error",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, password, database, schema, warehouse, role as keyword args or "
                    "a combination of account, user, database, schema, warehouse, role, "
                    "private_key as keyword args.",
                    "type": "value_error",
                },
            ],
            id="illegal config substitution - schema (path)",
        ),
        pytest.param(
            "snowflake://my_user:password@my_account",
            [
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "ConfigStr - contains no config template strings in the format '${MY_CONFIG_VAR}'. If your value contains a literal '$', it must be escaped as '\\$'. See https://docs.greatexpectations.io/docs/core/configure_project_settings/configure_credentials",  # noqa: E501 # FIXME CoP
                    "type": "value_error",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "URL path missing database/schema",
                    "type": "value_error.url.path",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, password, database, schema, warehouse, role as keyword args or "
                    "a combination of account, user, database, schema, warehouse, role, "
                    "private_key as keyword args.",
                    "type": "value_error",
                },
            ],
            id="missing path",
        ),
        pytest.param(
            "snowflake://my_user:password@my_account//",
            [
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "ConfigStr - contains no config template strings in the format '${MY_CONFIG_VAR}'. If your value contains a literal '$', it must be escaped as '\\$'. See https://docs.greatexpectations.io/docs/core/configure_project_settings/configure_credentials",  # noqa: E501 # FIXME CoP
                    "type": "value_error",
                },
                {
                    "ctx": {"msg": "missing database"},
                    "loc": ("connection_string",),
                    "msg": "URL path missing database/schema",
                    "type": "value_error.url.path",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, password, database, schema, warehouse, role as keyword args or "
                    "a combination of account, user, database, schema, warehouse, role, "
                    "private_key as keyword args.",
                    "type": "value_error",
                },
            ],
            id="missing database + schema",
        ),
        pytest.param(
            "snowflake://my_user:password@my_account/my_db",
            [
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "ConfigStr - contains no config template strings in the format '${MY_CONFIG_VAR}'. If your value contains a literal '$', it must be escaped as '\\$'. See https://docs.greatexpectations.io/docs/core/configure_project_settings/configure_credentials",  # noqa: E501 # FIXME CoP
                    "type": "value_error",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "URL path missing database/schema",
                    "type": "value_error.url.path",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, password, database, schema, warehouse, role as keyword args or "
                    "a combination of account, user, database, schema, warehouse, role, "
                    "private_key as keyword args.",
                    "type": "value_error",
                },
            ],
            id="missing schema",
        ),
        pytest.param(
            "snowflake://my_user:password@my_account/my_db/",
            [
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "ConfigStr - contains no config template strings in the format '${MY_CONFIG_VAR}'. If your value contains a literal '$', it must be escaped as '\\$'. See https://docs.greatexpectations.io/docs/core/configure_project_settings/configure_credentials",  # noqa: E501 # FIXME CoP
                    "type": "value_error",
                },
                {
                    "ctx": {"msg": "missing schema"},
                    "loc": ("connection_string",),
                    "msg": "URL path missing database/schema",
                    "type": "value_error.url.path",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, password, database, schema, warehouse, role as keyword args or "
                    "a combination of account, user, database, schema, warehouse, role, "
                    "private_key as keyword args.",
                    "type": "value_error",
                },
            ],
            id="missing schema 2",
        ),
        pytest.param(
            "snowflake://my_user:password@my_account//my_schema",
            [
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "ConfigStr - contains no config template strings in the format '${MY_CONFIG_VAR}'. If your value contains a literal '$', it must be escaped as '\\$'. See https://docs.greatexpectations.io/docs/core/configure_project_settings/configure_credentials",  # noqa: E501 # FIXME CoP
                    "type": "value_error",
                },
                {
                    "ctx": {"msg": "missing database"},
                    "loc": ("connection_string",),
                    "msg": "URL path missing database/schema",
                    "type": "value_error.url.path",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, password, database, schema, warehouse, role as keyword args or "
                    "a combination of account, user, database, schema, warehouse, role, "
                    "private_key as keyword args.",
                    "type": "value_error",
                },
            ],
            id="missing database",
        ),
    ],
)
def test_missing_required_params(
    connection_string: str,
    expected_errors: list[dict],  # TODO: use pydantic error dict
):
    with pytest.raises(pydantic.ValidationError) as exc_info:
        ds = SnowflakeDatasource(
            name="my_sf_ds",
            connection_string=connection_string,
        )
        print(f"{ds!r}")

    print(f"\n\tErrors:\n{pf(exc_info.value.errors())}")
    assert exc_info.value.errors() == expected_errors


@pytest.mark.unit
@pytest.mark.parametrize(
    "connection_string, expected_errors",
    [
        pytest.param(
            "user_login_name:password@account_identifier/db/schema?role=my_role&warehouse=my_wh",
            [
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "ConfigStr - contains no config template strings in the format '${MY_CONFIG_VAR}'. If your value contains a literal '$', it must be escaped as '\\$'. See https://docs.greatexpectations.io/docs/core/configure_project_settings/configure_credentials",  # noqa: E501 # FIXME CoP
                    "type": "value_error",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "invalid or missing URL scheme",
                    "type": "value_error.url.scheme",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, password, database, schema, warehouse, role as keyword args or "
                    "a combination of account, user, database, schema, warehouse, role, "
                    "private_key as keyword args.",
                    "type": "value_error",
                },
            ],
            id="missing scheme",
        ),
        pytest.param(
            "snowflake://user_login_name@account_identifier/db/schema?role=my_role&warehouse=my_wh",
            [
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "ConfigStr - contains no config template strings in the format '${MY_CONFIG_VAR}'. If your value contains a literal '$', it must be escaped as '\\$'. See https://docs.greatexpectations.io/docs/core/configure_project_settings/configure_credentials",  # noqa: E501 # FIXME CoP
                    "type": "value_error",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "URL password invalid",
                    "type": "value_error.url.password",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, password, database, schema, warehouse, role as keyword args or "
                    "a combination of account, user, database, schema, warehouse, role, "
                    "private_key as keyword args.",
                    "type": "value_error",
                },
            ],
            id="bad password",
        ),
        pytest.param(
            "snowflake://user_login_name:password@/db/schema?role=my_role&warehouse=my_wh",
            [
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "ConfigStr - contains no config template strings in the format '${MY_CONFIG_VAR}'. If your value contains a literal '$', it must be escaped as '\\$'. See https://docs.greatexpectations.io/docs/core/configure_project_settings/configure_credentials",  # noqa: E501 # FIXME CoP
                    "type": "value_error",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "URL domain invalid",
                    "type": "value_error.url.domain",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, password, database, schema, warehouse, role as keyword args or "
                    "a combination of account, user, database, schema, warehouse, role, "
                    "private_key as keyword args.",
                    "type": "value_error",
                },
            ],
            id="bad domain",
        ),
        pytest.param(
            "snowflake://user_login_name:password@account_identifier/db/schema?warehouse=my_wh",
            [
                {
                    "ctx": {"msg": "missing role"},
                    "loc": ("connection_string",),
                    "msg": "URL query param missing",
                    "type": "value_error.url.query",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, password, database, schema, warehouse, role as keyword args or "
                    "a combination of account, user, database, schema, warehouse, role, "
                    "private_key as keyword args.",
                    "type": "value_error",
                },
            ],
            id="missing role",
        ),
        pytest.param(
            "snowflake://user_login_name:password@account_identifier/db/schema?role=my_role",
            [
                {
                    "ctx": {"msg": "missing warehouse"},
                    "loc": ("connection_string",),
                    "msg": "URL query param missing",
                    "type": "value_error.url.query",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, password, database, schema, warehouse, role as keyword args or "
                    "a combination of account, user, database, schema, warehouse, role, "
                    "private_key as keyword args.",
                    "type": "value_error",
                },
            ],
            id="missing warehouse",
        ),
    ],
)
def test_invalid_connection_string_raises_dsn_error(
    connection_string: str, expected_errors: list[dict]
):
    with pytest.raises(pydantic.ValidationError) as exc_info:
        _ = SnowflakeDatasource(name="my_snowflake", connection_string=connection_string)

    assert expected_errors == exc_info.value.errors()


@pytest.mark.unit
def test_connection_updating_templated_connection_string():
    # Create datasource with templated connection string
    conn_str = "snowflake://user:${MY_PASSWORD}@account/db/schema?warehouse=wh&role=role"
    datasource = SnowflakeDatasource(
        name="my_snowflake",
        connection_string=conn_str,
    )

    # Verify initial connection_string is ConfigUri
    assert isinstance(datasource.connection_string, ConfigUri)
    assert datasource.connection_string.template_str == conn_str

    # Assign a new templated connection string directly
    new_conn_str = "snowflake://newuser:${NEW_PASSWORD}@newaccount/newdb/newschema?warehouse=newwh&role=newrole"
    datasource.connection_string = new_conn_str

    # Verify it's still a ConfigUri after assignment (not a plain str)
    assert isinstance(datasource.connection_string, ConfigUri), (
        f"Expected ConfigUri, got {type(datasource.connection_string)}. "
        "This indicates validate_assignment is not enabled."
    )
    assert datasource.connection_string.template_str == new_conn_str


@pytest.mark.parametrize(
    "private_key",
    [
        pytest.param("${MY_PRIVATE_KEY}", id="Env Variable"),
        pytest.param("$MY_PRIVATE_KEY", id="Config Variable"),
    ],
)
@pytest.mark.unit
def test_creating_datasource_with_templated_private_key(private_key):
    connection_args = {
        "user": "my_user",
        "private_key": private_key,
        "account": "my_account",
        "schema": "S_PUBLIC",
        "database": "D_PUBLIC",
        "role": "my_role",
        "warehouse": "my_wh",
    }
    datasource = SnowflakeDatasource(name="my_snowflake", **connection_args)

    assert isinstance(datasource.private_key, ConfigStr)


@pytest.mark.parametrize(
    "private_key",
    [
        pytest.param("${MY_PRIVATE_KEY}", id="Env Variable"),
        pytest.param("$MY_PRIVATE_KEY", id="Config Variable"),
    ],
)
@pytest.mark.unit
def test_updating_datasource_with_templated_private_key(private_key):
    datasource = SnowflakeDatasource(name="my_snowflake", **KEY_PAIR_CONNECTION_ARGS)

    # Verify initial connection_string
    assert datasource.private_key != private_key

    datasource.connection_string.private_key = private_key

    assert isinstance(datasource.private_key, ConfigStr)


# TODO: Cleanup how we install test dependencies and remove this skipif
@pytest.mark.skipif(True if not snowflake else False, reason="snowflake is not installed")
@pytest.mark.unit
def test_get_execution_engine_succeeds():
    connection_string = (
        "snowflake://my_user:password@my_account/my_db/my_schema?role=my_role&warehouse=my_wh"
    )
    datasource = SnowflakeDatasource(name="my_snowflake", connection_string=connection_string)
    # testing that this doesn't raise an exception
    datasource.get_execution_engine()


@pytest.mark.snowflake
@pytest.mark.parametrize(
    ["config", "expected_called_with"],
    [
        param(
            {
                "name": "std connection_str",
                "connection_string": "snowflake://user:password@account/db/SCHEMA?warehouse=wh&role=role",
            },
            {"url": ANY},
            id="std connection_string str",
        ),
        param(
            {
                "name": "std connection_details",
                "connection_string": {
                    "user": "user",
                    "password": "password",
                    "account": "account",
                    "database": "db",
                    "schema": "schema",
                    "warehouse": "wh",
                    "role": "role",
                },
            },
            {
                "url": "snowflake://user:password@account/db/schema?application=great_expectations_core&role=role&warehouse=wh",
            },
            id="std connection_string dict",
        ),
        param(
            {
                "name": "conn str with connect_args",
                "connection_string": "snowflake://user:password@account/db/schema?warehouse=wh&role=role",
                "kwargs": {"connect_args": {"private_key": b"my_key"}},
            },
            {
                "connect_args": {"private_key": b"my_key"},
                "url": ANY,
            },
            id="connection_string str with connect_args",
        ),
        param(
            {
                "name": "conn details with connect_args",
                "connection_string": {
                    "user": "user",
                    "password": "password",
                    "account": "account",
                    "database": "db",
                    "schema": "schema",
                    "warehouse": "wh",
                    "role": "role",
                },
                "kwargs": {"connect_args": {"private_key": b"my_key"}},
            },
            {
                "connect_args": {"private_key": b"my_key"},
                "url": "snowflake://user:password@account/db/schema?application=great_expectations_core&role=role&warehouse=wh",
            },
            id="connection_string dict with connect_args",
        ),
        param(
            {
                "name": "conn details with connect_args",
                "connection_string": {
                    "user": "user",
                    "private_key": "my_key",
                    "account": "account",
                    "database": "db",
                    "schema": "schema",
                    "warehouse": "wh",
                    "role": "role",
                },
            },
            {
                "connect_args": {"private_key": "my_key"},
                "url": "snowflake://user:@account/db/schema?application=great_expectations_core&role=role&warehouse=wh",
            },
            id="key pair connection string passes private_key via connect_args, not the url",
        ),
        param(
            {
                "name": "key pair details with connect_args",
                "connection_string": {
                    "user": "user",
                    "private_key": "my_key",
                    "account": "account",
                    "database": "db",
                    "schema": "schema",
                    "warehouse": "wh",
                    "role": "role",
                },
                "kwargs": {"connect_args": {"private_key": b"other_key"}},
            },
            {
                "connect_args": {"private_key": b"other_key"},
                "url": "snowflake://user:@account/db/schema?application=great_expectations_core&role=role&warehouse=wh",
            },
            id="key pair connection string with connect_args keeps the connect_args private_key",
        ),
    ],
)
def test_create_engine_is_called_with_expected_kwargs(
    mocker: MockerFixture,
    sf_test_connection_noop: None,
    ephemeral_context_with_defaults: AbstractDataContext,
    config: dict,
    expected_called_with: dict,
):
    create_engine_spy = mocker.spy(sa, "create_engine")

    # Check if this config has private_key in connect_args (deprecated pattern)
    has_deprecated_pattern = (
        isinstance(config.get("kwargs"), dict)
        and isinstance(config.get("kwargs", {}).get("connect_args"), dict)
        and "private_key" in config.get("kwargs", {}).get("connect_args", {})
    )

    if has_deprecated_pattern:
        with pytest.warns(DeprecationWarning, match="private_key.*deprecated"):
            datasource = ephemeral_context_with_defaults.data_sources.add_snowflake(**config)
    else:
        datasource = ephemeral_context_with_defaults.data_sources.add_snowflake(**config)

    print(datasource)
    engine = datasource.get_engine()
    print(engine)

    create_engine_spy.assert_called_once_with(**expected_called_with)


@pytest.mark.snowflake
@pytest.mark.parametrize(
    ("password", "encoded_password"),
    [
        pytest.param("abc", "abc", id="no need to encode"),
        pytest.param("a@b", "a%40b", id="encode it"),
    ],
)
def test_test_connection_encoding(
    mocker: MockerFixture,
    ephemeral_context_with_defaults: AbstractDataContext,
    password: str,
    encoded_password: str,
):
    account = "account"
    user = "foo"
    role = "role"
    warehouse = "warehouse"
    db = "db"
    schema = "schema"
    create_engine_spy = mocker.patch.object(sa, "create_engine")

    datasource = ephemeral_context_with_defaults.data_sources.add_snowflake(
        name="foo",
        account=account,
        user=user,
        password=password,
        database=db,
        schema=schema,
        role=role,
        warehouse=warehouse,
    )
    datasource.get_engine()

    create_engine_spy.assert_called_with(
        url=SnowflakeURL(
            account=account,
            user=user,
            password=encoded_password,
            database=db,
            schema=schema,
            role=role,
            warehouse=warehouse,
            application="great_expectations_core",
        )
    )


@pytest.mark.unit
@pytest.mark.parametrize("ds_config", VALID_DS_CONFIG_PARAMS)
class TestConvenienceProperties:
    def test_schema(
        self,
        ds_config: dict,
        seed_env_vars: None,
        param_id: str,
        ephemeral_context_with_defaults: AbstractDataContext,
    ):
        datasource = SnowflakeDatasource(name=param_id, **ds_config)
        if isinstance(datasource.connection_string, ConfigStr):
            # expect a warning if connection string is a ConfigStr
            with pytest.warns(GxContextWarning):
                assert not datasource.schema_, (
                    "Don't expect schema to be available without config_provider"
                )
            # attach context to enable config substitution
            datasource._data_context = ephemeral_context_with_defaults
            _ = datasource.schema_
        else:
            assert datasource.schema_ == datasource.connection_string.schema_.lower()

    def test_database(
        self,
        ds_config: dict,
        seed_env_vars: None,
        param_id: str,
        ephemeral_context_with_defaults: AbstractDataContext,
    ):
        datasource = SnowflakeDatasource(name=param_id, **ds_config)
        if isinstance(datasource.connection_string, ConfigStr):
            # expect a warning if connection string is a ConfigStr
            with pytest.warns(GxContextWarning):
                assert not datasource.database, (
                    "Don't expect database to be available without config_provider"
                )
            # attach context to enable config substitution
            datasource._data_context = ephemeral_context_with_defaults
            _ = datasource.database
        else:
            assert datasource.database == datasource.connection_string.database

    def test_warehouse(
        self,
        ds_config: dict,
        seed_env_vars: None,
        param_id: str,
        ephemeral_context_with_defaults: AbstractDataContext,
    ):
        datasource = SnowflakeDatasource(name=param_id, **ds_config)
        if isinstance(datasource.connection_string, ConfigStr):
            # expect a warning if connection string is a ConfigStr
            with pytest.warns(GxContextWarning):
                assert not datasource.warehouse, (
                    "Don't expect warehouse to be available without config_provider"
                )
            # attach context to enable config substitution
            datasource._data_context = ephemeral_context_with_defaults
            _ = datasource.warehouse
        else:
            assert datasource.warehouse == datasource.connection_string.warehouse

    def test_role(
        self,
        ds_config: dict,
        seed_env_vars: None,
        param_id: str,
        ephemeral_context_with_defaults: AbstractDataContext,
    ):
        datasource = SnowflakeDatasource(name=param_id, **ds_config)
        if isinstance(datasource.connection_string, ConfigStr):
            # expect a warning if connection string is a ConfigStr
            with pytest.warns(GxContextWarning):
                assert not datasource.role, (
                    "Don't expect role to be available without config_provider"
                )
            # attach context to enable config substitution
            datasource._data_context = ephemeral_context_with_defaults
            _ = datasource.role
        else:
            assert datasource.role == datasource.connection_string.role

    def test_account(
        self,
        ds_config: dict,
        seed_env_vars: None,
        param_id: str,
        ephemeral_context_with_defaults: AbstractDataContext,
    ):
        datasource = SnowflakeDatasource(name=param_id, **ds_config)
        if isinstance(datasource.connection_string, ConfigStr):
            # expect a warning if connection string is a ConfigStr
            with pytest.warns(GxContextWarning):
                assert not datasource.account, (
                    "Don't expect account to be available without config_provider"
                )
            # attach context to enable config substitution
            datasource._data_context = ephemeral_context_with_defaults
            _ = datasource.account
        else:
            assert datasource.account == datasource.connection_string.account

    def test_private_key(
        self,
        ds_config: dict,
        seed_env_vars: None,
        param_id: str,
        ephemeral_context_with_defaults: AbstractDataContext,
    ):
        datasource = SnowflakeDatasource(name=param_id, **ds_config)
        private_key = getattr(datasource.connection_string, "private_key", None)
        assert datasource.private_key == private_key


@pytest.mark.unit
class TestSnowflakeDatasourceDeprecationWarnings:
    """Test deprecation warnings for SnowflakeDatasource."""

    def test_private_key_in_kwargs_connect_args_deprecated_warning(self):
        """Warn when private_key is in kwargs['connect_args']."""
        with pytest.warns(DeprecationWarning, match="private_key.*deprecated"):
            SnowflakeDatasource(
                name="test_ds",
                user="my_user",
                password="<PLACEHOLDER PASSWORD>",
                account="my_account",
                schema="S_PUBLIC",
                database="D_PUBLIC",
                role="my_role",
                warehouse="my_wh",
                kwargs={"connect_args": {"private_key": b"test_key"}},
            )
