from __future__ import annotations

import base64
import logging
from pprint import pformat as pf
from sys import version_info as python_version
from typing import TYPE_CHECKING, Final, Sequence
from unittest.mock import ANY

import pytest
import sqlalchemy as sa
from pytest import param

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.snowflake import snowflake
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent import GxContextWarning, GxDatasourceWarning
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.snowflake_datasource import (
    AccountIdentifier,
    SnowflakeDatasource,
    SnowflakeDsn,
    SQLDatasource,
    TestConnectionError,
)
from great_expectations.execution_engine import SqlAlchemyExecutionEngine

if TYPE_CHECKING:
    from pytest.mark.structures import ParameterSet
    from pytest_mock import MockerFixture

TEST_LOGGER: Final = logging.getLogger(__name__)

_EXAMPLE_PRIVATE_KEY: Final[
    bytes
] = b"""-----BEGIN RSA PRIVATE KEY-----
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

_EXAMPLE_B64_ENCODED_PRIVATE_KEY: Final[bytes] = base64.standard_b64encode(
    _EXAMPLE_PRIVATE_KEY
)


VALID_DS_CONFIG_PARAMS: Final[Sequence[ParameterSet]] = [
    param(
        {
            "connection_string": "snowflake://my_user:password@myAccount.us-east-1/d_public/s_public?numpy=True"
        },
        id="connection_string str",
    ),
    param(
        {
            "connection_string": "snowflake://my_user:password@myAccount.us-east-1/d_public/s_public"
            "?warehouse=my_wh&role=my_role",
            "assets": [
                {"name": "min_table_asset", "type": "table"},
                {
                    "name": "table_asset_all_standard_fields",
                    "type": "table",
                    "table_name": "my_table",
                    "schema_name": "s_public",
                },
                {
                    "name": "table_asset_all_forward_compatible_fields",
                    "type": "table",
                    "table_name": "my_table",
                    "schema_name": "s_public",
                    "database": "d_public",
                    "database_name": "d_public",
                },
                {"name": "min_query_asset", "type": "query", "query": "SELECT 1"},
                {
                    "name": "query_asset_all_standard_fields",
                    "type": "query",
                    "query": "SELECT 1",
                },
                {
                    "name": "query_asset_all_forward_compatible_fields",
                    "type": "query",
                    "query": "SELECT 1",
                    "database": "d_public",
                    "database_name": "d_public",
                },
            ],
        },
        id="heterogenous assets",
    ),
    param(
        {"connection_string": "snowflake://my_user:password@myAccount.us-east-1"},
        id="min connection_string str",
    ),
    param(
        {
            "connection_string": "snowflake://my_user:${MY_PASSWORD}@myAccount.us-east-1/d_public/s_public"
        },
        id="connection_string ConfigStr - password sub",
    ),
    param(
        {
            "connection_string": "snowflake://${MY_USER}:${MY_PASSWORD}@myAccount.us-east-1/d_public/s_public"
        },
        id="connection_string ConfigStr - user + password sub",
    ),
    param(
        {
            "connection_string": "snowflake://${MY_USER}:${MY_PASSWORD}@{MY_ACCOUNT}}/d_public/s_public"
        },
        id="connection_string ConfigStr - user, password, account sub",
    ),
    param(
        {
            "connection_string": {
                "user": "my_user",
                "password": "password",
                "account": "myAccount.us-east-1",
                "schema": "s_public",
                "database": "d_public",
            }
        },
        id="connection_string dict",
    ),
    param(
        {
            "connection_string": {
                "user": "my_user",
                "password": "password",
                "account": "myAccount.us-east-1",
            }
        },
        id="min connection_string dict",
    ),
    param(
        {
            "connection_string": {
                "user": "my_user",
                "password": "${MY_PASSWORD}",
                "account": "myAccount.us-east-1",
                "schema": "s_public",
                "database": "d_public",
            }
        },
        id="connection_string dict with password ConfigStr",
    ),
    param(
        {
            "connection_string": {
                "user": "my_user",
                "password": "${MY_PASSWORD}",
                "account": "myAccount.us-east-1",
            }
        },
        id="min connection_string dict with password ConfigStr",
    ),
    param(
        {
            "connection_string": {
                "user": "my_user",
                "password": "DUMMY_VALUE",
                "account": "myAccount.us-east-1",
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
                "account": "myAccount.us-east-1",
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
    monkeypatch.setenv("MY_ACCOUNT", "myAccount.us-east-1")


@pytest.fixture
def sf_test_connection_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    TEST_LOGGER.warning(
        "Monkeypatching SnowflakeDatasource.test_connection() to a noop"
    )

    def noop(self):
        TEST_LOGGER.info(".test_connection noop")

    monkeypatch.setattr(SnowflakeDatasource, "test_connection", noop)


@pytest.fixture
def sql_ds_test_connection_always_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Monkey patch the parent class' test_connection() method to always fail.
    Useful for testing the extra handling that SnowflakeDatasource.test_connection() provides.
    """
    TEST_LOGGER.warning(
        "Monkeypatching SQLDatasource.test_connection() to a always fail"
    )

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
        account_identifier: AccountIdentifier = pydantic.parse_obj_as(
            AccountIdentifier, value
        )
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
        """The cloud portion is technically optional if the the provider is AWS, but expecting greatly simplifies our parsing logic"""
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
        """Test that an invalid format does not match but can still be stringified as the original value."""
        print(f"{value=}")
        account_identifier = pydantic.parse_obj_as(AccountIdentifier, value)
        assert not account_identifier.match
        assert str(account_identifier) == value


@pytest.mark.unit
def test_snowflake_dsn_all_parts():
    dsn = pydantic.parse_obj_as(
        SnowflakeDsn,
        "snowflake://my_user:password@xy12345.us-east-1/my_db/my_schema?role=my_role&warehouse=my_wh",
    )
    assert dsn.user == "my_user"
    assert dsn.password == "password"
    assert dsn.account_identifier == "xy12345.us-east-1"
    assert dsn.database == "my_db"
    assert dsn.schema_ == "my_schema"
    assert dsn.role == "my_role"
    assert dsn.warehouse == "my_wh"


@pytest.mark.unit
def test_snowflake_dsn_minimal_parts():
    dsn = pydantic.parse_obj_as(
        SnowflakeDsn, "snowflake://my_user:password@xy12345.us-east-1"
    )
    assert dsn.user == "my_user"
    assert dsn.password == "password"
    assert dsn.account_identifier == "xy12345.us-east-1"
    assert not dsn.database
    assert not dsn.schema_
    assert not dsn.role
    assert not dsn.warehouse


@pytest.mark.snowflake  # TODO: make this a unit test
@pytest.mark.parametrize(
    "config_kwargs",
    [
        *VALID_DS_CONFIG_PARAMS,
        param(
            {
                "user": "my_user",
                "password": "password",
                "account": "xy12345.us-east-1",
            },
            id="min old config format - top level keys",
        ),
        param(
            {
                "user": "my_user",
                "password": "password",
                "account": "xy12345.us-east-1",
                "schema": "s_public",
                "database": "d_public",
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

    my_sf_ds_1._data_context = (
        empty_file_context  # attach to enable config substitution
    )
    sql_engine = my_sf_ds_1.get_engine()
    assert isinstance(sql_engine, sa.engine.Engine)

    exec_engine = my_sf_ds_1.get_execution_engine()
    assert isinstance(exec_engine, SqlAlchemyExecutionEngine)


@pytest.mark.unit
@pytest.mark.parametrize(
    ["connection_string", "other_args", "expected_errors"],
    [
        pytest.param(
            "snowflake://user:password@",
            {},
            [
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "ConfigStr - contains no config template strings in the format "
                    "'${MY_CONFIG_VAR}' or '$MY_CONFIG_VAR'",
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
                    "user, and password.",
                    "type": "value_error",
                },
            ],
            id="missing/invalid account",
        ),
        pytest.param(
            "snowflake://user:password@account",
            {
                "assets": [
                    {"name": "valid_asset", "type": "table"},
                    {
                        "name": "valid_but_with_fields_that_will_be_ignored",
                        "type": "table",
                        # these will be elided by some forward compatibility logic
                        "database": "this will be ignored",
                        "database_name": "this will be ignored",
                    },
                    {
                        "name": "extra_fields",
                        "type": "table",
                        # these will be elided by some forward compatibility logic
                        "database": "this will be ignored",
                        "database_name": "this will be ignored",
                        # these will not
                        "db": "causes an error",
                        "foo": "this too",
                    },
                ]
            },
            [
                {
                    "loc": ("assets", 2, "TableAsset", "db"),
                    "msg": "extra fields not permitted",
                    "type": "value_error.extra",
                },
                {
                    "loc": ("assets", 2, "TableAsset", "foo"),
                    "msg": "extra fields not permitted",
                    "type": "value_error.extra",
                },
            ],
            id="invalid assets",
        ),
        pytest.param(
            "snowflake://",
            {},
            [
                {
                    "loc": ("connection_string",),
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "ConfigStr - contains no config template strings in the format "
                    "'${MY_CONFIG_VAR}' or '$MY_CONFIG_VAR'",
                    "type": "value_error",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "userinfo required in URL but missing",
                    "type": "value_error.url.userinfo",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, and password.",
                    "type": "value_error",
                },
            ],
            id="missing everything",
        ),
    ],
)
def test_invalid_params(
    connection_string: str,
    other_args: dict,
    expected_errors: list[dict],  # TODO: use pydantic error dict
):
    with pytest.raises(pydantic.ValidationError) as exc_info:
        ds = SnowflakeDatasource(
            name="my_sf_ds",
            connection_string=connection_string,
            **other_args,
        )
        print(f"{ds!r}")

    print(f"\n\tErrors:\n{pf(exc_info.value.errors())}")
    assert exc_info.value.errors() == expected_errors


@pytest.mark.unit
@pytest.mark.parametrize(
    "connection_string, connect_args, expected_errors",
    [
        pytest.param(
            "snowflake://my_user:password@myAccount.us-east-1/foo/bar?numpy=True",
            {
                "account": "myAccount.us-east-1",
                "user": "my_user",
                "password": "123456",
                "schema": "foo",
                "database": "bar",
            },
            [
                {
                    "loc": ("__root__",),
                    "msg": "Cannot provide both a connection string and a combination of account, user, and password.",
                    "type": "value_error",
                }
            ],
            id="both connection_string and connect_args",
        ),
        pytest.param(
            None,
            {},
            [
                {
                    "loc": ("connection_string",),
                    "msg": "none is not an allowed value",
                    "type": "type_error.none.not_allowed",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, user, and password.",
                    "type": "value_error",
                },
            ],
            id="neither connection_string nor connect_args",
        ),
        pytest.param(
            None,
            {
                "account": "myAccount.us-east-1",
                "user": "my_user",
                "schema": "foo",
                "database": "bar",
            },
            [
                {
                    "loc": ("connection_string", "password"),
                    "msg": "field required",
                    "type": "value_error.missing",
                },
                {
                    "loc": ("connection_string",),
                    "msg": f"""expected string or bytes-like object{"" if python_version < (3, 11) else ", got 'dict'"}""",
                    "type": "type_error",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "str type expected",
                    "type": "type_error.str",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, user, and password.",
                    "type": "value_error",
                },
            ],
            id="incomplete connect_args",
        ),
        pytest.param(
            {
                "account": "myAccount.us-east-1",
                "user": "my_user",
                "schema": "foo",
                "database": "bar",
            },
            {},
            [
                {
                    "loc": ("connection_string", "password"),
                    "msg": "field required",
                    "type": "value_error.missing",
                },
                {
                    "loc": ("connection_string",),
                    "msg": f"""expected string or bytes-like object{"" if python_version < (3, 11) else ", got 'dict'"}""",
                    "type": "type_error",
                },
                {
                    "loc": ("connection_string",),
                    "msg": "str type expected",
                    "type": "type_error.str",
                },
                {
                    "loc": ("__root__",),
                    "msg": "Must provide either a connection string or a combination of account, "
                    "user, and password.",
                    "type": "value_error",
                },
            ],
            id="incomplete connection_string dict connect_args",
        ),
    ],
)
def test_conflicting_connection_string_and_args_raises_error(
    connection_string: ConfigStr | SnowflakeDsn | None | dict,
    connect_args: dict,
    expected_errors: list[dict],
):
    with pytest.raises(pydantic.ValidationError) as exc_info:
        _ = SnowflakeDatasource(
            name="my_sf_ds", connection_string=connection_string, **connect_args
        )
    assert exc_info.value.errors() == expected_errors


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


@pytest.mark.unit
@pytest.mark.parametrize(
    "asset",
    [
        {
            "name": "database_name field",
            "type": "table",
            "table_name": "my_table",
            "schema_name": "s_public",
            "database_name": "d_public",
        },
        {
            "name": "database field",
            "type": "table",
            "table_name": "my_table",
            "schema_name": "s_public",
            "database": "d_public",
        },
        {
            "name": "all_forward_compatible_fields",
            "type": "table",
            "table_name": "my_table",
            "schema_name": "s_public",
            "database_name": "d_public",
            "database": "d_public",
        },
    ],
    ids=lambda x: x["name"],
)
def test_forward_compatibility_changes_raise_warning(asset: dict):
    """If the datasource fields result in a forward compatibility change, a warning should be raised."""
    with pytest.warns(GxDatasourceWarning):
        ds = SnowflakeDatasource(
            name="my_sf_ds",
            connection_string="snowflake://my_user:password@account.us-east-1/d_public/s_public",
            assets=[asset],
        )
        print(ds)
    assert ds


# TODO: Cleanup how we install test dependencies and remove this skipif
@pytest.mark.skipif(
    True if not snowflake else False, reason="snowflake is not installed"
)
@pytest.mark.unit
def test_get_execution_engine_succeeds():
    connection_string = (
        "snowflake://my_user:password@myAccount.us-east-1/my_db/my_schema"
    )
    datasource = SnowflakeDatasource(
        name="my_snowflake", connection_string=connection_string
    )
    # testing that this doesn't raise an exception
    datasource.get_execution_engine()


@pytest.mark.snowflake
@pytest.mark.parametrize(
    "connection_string",
    [
        param(
            "snowflake://my_user:password@account.us-east-1/my_db/my_schema?numpy=True",
            id="connection_string str",
        ),
        param(
            {
                "user": "my_user",
                "password": "password",
                "account": "account.us-east-1",
                "database": "foo",
                "schema": "bar",
            },
            id="connection_string dict",
        ),
    ],
)
@pytest.mark.parametrize(
    "context_fixture_name,expected_query_param",
    [
        param(
            "empty_file_context",
            "great_expectations_core",
            id="file context",
        ),
        param(
            "empty_cloud_context_fluent",
            "great_expectations_platform",
            id="cloud context",
        ),
    ],
)
def test_get_engine_correctly_sets_application_query_param(
    request,
    context_fixture_name: str,
    expected_query_param: str,
    connection_string: str | dict,
):
    context = request.getfixturevalue(  # TODO: fix this and make it a fixture in the root conftest
        context_fixture_name
    )
    my_sf_ds = SnowflakeDatasource(name="my_sf_ds", connection_string=connection_string)
    my_sf_ds._data_context = context

    sql_engine = my_sf_ds.get_engine()
    application_query_param = sql_engine.url.query.get("application")
    assert application_query_param == expected_query_param


@pytest.mark.snowflake
@pytest.mark.parametrize(
    ["config", "expected_called_with"],
    [
        param(
            {
                "name": "std connection_str",
                "connection_string": "snowflake://user:password@account/db/schema?warehouse=wh&role=role",
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
                    "account": "account.us-east-1",
                    "database": "db",
                    "schema": "schema",
                    "warehouse": "wh",
                    "role": "role",
                },
            },
            {
                "url": "snowflake://user:password@account.us-east-1/db/schema?application=great_expectations_core&role=role&warehouse=wh",
            },
            id="std connection_string dict",
        ),
        param(
            {
                "name": "conn str with connect_args",
                "connection_string": "snowflake://user:password@account.us-east-1/db/schema?warehouse=wh&role=role",
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
                    "account": "account.us-east-1",
                    "database": "db",
                    "schema": "schema",
                    "warehouse": "wh",
                    "role": "role",
                },
                "kwargs": {"connect_args": {"private_key": b"my_key"}},
            },
            {
                "connect_args": {"private_key": b"my_key"},
                "url": "snowflake://user:password@account.us-east-1/db/schema?application=great_expectations_core&role=role&warehouse=wh",
            },
            id="connection_string dict with connect_args",
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

    datasource = ephemeral_context_with_defaults.sources.add_snowflake(**config)
    print(datasource)
    engine = datasource.get_engine()
    print(engine)

    create_engine_spy.assert_called_once_with(**expected_called_with)


@pytest.mark.unit
class TestConnectionTest:
    @pytest.mark.parametrize(
        "connection_string",
        [
            "snowflake://user:password@xy12345.us-east-2.aws",
            {
                "user": "user",
                "account": "xy12345.us-east-1.aws",
                "password": "password",
            },
        ],
        ids=type,
    )
    def test_w_valid_account_identifier_format(
        self, sql_ds_test_connection_always_fail: None, connection_string: str | dict
    ):
        sf = SnowflakeDatasource(
            name="my_sf",
            connection_string=connection_string,
        )
        with pytest.raises(TestConnectionError, match="always fail") as exc_info:
            sf.test_connection()

        assert not exc_info.value.__cause__, "The error has no initial cause"

    @pytest.mark.parametrize(
        "connection_string",
        [
            "snowflake://user:password@invalid_format",
            {
                "user": "user",
                "account": "invalid_format",
                "password": "password",
            },
        ],
        ids=type,
    )
    def test_warns_about_account_identifier_format(
        self, sql_ds_test_connection_always_fail: None, connection_string: str | dict
    ):
        sf = SnowflakeDatasource(name="my_sf", connection_string=connection_string)
        with pytest.raises(
            TestConnectionError,
            match=f"Account identifier 'invalid_format' does not match expected format {AccountIdentifier.FORMATS} ; it MAY be invalid. "
            "https://docs.snowflake.com/en/user-guide/admin-account-identifier",
        ) as exc_info:
            sf.test_connection()

        print(f"{exc_info.value!r}")
        assert (
            repr(exc_info.value.__cause__) == "TestConnectionError('always fail')"
        ), "Original Error should be preserved"


@pytest.mark.snowflake
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
                assert (
                    not datasource.schema_
                ), "Don't expect schema to be available without config_provider"
            # attach context to enable config substitution
            datasource._data_context = ephemeral_context_with_defaults
            _ = datasource.schema_
        else:
            assert datasource.schema_ == datasource.connection_string.schema_

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
                assert (
                    not datasource.database
                ), "Don't expect database to be available without config_provider"
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
                assert (
                    not datasource.warehouse
                ), "Don't expect warehouse to be available without config_provider"
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
                assert (
                    not datasource.role
                ), "Don't expect role to be available without config_provider"
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
                assert (
                    not datasource.account
                ), "Don't expect account to be available without config_provider"
            # attach context to enable config substitution
            datasource._data_context = ephemeral_context_with_defaults
            _ = datasource.account
        else:
            assert datasource.account == datasource.connection_string.account


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
