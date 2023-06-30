from __future__ import annotations

import pytest

from great_expectations.datasource.fluent import SnowflakeDatasource


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
    connection_string: str | None, connect_args: dict
):
    with pytest.raises(ValueError):
        _ = SnowflakeDatasource(connection_string=connection_string, **connect_args)
