from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import sqlalchemy

from great_expectations.datasource.fluent import SQLDatasource

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@pytest.mark.unit
def test_kwargs_are_passed_to_create_engine(mocker: MockerFixture):
    create_engine_spy = mocker.spy(sqlalchemy, "create_engine")

    ds = SQLDatasource(
        name="my_datasource",
        connection_string="sqlite:///",
        kwargs={"isolation_level": "SERIALIZABLE"},
    )
    print(ds)
    ds.test_connection()

    create_engine_spy.assert_called_once_with(
        "sqlite:///", **{"isolation_level": "SERIALIZABLE"}
    )


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
