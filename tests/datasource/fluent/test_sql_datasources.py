from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy
import pytest

from great_expectations.datasource.fluent import SQLDatasource

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@pytest.mark.unit
def test_kwargs_are_passed_to_create_engine(mocker: MockerFixture):

    create_engine_spy = mocker.spy(sqlalchemy, "create_engine")

    ds = SQLDatasource(
        name="my_datasource",
        connection_string="sqlite:///",
        kwargs={"foo": "bar", "fizz": "buzz"},
    )
    print(ds)
    ds.test_connection()

    assert create_engine_spy.called


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
