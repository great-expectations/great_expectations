from typing import Generator
import pytest

from pytest import param as p
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations import get_context
from great_expectations.data_context import EphemeralDataContext


@pytest.fixture
def context() -> EphemeralDataContext:
    ctx = get_context(cloud_mode=False)
    assert isinstance(ctx, EphemeralDataContext)
    return ctx


@pytest.fixture(
    scope="function",
    params=[
        p(
            {
                "name": "my_postgres",
                "type": "postgres",
                "connection_string": "postgresql+psycopg2://postgres:postgres@localhost:5432/mercury",
            },
            id="postgres",
        )
    ],
)
def datasources(context, request) -> Generator[SQLDatasource, None, None]:
    ds_type = request.param["type"]
    factory_method = getattr(context.sources, f"add_{ds_type}")
    ds = factory_method(**request.param)
    yield ds


@pytest.mark.parametrize(
    ["asset_config"],
    [
        p(
            {"name": "unquoted_lower", "table_name": "checkpoints"},
            id="unquoted_lower",
        ),
        p(
            {"name": "quoted_lower", "table_name": "'checkpoints'"},
            id="quoted_lower",
        ),
        p(
            {"name": "unqouted_upper", "table_name": "CHECKPOINTS"},
            id="unqouted_upper",
        ),
        p(
            {"name": "qouted_upper", "table_name": "'CHECKPOINTS'"},
            id="qouted_upper",
        ),
    ],
)
class TestIndentifiers:
    def test_add_table_asset(self, datasources: SQLDatasource, asset_config: dict):
        print(datasources)
        print(asset_config)

        asset = datasources.add_table_asset(**asset_config)
        print(asset)


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
