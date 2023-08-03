from __future__ import annotations

from typing import Final, Generator, Literal

import pytest
from pytest import param as p

from great_expectations import get_context
from great_expectations.data_context import EphemeralDataContext
from great_expectations.datasource.fluent import (
    PostgresDatasource,
    SnowflakeDatasource,
    SQLDatasource,
)
from great_expectations.expectations.expectation import (
    ExpectationConfiguration,
)

pg_table: Final[str] = "checkpoints"
snowflake_table: Final[str] = "nyc_tlc__yellow__bronze"
trino_table: Final[str] = "customer"


TABLE_NAME_MAPPING: Final[
    dict[
        type[SQLDatasource],
        dict[
            Literal["unquoted_lower", "quoted_lower", "unquoted_upper", "quoted_upper"],
            str,
        ],
    ]
] = {
    PostgresDatasource: {
        "unquoted_lower": pg_table.lower(),
        "quoted_lower": f"'{pg_table.lower()}'",
        "unquoted_upper": pg_table.upper(),
        "quoted_upper": f"'{pg_table.upper()}'",
    },
    SnowflakeDatasource: {
        "unquoted_lower": snowflake_table.lower(),
        "quoted_lower": f"'{snowflake_table.lower()}'",
        "unquoted_upper": snowflake_table.upper(),
        "quoted_upper": f"'{snowflake_table.upper()}'",
    },
    SQLDatasource: {  # There is no TrinoDatasource
        "unquoted_lower": trino_table.lower(),
        "quoted_lower": f"'{trino_table.lower()}'",
        "unquoted_upper": trino_table.upper(),
        "quoted_upper": f"'{trino_table.upper()}'",
    },
}


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
        ),
        p(
            {
                "name": "my_trino",
                "type": "sql",
                "connection_string": "trino://user:@localhost:8088/tpch/sf1",
            },
            id="trino",
        ),
        p(
            {
                "name": "my_snowflake",
                "type": "snowflake",
                # set env vars or replace with actual values
                "connection_string": r"snowflake://${SF_USERNAME}:${SF_PASSWORD}"
                r"@${SF_ACCOUNT}/${SF_DB}}?warehouse=${SF_WAREHOUSE}>&role=${SF_ROLE}",
            },
            marks=[pytest.mark.xfail(reason="snowflake fix not implemented")],
            id="snowflake",
        ),
    ],
)
def datasources(
    context: EphemeralDataContext, request: pytest.FixtureRequest
) -> Generator[SQLDatasource, None, None]:
    ds_type = request.param["type"]
    factory_method = getattr(context.sources, f"add_{ds_type}")
    ds = factory_method(**request.param)
    yield ds


@pytest.fixture
def ds_w_assets(datasources: SQLDatasource) -> Generator[SQLDatasource, None, None]:
    datasources.add_table_asset(
        "unquoted_lower_asset",
        table_name=TABLE_NAME_MAPPING[type(datasources)]["unquoted_lower"],
    )

    yield datasources


@pytest.mark.parametrize(
    ["asset_name"],
    [
        p("unquoted_lower"),
        p("quoted_lower"),
        p(
            "unquoted_upper",
            marks=[
                pytest.mark.xfail(
                    reason="pg table names should be lowercase. Why doesn't sqla fix the casing?"
                )
            ],
        ),
        p(
            "quoted_upper",
            marks=[pytest.mark.xfail(reason="pg table names should be lowercase")],
        ),
    ],
)
class TestIndentifiers:
    def test_add_table_asset(
        self,
        datasources: SQLDatasource,
        asset_name: Literal[
            "unquoted_lower", "quoted_lower", "unquoted_upper", "quoted_upper"
        ],
    ):
        print(datasources)

        asset = datasources.add_table_asset(
            asset_name, table_name=TABLE_NAME_MAPPING[type(datasources)][asset_name]
        )
        print(asset)

    def test_run_checkpoint(
        self,
        context: EphemeralDataContext,
        datasources: SQLDatasource,
        asset_name: Literal[
            "unquoted_lower", "quoted_lower", "unquoted_upper", "quoted_upper"
        ],
    ):
        asset = datasources.add_table_asset(
            asset_name, table_name=TABLE_NAME_MAPPING[type(datasources)][asset_name]
        )
        suite = context.add_expectation_suite(
            expectation_suite_name=f"{datasources.name}-{asset.name}"
        )
        suite.add_expectation(
            expectation_configuration=ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={
                    "column": "val",
                    "mostly": 1,
                },
            )
        )

        checkpoint_name = f"{datasources.name}-{asset.name}"
        print(f"WTF: {asset.name}")

        checkpoint_config = {
            "name": checkpoint_name,
            "validations": [
                {
                    "expectation_suite_name": suite.expectation_suite_name,
                    "expectation_suite_ge_cloud_id": suite.ge_cloud_id,
                    "batch_request": {
                        "datasource_name": datasources.name,
                        "data_asset_name": asset.name,
                    },
                }
            ],
        }
        checkpoint = context.add_checkpoint(**checkpoint_config)
        result = checkpoint.run()
        assert result.success is True


def test_does_it_work(context: EphemeralDataContext, ds_w_assets: SQLDatasource):
    asset = ds_w_assets.get_asset("unquoted_lower_asset")

    suite = context.add_expectation_suite(
        expectation_suite_name=f"{ds_w_assets.name}-{asset.name}"
    )
    suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": "val",
                "mostly": 1,
            },
        )
    )

    checkpoint_name = f"{ds_w_assets.name}-{asset.name}"
    print(f"WTF: {asset.name}")

    checkpoint_config = {
        "name": checkpoint_name,
        "validations": [
            {
                "expectation_suite_name": suite.expectation_suite_name,
                "expectation_suite_ge_cloud_id": suite.ge_cloud_id,
                "batch_request": {
                    "datasource_name": ds_w_assets.name,
                    "data_asset_name": asset.name,
                },
            }
        ],
    }
    checkpoint = context.add_checkpoint(**checkpoint_config)
    result = checkpoint.run()
    assert result.success is True


if __name__ == "__main__":
    pytest.main(
        [
            __file__,
            "-vv",
            "-rxXpEf",
            # "--sw",
        ]
    )
