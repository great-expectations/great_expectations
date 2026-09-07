"""Regression tests for query assets carrying a daily or monthly Batch Definition.

GX wraps a query asset's raw SQL in a subquery before running anything against it. Rebuilding
that statement as a select over everything after its "SELECT" produces a select that owns no
FROM clause, and Oracle completes such a select by appending "FROM DUAL" -- so the wrapped
query went out with two FROM clauses and the database rejected it.

The unpartitioned query asset was exempted from the wrapping and so escaped this. A daily or
monthly Batch Definition is not exempt, and adding one to a query asset failed before a batch
was ever read: the Batch Definition is validated by probing the partition column through that
same selectable.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import pandas as pd

import great_expectations.expectations as gxe
from great_expectations.datasource.fluent.sql_datasource import SQLDatasource, TableAsset
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config import (
    OracleDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    SqliteDatasourceTestConfig,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import Batch

RECORD_DATE_COL = "record_date"
LABEL_COL = "label"

OUTSIDE_MONTH = "outside_month"
MARCH_FIRST = "march_first"
MARCH_SECOND_A = "march_second_a"
MARCH_SECOND_B = "march_second_b"

# Values are chosen so a daily and a monthly partition select different, known row sets: the
# daily partition (2024-03-02) returns a strict subset of the monthly partition (2024-03), and
# neither equals the full frame, which also carries a row outside the target month entirely.
# Without that spread, a selectable that ignored the partition clause would still pass.
DATA = pd.DataFrame(
    {
        RECORD_DATE_COL: [
            date(2023, 6, 15),
            date(2024, 3, 1),
            date(2024, 3, 2),
            date(2024, 3, 2),
        ],
        LABEL_COL: [OUTSIDE_MONTH, MARCH_FIRST, MARCH_SECOND_A, MARCH_SECOND_B],
    }
)

DATA_SOURCES = [
    SqliteDatasourceTestConfig(),
    PostgreSQLDatasourceTestConfig(),
    OracleDatasourceTestConfig(),
]


def _add_query_asset(batch_for_datasource: Batch, name: str):
    """Build a query asset selecting the whole fixture table, through its own datasource.

    Identifiers are quoted the way the connected dialect quotes them rather than pasted in
    bare: the harness creates lower-case identifiers, which a dialect that folds unquoted
    names to upper case (Oracle) would otherwise fail to resolve.
    """
    asset = batch_for_datasource.data_asset
    assert isinstance(asset, TableAsset)
    datasource = batch_for_datasource.datasource
    assert isinstance(datasource, SQLDatasource)

    quote = datasource.get_engine().dialect.identifier_preparer.quote
    query = f"SELECT {quote(RECORD_DATE_COL)}, {quote(LABEL_COL)} FROM {quote(asset.table_name)}"
    return datasource.add_query_asset(name=name, query=query)


@parameterize_batch_for_data_sources(data_source_configs=DATA_SOURCES, data=DATA)
def test_query_asset_daily_batch_definition(batch_for_datasource: Batch) -> None:
    """A daily Batch Definition on a query asset selects that day's rows and no others."""
    query_asset = _add_query_asset(batch_for_datasource, "daily_query_asset")

    batch_definition = query_asset.add_batch_definition_daily(
        "daily_query_asset_bd", column=RECORD_DATE_COL
    )
    batch = batch_definition.get_batch(batch_parameters={"year": 2024, "month": 3, "day": 2})
    result = batch.validate(
        gxe.ExpectColumnDistinctValuesToEqualSet(
            column=LABEL_COL, value_set=[MARCH_SECOND_A, MARCH_SECOND_B]
        )
    )

    assert result.success, result.exception_info


@parameterize_batch_for_data_sources(data_source_configs=DATA_SOURCES, data=DATA)
def test_query_asset_monthly_batch_definition(batch_for_datasource: Batch) -> None:
    """A monthly Batch Definition on a query asset selects that month's rows and no others."""
    query_asset = _add_query_asset(batch_for_datasource, "monthly_query_asset")

    batch_definition = query_asset.add_batch_definition_monthly(
        "monthly_query_asset_bd", column=RECORD_DATE_COL
    )
    batch = batch_definition.get_batch(batch_parameters={"year": 2024, "month": 3})
    result = batch.validate(
        gxe.ExpectColumnDistinctValuesToEqualSet(
            column=LABEL_COL, value_set=[MARCH_FIRST, MARCH_SECOND_A, MARCH_SECOND_B]
        )
    )

    assert result.success, result.exception_info
