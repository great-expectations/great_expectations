"""
Reproduction for community issue #11199.

Spark CSV assets fail to recognize columns whose names contain a dot (e.g.
``Data.Entrega``). Any expectation targeting such a column raises
``The column "Data.Entrega" in BatchData does not exist`` because Spark SQL
treats ``.`` as nested field access unless the identifier is backtick-quoted.

Also covers issue #12196: requesting ``unexpected_index_column_names`` for
such a column returned an empty result.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

import great_expectations.expectations as gxe
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config import (
    SparkFilesystemCsvDatasourceTestConfig,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import Batch

COLUMN_WITH_DOT = "Data.Entrega"

DATA = pd.DataFrame(
    {
        COLUMN_WITH_DOT: ["2024-01-01", "2024-02-01", "2024-03-01"],
    }
)


@parameterize_batch_for_data_sources(
    data_source_configs=[SparkFilesystemCsvDatasourceTestConfig()],
    data=DATA,
)
def test_spark_column_with_dot_in_name_is_recognized(batch_for_datasource: Batch) -> None:
    """Spark should recognize columns whose names contain a dot.

    Reported in community issue #11199: validating any expectation on a
    column like ``Data.Entrega`` raises ``The column "Data.Entrega" in
    BatchData does not exist`` because Spark parses the dot as nested field
    access.
    """
    result = batch_for_datasource.validate(
        gxe.ExpectColumnValuesToNotBeNull(column=COLUMN_WITH_DOT)
    )
    assert result.success


ID_COLUMN = "ID"

DATA_WITH_A_NULL = pd.DataFrame(
    {
        ID_COLUMN: [1, 2],
        COLUMN_WITH_DOT: ["2024-01-01", None],
    }
)


@parameterize_batch_for_data_sources(
    data_source_configs=[SparkFilesystemCsvDatasourceTestConfig()],
    data=DATA_WITH_A_NULL,
)
def test_spark_column_with_dot_in_name_returns_unexpected_index_list(
    batch_for_datasource: Batch,
) -> None:
    """A dotted domain column must not empty the result when index columns are requested.

    Reported in issue #12196.
    """
    result = batch_for_datasource.validate(
        gxe.ExpectColumnValuesToNotBeNull(column=COLUMN_WITH_DOT),
        result_format={
            "result_format": "COMPLETE",
            "unexpected_index_column_names": [ID_COLUMN],
        },
    )
    assert result.result, result.exception_info
    assert result.result["unexpected_count"] == 1
    assert result.result["unexpected_index_list"] == [{ID_COLUMN: 2, COLUMN_WITH_DOT: None}]
    assert f"`{COLUMN_WITH_DOT}`" in result.result["unexpected_index_query"]


OTHER_COLUMN_WITH_DOT = "Data.Prevista"

PAIR_DATA = pd.DataFrame(
    {
        ID_COLUMN: [1, 2],
        COLUMN_WITH_DOT: ["a", "b"],
        OTHER_COLUMN_WITH_DOT: ["a", "c"],
    }
)


@parameterize_batch_for_data_sources(
    data_source_configs=[SparkFilesystemCsvDatasourceTestConfig()],
    data=PAIR_DATA,
)
def test_spark_column_pair_with_dots_in_names_returns_unexpected_index_list(
    batch_for_datasource: Batch,
) -> None:
    """Dotted column-pair domains must also return the full result with index columns.

    Reported in issue #12196.
    """
    result = batch_for_datasource.validate(
        gxe.ExpectColumnPairValuesToBeEqual(
            column_A=COLUMN_WITH_DOT, column_B=OTHER_COLUMN_WITH_DOT
        ),
        result_format={
            "result_format": "COMPLETE",
            "unexpected_index_column_names": [ID_COLUMN],
        },
    )
    assert result.result, result.exception_info
    assert result.result["unexpected_count"] == 1
    assert result.result["unexpected_index_list"] == [
        {ID_COLUMN: 2, COLUMN_WITH_DOT: "b", OTHER_COLUMN_WITH_DOT: "c"}
    ]
