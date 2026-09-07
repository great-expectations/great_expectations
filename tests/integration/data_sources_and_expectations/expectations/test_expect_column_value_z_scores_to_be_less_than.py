from unittest.mock import ANY

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
    NON_SQL_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    SQL_DATA_SOURCES,
    PostgreSQLDatasourceTestConfig,
)

BASIC_COL = "basic"
DISTRIBUTION_WITH_OUTLIER = "with_outlier"
MOSTLY_ZERO_DISTRIBUTION = "mostly_zero"
DISTRIBUTION_WITH_NULLS = "lotta_nulls"

DATA = pd.DataFrame(
    {
        BASIC_COL: [1, 1, 1, 3, 3],
        DISTRIBUTION_WITH_OUTLIER: [-1000000, -1, 0, 1, 1],
        MOSTLY_ZERO_DISTRIBUTION: [1, 0, 0, 0, 0],
        DISTRIBUTION_WITH_NULLS: [-1, 0, 1, None, None],
    },
    dtype="object",
)


@parameterize_batch_for_data_sources(data_source_configs=NON_SQL_DATA_SOURCES, data=DATA)
def test_success_complete__non_sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValueZScoresToBeLessThan(
        column=BASIC_COL, threshold=1.96, double_sided=True
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success


@parameterize_batch_for_data_sources(data_source_configs=SQL_DATA_SOURCES, data=DATA)
def test_success_complete__sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValueZScoresToBeLessThan(
        column=BASIC_COL, threshold=1.96, double_sided=True
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {
        "element_count": 5,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "partial_unexpected_list": [],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 0.0,
        "unexpected_percent_nonmissing": 0.0,
        "partial_unexpected_counts": [],
        "unexpected_list": [],
        "unexpected_index_query": ANY,
    }


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValueZScoresToBeLessThan(
                column=BASIC_COL, threshold=1.96, double_sided=True
            ),
            id="basic_successful_test",
        ),
        pytest.param(
            gxe.ExpectColumnValueZScoresToBeLessThan(
                column=BASIC_COL, threshold=1.96, double_sided=True, mostly=0.8
            ),
            id="successful_test_with_mostly",
        ),
        pytest.param(
            gxe.ExpectColumnValueZScoresToBeLessThan(
                column=DISTRIBUTION_WITH_OUTLIER, threshold=1, double_sided=True, mostly=0.6
            ),
            id="outlier_test_with_mostly",
        ),
        pytest.param(
            gxe.ExpectColumnValueZScoresToBeLessThan(
                column=BASIC_COL, threshold=1.96, double_sided=False
            ),
            id="single_sided_test",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValueZScoresToBeLessThan,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValueZScoresToBeLessThan(
                column=BASIC_COL, threshold=0.1, double_sided=True
            ),
            id="threshold_too_low",
        ),
        pytest.param(
            gxe.ExpectColumnValueZScoresToBeLessThan(
                column=DISTRIBUTION_WITH_OUTLIER, threshold=1, double_sided=True
            ),
            id="extreme_outlier",
        ),
        pytest.param(
            gxe.ExpectColumnValueZScoresToBeLessThan(
                column=MOSTLY_ZERO_DISTRIBUTION, threshold=1, double_sided=True, mostly=0.9
            ),
            id="mostly_requirement_not_met",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValueZScoresToBeLessThan,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows_pandas(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValueZScoresToBeLessThan."""
    expectation = gxe.ExpectColumnValueZScoresToBeLessThan(
        column=DISTRIBUTION_WITH_OUTLIER, threshold=1, double_sided=True
    )
    result = batch_for_datasource.validate(
        expectation, result_format={"result_format": "BASIC", "include_unexpected_rows": True}
    )

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_rows is present and contains the expected data
    assert "unexpected_rows" in result_dict
    assert result_dict["unexpected_rows"] is not None

    # Convert to DataFrame for easier comparison
    unexpected_rows_data = result_dict["unexpected_rows"]
    assert isinstance(unexpected_rows_data, pd.DataFrame)
    unexpected_rows_df = unexpected_rows_data

    # Should contain 1 row with the extreme outlier (-1000000 has high z-score)
    assert len(unexpected_rows_df) == 1
    assert list(unexpected_rows_df.index) == [0]

    # The unexpected row should have the outlier value
    assert unexpected_rows_df.loc[0, DISTRIBUTION_WITH_OUTLIER] == -1000000


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_include_unexpected_rows_sql(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValueZScoresToBeLessThan with SQL."""
    expectation = gxe.ExpectColumnValueZScoresToBeLessThan(
        column=DISTRIBUTION_WITH_OUTLIER, threshold=1, double_sided=True
    )
    result = batch_for_datasource.validate(
        expectation, result_format={"result_format": "BASIC", "include_unexpected_rows": True}
    )

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_rows is present and contains the expected data
    assert "unexpected_rows" in result_dict
    assert result_dict["unexpected_rows"] is not None

    unexpected_rows_data = result_dict["unexpected_rows"]
    assert isinstance(unexpected_rows_data, list)

    # Should contain 1 row with the extreme outlier (-1000000 has high z-score)
    assert len(unexpected_rows_data) == 1

    # Check that "-1000000" appears in the unexpected rows data
    unexpected_rows_str = str(unexpected_rows_data)
    assert "-1000000" in unexpected_rows_str
