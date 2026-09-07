from typing import Sequence

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.compatibility import pydantic
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    DatabricksDatasourceTestConfig,
    DataSourceTestConfig,
    GenericSQLDatasourceTestConfig,
    MySQLDatasourceTestConfig,
    PandasDataFrameDatasourceTestConfig,
    PandasFilesystemCsvDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    RedshiftDatasourceTestConfig,
    SnowflakeDatasourceTestConfig,
    SparkFilesystemCsvDatasourceTestConfig,
    SqliteDatasourceTestConfig,
)

COL_A = "col_a"
COL_B = "col_b"


DATA = pd.DataFrame(
    {
        COL_A: ["aa", "ab", "ac", None],
        COL_B: ["aa", "bb", "cc", None],
    }
)

SUPPORTED_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    PandasDataFrameDatasourceTestConfig(),
    PandasFilesystemCsvDatasourceTestConfig(),
    DatabricksDatasourceTestConfig(),
    MySQLDatasourceTestConfig(),
    PostgreSQLDatasourceTestConfig(),
    RedshiftDatasourceTestConfig(),
    GenericSQLDatasourceTestConfig(),
    SnowflakeDatasourceTestConfig(),
    SparkFilesystemCsvDatasourceTestConfig(),
    SqliteDatasourceTestConfig(),
]


@parameterize_batch_for_data_sources(data_source_configs=SUPPORTED_DATA_SOURCES, data=DATA)
def test_golden_path(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToNotMatchRegexList(column=COL_A, regex_list=["x.", "a.."])
    result = batch_for_datasource.validate(expectation)
    assert result.success


class TestNormalSql:
    @pytest.mark.parametrize(
        "expectation",
        [
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchRegexList(column=COL_A, regex_list=["a[x-z]"]),
                id="non_matching",
            ),
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchRegexList(
                    column=COL_A,
                    regex_list=["x.", "a.."],
                ),
                id="multiple_non_matching",
            ),
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchRegexList(
                    column=COL_B, regex_list=["a."], mostly=0.6
                ),
                id="mostly",
            ),
        ],
    )
    @parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
    def test_success(
        self,
        batch_for_datasource: Batch,
        expectation: gxe.ExpectColumnValuesToNotMatchRegexList,
    ) -> None:
        result = batch_for_datasource.validate(expectation)
        assert result.success

    @pytest.mark.parametrize(
        "expectation",
        [
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchRegexList(column=COL_A, regex_list=["^a[abc]$"]),
                id="all_matches",
            ),
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchRegexList(
                    column=COL_A,
                    regex_list=["a.", "x."],
                ),
                id="one_matching",
            ),
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchRegexList(
                    column=COL_A,
                    regex_list=["a.", "\\w+"],
                ),
                id="all_matching",
            ),
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchRegexList(
                    column=COL_B, regex_list=["a."], mostly=0.7
                ),
                id="mostly_threshold_not_met",
            ),
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchRegexList(column=COL_A, regex_list=[""]),
                id="empty_regex",
            ),
        ],
    )
    @parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
    def test_failure(
        self,
        batch_for_datasource: Batch,
        expectation: gxe.ExpectColumnValuesToNotMatchRegexList,
    ) -> None:
        result = batch_for_datasource.validate(expectation)
        assert not result.success

    @parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
    def test_include_unexpected_rows_pandas(self, batch_for_datasource: Batch) -> None:
        """Test include_unexpected_rows for ExpectColumnValuesToNotMatchRegexList."""
        expectation = gxe.ExpectColumnValuesToNotMatchRegexList(
            column=COL_A, regex_list=["^a[abc]$"]
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

        # Should contain 3 rows where COL_A matches regex_list ["^a[abc]$"]
        # ("aa", "ab", "ac" all match)
        assert len(unexpected_rows_df) == 3

        # The unexpected rows should contain all the matching values
        unexpected_values = sorted(unexpected_rows_df[COL_A].tolist())
        assert unexpected_values == ["aa", "ab", "ac"]

    @parameterize_batch_for_data_sources(
        data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
    )
    def test_include_unexpected_rows_sql(self, batch_for_datasource: Batch) -> None:
        """Test include_unexpected_rows for ExpectColumnValuesToNotMatchRegexList with SQL."""
        expectation = gxe.ExpectColumnValuesToNotMatchRegexList(
            column=COL_A, regex_list=["^a[abc]$"]
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

        # Should contain 3 rows where COL_A matches regex_list ["^a[abc]$"]
        assert len(unexpected_rows_data) == 3

        # Check that all matching values appear in the unexpected rows data
        unexpected_rows_str = str(unexpected_rows_data)
        for value in ["aa", "ab", "ac"]:
            assert value in unexpected_rows_str


@pytest.mark.unit
def test_invalid_config() -> None:
    with pytest.raises(pydantic.ValidationError):
        gxe.ExpectColumnValuesToNotMatchRegexList(column=COL_A, regex_list=[])
