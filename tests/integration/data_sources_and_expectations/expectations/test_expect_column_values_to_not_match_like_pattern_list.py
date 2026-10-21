from typing import Sequence

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config import (
    DataSourceTestConfig,
    GenericSQLDatasourceTestConfig,
    MySQLDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    RedshiftDatasourceTestConfig,
    SnowflakeDatasourceTestConfig,
    SqliteDatasourceTestConfig,
    SQLServerDatasourceTestConfig,
)

COL_NAME = "col_name"
NO_LITERAL_UNDERSCORE = "no_literal_underscore"


DATA = pd.DataFrame(
    {
        COL_NAME: ["aa", "ab", "ac", None],
        # No row holds a literal underscore, so an escaped pattern must match nothing.
        NO_LITERAL_UNDERSCORE: ["axb", "ayb", "a%b", None],
    }
)

REGULAR_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    MySQLDatasourceTestConfig(),
    PostgreSQLDatasourceTestConfig(),
    RedshiftDatasourceTestConfig(),
    GenericSQLDatasourceTestConfig(),
    SnowflakeDatasourceTestConfig(),
    SqliteDatasourceTestConfig(),
]


class TestNormalSql:
    @pytest.mark.parametrize(
        "expectation",
        [
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchLikePatternList(
                    column=COL_NAME, like_pattern_list=["bc"]
                ),
                id="one_pattern",
            ),
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchLikePatternList(
                    column=COL_NAME, like_pattern_list=["bc", "%de%"]
                ),
                id="multiple_patterns",
            ),
        ],
    )
    @parameterize_batch_for_data_sources(data_source_configs=REGULAR_DATA_SOURCES, data=DATA)
    def test_success(
        self,
        batch_for_datasource: Batch,
        expectation: gxe.ExpectColumnValuesToNotMatchLikePatternList,
    ) -> None:
        result = batch_for_datasource.validate(expectation)
        assert result.success

    @pytest.mark.parametrize(
        "expectation",
        [
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchLikePatternList(
                    column=COL_NAME, like_pattern_list=["%a%"]
                ),
                id="one_pattern",
            ),
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchLikePatternList(
                    column=COL_NAME, like_pattern_list=["%a%", "not_this"]
                ),
                id="matches_one_pattern_but_not_all",
            ),
        ],
    )
    @parameterize_batch_for_data_sources(data_source_configs=REGULAR_DATA_SOURCES, data=DATA)
    def test_failure(
        self,
        batch_for_datasource: Batch,
        expectation: gxe.ExpectColumnValuesToNotMatchLikePatternList,
    ) -> None:
        """A value is expected only when it matches none of the patterns.

        The matches_one_pattern_but_not_all case pins that reading. Every value matches
        "%a%" and none match "not_this", so an implementation that flagged only values
        matching every pattern would report success here. Matching none of the patterns
        is the only semantics this Expectation offers.
        """
        result = batch_for_datasource.validate(expectation)
        assert not result.success

    @parameterize_batch_for_data_sources(
        data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
    )
    def test_include_unexpected_rows_postgres(self, batch_for_datasource: Batch) -> None:
        """Test include_unexpected_rows for ExpectColumnValuesToNotMatchLikePatternList."""
        expectation = gxe.ExpectColumnValuesToNotMatchLikePatternList(
            column=COL_NAME, like_pattern_list=["%a%"]
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

        # Should contain 3 rows where COL_NAME matches like_pattern_list ["%a%"]
        # ("aa", "ab", "ac" all contain 'a')
        assert len(unexpected_rows_data) == 3

        # Check that "aa", "ab", and "ac" appear in the unexpected rows data
        unexpected_rows_str = str(unexpected_rows_data)
        assert "aa" in unexpected_rows_str
        assert "ab" in unexpected_rows_str
        assert "ac" in unexpected_rows_str


class TestSQLServer:
    @pytest.mark.parametrize(
        "expectation",
        [
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchLikePatternList(
                    column=COL_NAME, like_pattern_list=["bc"]
                ),
                id="one_pattern",
            ),
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchLikePatternList(
                    column=COL_NAME, like_pattern_list=["bc", "%de%"]
                ),
                id="multiple_patterns",
            ),
        ],
    )
    @parameterize_batch_for_data_sources(
        data_source_configs=[SQLServerDatasourceTestConfig()], data=DATA
    )
    def test_success(
        self,
        batch_for_datasource: Batch,
        expectation: gxe.ExpectColumnValuesToNotMatchLikePatternList,
    ) -> None:
        result = batch_for_datasource.validate(expectation)
        assert result.success

    @pytest.mark.parametrize(
        "expectation",
        [
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchLikePatternList(
                    column=COL_NAME, like_pattern_list=["%a[b]%"]
                ),
                id="one_pattern",
            ),
            pytest.param(
                gxe.ExpectColumnValuesToNotMatchLikePatternList(
                    column=COL_NAME, like_pattern_list=["%[a]%", "not_this"]
                ),
                id="matches_one_pattern_but_not_all",
            ),
        ],
    )
    @parameterize_batch_for_data_sources(
        data_source_configs=[SQLServerDatasourceTestConfig()], data=DATA
    )
    def test_failure(
        self,
        batch_for_datasource: Batch,
        expectation: gxe.ExpectColumnValuesToNotMatchLikePatternList,
    ) -> None:
        result = batch_for_datasource.validate(expectation)
        assert not result.success


@parameterize_batch_for_data_sources(data_source_configs=REGULAR_DATA_SOURCES, data=DATA)
def test_escape_makes_underscore_literal(batch_for_datasource: Batch) -> None:
    """The escape character must reach every pattern of a NOT LIKE list.

    Unescaped, 'a_b' matches every row and the expectation fails; escaped, it means a
    literal underscore that no row contains, so it succeeds.
    """
    unescaped = gxe.ExpectColumnValuesToNotMatchLikePatternList(
        column=NO_LITERAL_UNDERSCORE, like_pattern_list=["a_b"]
    )
    assert not batch_for_datasource.validate(unescaped).success

    escaped = gxe.ExpectColumnValuesToNotMatchLikePatternList(
        column=NO_LITERAL_UNDERSCORE, like_pattern_list=["a!_b"], escape="!"
    )
    assert batch_for_datasource.validate(escaped).success
