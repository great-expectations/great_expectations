from types import ModuleType

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.expectations.expectation import Expectation
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config import (
    SingleStoreDatasourceTestConfig,
    SqliteDatasourceTestConfig,
)

LIKE_EXPECTATIONS = [
    pytest.param(
        gxe.ExpectColumnValuesToMatchLikePattern(column="value", like_pattern="a!_b"),
        id="match-one",
    ),
    pytest.param(
        gxe.ExpectColumnValuesToNotMatchLikePattern(column="value", like_pattern="a!_b"),
        id="not-match-one",
    ),
    pytest.param(
        gxe.ExpectColumnValuesToMatchLikePatternList(column="value", like_pattern_list=["a!_b"]),
        id="match-list",
    ),
    pytest.param(
        gxe.ExpectColumnValuesToNotMatchLikePatternList(column="value", like_pattern_list=["a!_b"]),
        id="not-match-list",
    ),
]
DATA = pd.DataFrame({"value": ["a_b", "axb", "a%b"]})


@pytest.mark.parametrize("expectation", LIKE_EXPECTATIONS)
@parameterize_batch_for_data_sources(data_source_configs=[SqliteDatasourceTestConfig()], data=DATA)
def test_unsupported_dialect_returns_descriptive_exception(
    batch_for_datasource: Batch, expectation: Expectation, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exercise metric resolution with the same module shape supplied by SQL engines."""

    class UnsupportedDialect:
        name = "unsupported"

    class UnsupportedDialectModule(ModuleType):
        dialect = UnsupportedDialect

    dialect = UnsupportedDialectModule("test_unsupported_dialect")
    monkeypatch.setattr(batch_for_datasource.data.execution_engine, "dialect_module", dialect)

    result = batch_for_datasource.validate(expectation)

    assert not result.success
    exceptions = []
    for info in result.exception_info.values():
        if info["raised_exception"]:
            exceptions.append(info)

    assert exceptions
    for info in exceptions:
        assert (
            info["exception_message"] == "Like patterns are not supported for dialect unsupported"
        )
        assert "NotImplementedError" in info["exception_traceback"]
        assert "AttributeError" not in info["exception_traceback"]


@pytest.mark.parametrize("expectation", LIKE_EXPECTATIONS)
@parameterize_batch_for_data_sources(
    data_source_configs=[SingleStoreDatasourceTestConfig()], data=DATA
)
def test_singlestore_explicit_escape_matches_underscore_literally(
    batch_for_datasource: Batch, expectation: Expectation
) -> None:
    """With `escape`, 'a!_b' must match only the literal 'a_b', as an ESCAPE clause would.

    SingleStore has no ESCAPE clause, so the pattern is translated to 'a\\_b'. Were the
    escape dropped instead, '!' would be an ordinary character and no row would match.
    """
    expectation = expectation.copy(update={"escape": "!"})
    result = batch_for_datasource.validate(expectation)

    assert result.exception_info["raised_exception"] is False
    if isinstance(
        expectation,
        (gxe.ExpectColumnValuesToMatchLikePattern, gxe.ExpectColumnValuesToMatchLikePatternList),
    ):
        assert sorted(result.result["partial_unexpected_list"]) == ["a%b", "axb"]
    else:
        assert result.result["partial_unexpected_list"] == ["a_b"]


@pytest.mark.parametrize(
    ("expectation", "unexpected_count"),
    [
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePattern(column="value", like_pattern=r"a\_b"),
            2,
            id="match-one",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToNotMatchLikePattern(column="value", like_pattern=r"a\_b"),
            1,
            id="not-match-one",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePatternList(
                column="value", like_pattern_list=[r"a\_b"]
            ),
            2,
            id="match-list",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToNotMatchLikePatternList(
                column="value", like_pattern_list=[r"a\_b"]
            ),
            1,
            id="not-match-list",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[SingleStoreDatasourceTestConfig()], data=DATA
)
def test_singlestore_native_backslash_escape(
    batch_for_datasource: Batch, expectation: Expectation, unexpected_count: int
) -> None:
    """The error's suggested workaround must distinguish a literal '_' from its wildcard."""
    result = batch_for_datasource.validate(expectation)

    assert not result.success
    assert result.result["unexpected_count"] == unexpected_count
    assert result.exception_info["raised_exception"] is False
