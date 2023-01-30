import pytest

from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect


@pytest.mark.unit
def test_dialect_instantiation_with_string():
    assert GXSqlDialect("hive") == GXSqlDialect.HIVE


@pytest.mark.unit
def test_dialect_instantiation_with_byte_string():
    assert GXSqlDialect(b"hive") == GXSqlDialect.HIVE


@pytest.mark.unit
def test_string_equivalence():
    assert GXSqlDialect.HIVE == "hive"


@pytest.mark.unit
def test_byte_string_equivalence():
    assert GXSqlDialect.HIVE == b"hive"


@pytest.mark.unit
def test_get_all_dialect_names_no_other_dialects():
    assert GXSqlDialect.OTHER.value not in GXSqlDialect.get_all_dialect_names()


@pytest.mark.unit
def test_get_all_dialects_no_other_dialects():
    assert GXSqlDialect.OTHER not in GXSqlDialect.get_all_dialects()
