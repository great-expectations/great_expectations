import pytest

from great_expectations.execution_engine.sqlalchemy_dialect import GESqlDialect


@pytest.mark.unit
def test_dialect_instantiation_with_string():
    assert GESqlDialect("hive") == GESqlDialect.HIVE


@pytest.mark.unit
def test_dialect_instantiation_with_byte_string():
    assert GESqlDialect(b"hive") == GESqlDialect.HIVE


@pytest.mark.unit
def test_string_equivalence():
    assert GESqlDialect.HIVE == "hive"


@pytest.mark.unit
def test_byte_string_equivalence():
    assert GESqlDialect.HIVE == b"hive"


@pytest.mark.unit
def test_get_all_dialect_names_no_other_dialects():
    assert GESqlDialect.OTHER.value not in GESqlDialect.get_all_dialect_names()


@pytest.mark.unit
def test_get_all_dialects_no_other_dialects():
    assert GESqlDialect.OTHER not in GESqlDialect.get_all_dialects()
