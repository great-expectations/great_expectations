from great_expectations.execution_engine.sqlalchemy_dialect import GESqlDialect


def test_dialect_instantiation_with_string():
    assert GESqlDialect("hive") == GESqlDialect.HIVE


def test_dialect_instantiation_with_byte_string():
    assert GESqlDialect(b"hive") == GESqlDialect.HIVE


def test_get_all_dialect_names_no_unsupported():
    assert GESqlDialect.UNSUPPORTED.value not in GESqlDialect.get_all_dialect_names()


def test_get_all_dialects_no_unsupported():
    assert GESqlDialect.UNSUPPORTED not in GESqlDialect.get_all_dialects()
