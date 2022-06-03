from great_expectations.execution_engine.sqlalchemy_dialect import GESqlDialect


def test_dialect_instantiation_with_string():
    assert GESqlDialect("hive") == GESqlDialect.HIVE


def test_dialect_instantiation_with_byte_string():
    assert GESqlDialect(b"hive") == GESqlDialect.HIVE
