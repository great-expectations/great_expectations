from great_expectations.expectations.row_conditions import (
    _parse_great_expectations_condition,
    parse_condition_to_spark,
    parse_condition_to_sqlalchemy,
)


def test_notnull_parser():
    res = _parse_great_expectations_condition('col("foo").notNull()')
    assert res["column"] == "foo"
    assert res["notnull"] is True

    res = _parse_great_expectations_condition('col("foo") > 5')
    assert res["column"] == "foo"
    assert "notnull" not in res


def test_condition_parser():
    res = _parse_great_expectations_condition('col("foo") > 5')
    assert res["column"] == "foo"
    assert res["op"] == ">"
    assert res["fnumber"] == "5"


def test_parse_condition_to_spark(spark_session):
    res = parse_condition_to_spark('col("foo") > 5')
    # This is mostly a demonstrative test; it may be brittle. I do not know how to test
    # a condition itself.
    assert str(res) == "Column<b'(foo > 5)'>"

    res = parse_condition_to_spark('col("foo").notNull()')
    # This is mostly a demonstrative test; it may be brittle. I do not know how to test
    # a condition itself.
    print(str(res))
    assert str(res) == "Column<b'(foo IS NOT NULL)'>"


def test_parse_condition_to_sqlalchemy(sa):
    res = parse_condition_to_sqlalchemy('col("foo") > 5')
    assert str(res) == "foo > :foo_1"

    res = parse_condition_to_sqlalchemy('col("foo").notNull()')
    assert str(res) == "foo IS NOT NULL"
