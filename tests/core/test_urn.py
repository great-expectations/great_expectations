from urllib.parse import parse_qs

import pytest
from pyparsing import ParseException

from great_expectations.core.urn import ge_urn


def test_ge_validations_urn():
    # We should be able to parse validations urns
    urn = (
        "urn:great_expectations:validations:my_suite:expect_something.observed_value:query=s%20tring&query="
        "string3&query2=string2"
    )
    res = ge_urn.parseString(urn)

    assert res["urn_type"] == "validations"
    assert res["expectation_suite_name"] == "my_suite"
    assert res["metric_name"] == "expect_something.observed_value"
    kwargs_dict = parse_qs(res["metric_kwargs"])
    assert kwargs_dict == {"query": ["s tring", "string3"], "query2": ["string2"]}

    # no kwargs is ok
    urn = "urn:great_expectations:validations:my_suite:expect_something.observed_value"
    res = ge_urn.parseString(urn)

    assert res["urn_type"] == "validations"
    assert res["expectation_suite_name"] == "my_suite"
    assert res["metric_name"] == "expect_something.observed_value"
    assert "metric_kwargs" not in res


def test_ge_metrics_urn():
    urn = "urn:great_expectations:metrics:20200403T1234.324Z:my_suite:expect_something.observed_value:column=mycol"
    res = ge_urn.parseString(urn)

    assert res["urn_type"] == "metrics"
    assert res["run_id"] == "20200403T1234.324Z"
    assert res["expectation_suite_name"] == "my_suite"
    assert res["metric_name"] == "expect_something.observed_value"
    kwargs_dict = parse_qs(res["metric_kwargs"])
    assert kwargs_dict == {"column": ["mycol"]}

    # No kwargs is ok
    urn = "urn:great_expectations:metrics:20200403T1234.324Z:my_suite:expect_something.observed_value"
    res = ge_urn.parseString(urn)

    assert res["urn_type"] == "metrics"
    assert res["run_id"] == "20200403T1234.324Z"
    assert res["expectation_suite_name"] == "my_suite"
    assert res["metric_name"] == "expect_something.observed_value"
    assert "kwargs_dict" not in res


def test_ge_stores_urn():
    urn = "urn:great_expectations:stores:my_store:mymetric:kw=param"
    res = ge_urn.parseString(urn)

    assert res["urn_type"] == "stores"
    assert res["store_name"] == "my_store"
    assert res["metric_name"] == "mymetric"
    kwargs_dict = parse_qs(res["metric_kwargs"])
    assert kwargs_dict == {
        "kw": ["param"],
    }

    # No kwargs is ok
    urn = "urn:great_expectations:stores:my_store:mymetric"
    res = ge_urn.parseString(urn)

    assert res["urn_type"] == "stores"
    assert res["store_name"] == "my_store"
    assert res["metric_name"] == "mymetric"
    assert "metric_kwargs" not in res


def test_invalid_urn():
    # Must start with "urn:great_expectations"
    with pytest.raises(ParseException) as e:
        ge_urn.parseString("not_a_ge_urn")
    assert "not_a_ge_urn" in e.value.line

    # Must have one of the recognized types
    with pytest.raises(ParseException) as e:
        ge_urn.parseString("urn:great_expectations:foo:bar:baz:bin:barg")
    assert "urn:great_expectations:foo:bar:baz:bin:barg" in e.value.line

    # Cannot have too many parts
    with pytest.raises(ParseException) as e:
        ge_urn.parseString(
            "urn:great_expectations:validations:foo:bar:baz:bin:barg:boo"
        )
    assert "urn:great_expectations:validations:foo:bar:baz:bin:barg:boo" in e.value.line
