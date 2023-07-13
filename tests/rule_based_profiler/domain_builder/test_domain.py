from typing import Optional

import pytest

from great_expectations.core.domain import (
    INFERRED_SEMANTIC_TYPE_KEY,
    Domain,
    SemanticDomainTypes,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.helpers.util import (
    integer_semantic_domain_type,
)

# module level markers
pytestmark = [pytest.mark.unit]


def test_semantic_domain_consistency():
    domain: Domain

    with pytest.raises(ValueError) as excinfo:
        # noinspection PyUnusedLocal
        domain = Domain(
            domain_type="column",
            domain_kwargs={"column": "passenger_count"},
            details={
                "estimator": "categorical",
                "cardinality": "low",
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "num_passengers": SemanticDomainTypes.NUMERIC,
                },
            },
            rule_name="my_rule",
        )

    assert (
        """Cannot instantiate Domain (domain_type "MetricDomainTypes.COLUMN" of type "<enum 'MetricDomainTypes'>" -- key "num_passengers", detected in "inferred_semantic_domain_type" dictionary, does not exist as value of appropriate key in "domain_kwargs" dictionary."""
        in str(excinfo.value)
    )


def test_semantic_domain_serialization():
    domain: Domain

    domain = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "low",
        },
        rule_name="my_rule",
    )

    assert domain.to_json_dict() == {
        "domain_type": "column",
        "domain_kwargs": {"column": "passenger_count"},
        "details": {
            "estimator": "categorical",
            "cardinality": "low",
        },
        "rule_name": "my_rule",
    }

    domain = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "low",
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="my_rule",
    )

    assert domain.to_json_dict() == {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "passenger_count",
        },
        "details": {
            "estimator": "categorical",
            "cardinality": "low",
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC.value,
            },
        },
        "rule_name": "my_rule",
    }

    domain = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "low",
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="my_rule",
    )

    assert domain.to_json_dict() == {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "passenger_count",
        },
        "details": {
            "estimator": "categorical",
            "cardinality": "low",
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC.value,
            },
        },
        "rule_name": "my_rule",
    }


def test_semantic_domain_equivalence():
    domain_a: Domain
    domain_b: Domain
    domain_c: Domain

    domain_a = Domain(
        domain_type="column",
        domain_kwargs={"column": "VendorID"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "VendorID": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="my_rule",
    )
    domain_b = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="my_rule",
    )
    domain_c = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="my_rule",
    )

    assert not (domain_a == domain_b)
    assert domain_b == domain_c

    domain_a = Domain(
        domain_type="column",
        domain_kwargs={"column": "VendorID"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "VendorID": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="my_rule",
    )
    domain_b = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="my_rule",
    )
    domain_c = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="my_rule",
    )

    assert not (domain_a == domain_b)
    assert domain_b == domain_c

    domain_d = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": "unknown_semantic_type_as_string",
            },
        },
        rule_name="my_rule",
    )

    with pytest.raises(ValueError) as excinfo:
        # noinspection PyUnusedLocal
        domain_as_dict: dict = domain_d.to_json_dict()

    assert (
        "'unknown_semantic_type_as_string' is not a valid SemanticDomainTypes"
        in str(excinfo.value)
    )

    domain_e = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "low",
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": "unknown_semantic_type_as_string",
            },
        },
        rule_name="my_rule",
    )

    with pytest.raises(ValueError) as excinfo:
        # noinspection PyUnusedLocal
        domain_as_dict: dict = domain_e.to_json_dict()

    assert (
        "'unknown_semantic_type_as_string' is not a valid SemanticDomainTypes"
        in str(excinfo.value)
    )


def test_semantic_domain_comparisons_inclusion():
    domain_a: Optional[Domain]
    domain_b: Optional[Domain]

    domain_a = Domain(
        domain_type=MetricDomainTypes.TABLE,
        domain_kwargs={
            "table": "animal_names",
        },
        rule_name="my_rule",
    )

    domain_b = Domain(
        domain_type="table",
    )
    assert domain_a.is_superset(other=domain_b)

    domain_a = Domain(
        domain_type="column_pair",
        domain_kwargs={"column_A": "w", "column_B": "x"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "w": SemanticDomainTypes.NUMERIC,
                "x": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="my_rule",
    )

    domain_b = Domain(
        domain_type="column_pair",
        domain_kwargs={
            "column_A": "w",
            "column_B": "x",
        },
    )
    assert domain_a.is_superset(other=domain_b)

    domain_b = Domain(
        domain_type="column_pair",
        domain_kwargs={
            "column_B": "x",
            "column_A": "w",
        },
    )
    assert domain_a.is_superset(other=domain_b)

    domain_a = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "low",
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="my_rule",
    )
    domain_b = None
    assert domain_a.is_superset(other=domain_b)

    domain_b = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "low",
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="my_rule",
    )
    assert domain_a.is_superset(other=domain_b)

    domain_b = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "low",
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="",
    )
    assert domain_a.is_superset(other=domain_b)

    domain_b = Domain(
        domain_type="column",
        domain_kwargs=None,
        details={
            "estimator": "categorical",
            "cardinality": "low",
            INFERRED_SEMANTIC_TYPE_KEY: {},
        },
        rule_name=None,
    )
    assert domain_a.is_superset(other=domain_b)

    domain_b = Domain(
        domain_type="column",
        domain_kwargs=None,
        details={
            "estimator": "categorical",
            "cardinality": "low",
        },
    )
    assert domain_a.is_superset(other=domain_b)

    domain_b = Domain(
        domain_type="column",
        domain_kwargs=None,
        details={},
        rule_name=None,
    )
    assert domain_a.is_superset(other=domain_b)

    domain_b = Domain(
        domain_type="column",
        domain_kwargs=None,
        details=None,
        rule_name="",
    )
    assert domain_a.is_superset(other=domain_b)

    domain_b = Domain(
        domain_type="column",
        rule_name="",
    )
    assert domain_a.is_superset(other=domain_b)

    domain_b = Domain(
        domain_type="column",
        rule_name="my_other_rule",
    )
    assert not domain_a.is_superset(other=domain_b)

    domain_b = Domain(
        domain_type="table",
        rule_name="my_other_rule",
    )
    assert not domain_a.is_superset(other=domain_b)

    domain_b = Domain(
        domain_type="table",
        rule_name="",
    )
    assert not domain_a.is_superset(other=domain_b)

    domain_b = Domain(
        domain_type="table",
        rule_name="my_rule",
    )
    assert not domain_a.is_superset(other=domain_b)

    domain_b = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "medium",
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="my_rule",
    )
    assert not domain_a.is_superset(other=domain_b)

    domain_b = Domain(
        domain_type="column",
        domain_kwargs={"column": "fair_amount"},
        rule_name="my_rule",
    )
    assert not domain_a.is_superset(other=domain_b)

    domain_b = Domain(
        domain_type="column",
        domain_kwargs={"column": "fair_amount"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "fair_amount": SemanticDomainTypes.CURRENCY,
            },
        },
        rule_name="my_rule",
    )
    assert not domain_a.is_superset(other=domain_b)


def test_integer_semantic_domain_type():
    domain: Domain

    domain = Domain(
        domain_type="column",
        domain_kwargs={
            "column": "passenger_count",
        },
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC,
            },
        },
        rule_name="my_rule",
    )
    assert not integer_semantic_domain_type(domain=domain)

    domain = Domain(
        domain_type="column",
        domain_kwargs={
            "column": "VendorID",
        },
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "VendorID": SemanticDomainTypes.IDENTIFIER,
            },
        },
        rule_name="my_rule",
    )
    assert integer_semantic_domain_type(domain=domain)

    domain = Domain(
        domain_type="column",
        domain_kwargs={
            "column": "is_night_time",
        },
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "is_night_time": SemanticDomainTypes.LOGIC,
            },
        },
        rule_name="my_rule",
    )
    assert integer_semantic_domain_type(domain=domain)

    domain = Domain(
        domain_type="column",
        domain_kwargs={
            "column_A": "passenger_count",
            "column_B": "fare_amount",
        },
        rule_name="my_rule",
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC,
                "fare_amount": SemanticDomainTypes.NUMERIC,
            },
        },
    )
    assert not integer_semantic_domain_type(domain=domain)

    domain = Domain(
        domain_type="column",
        domain_kwargs={
            "column_A": "passenger_count",
            "column_B": "VendorID",
        },
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC,
                "VendorID": SemanticDomainTypes.IDENTIFIER,
            },
        },
        rule_name="my_rule",
    )
    assert not integer_semantic_domain_type(domain=domain)

    domain = Domain(
        domain_type="column",
        domain_kwargs={
            "column_A": "is_night_time",
            "column_B": "VendorID",
        },
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "is_night_time": SemanticDomainTypes.LOGIC,
                "VendorID": SemanticDomainTypes.IDENTIFIER,
            },
        },
        rule_name="my_rule",
    )
    assert integer_semantic_domain_type(domain=domain)

    domain = Domain(
        domain_type="column",
        domain_kwargs={
            "column_list": [
                "passenger_count",
                "fare_amount",
                "is_night_time",
            ],
        },
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC,
                "fare_amount": SemanticDomainTypes.NUMERIC,
                "is_night_time": SemanticDomainTypes.LOGIC,
            },
        },
        rule_name="my_rule",
    )
    assert not integer_semantic_domain_type(domain=domain)

    domain = Domain(
        domain_type="column",
        domain_kwargs={
            "column_list": [
                "RatecodeID",
                "VendorID",
                "fare_amount",
                "is_night_time",
            ],
        },
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "VendorID": SemanticDomainTypes.IDENTIFIER,
                "RatecodeID": SemanticDomainTypes.IDENTIFIER,
                "is_night_time": SemanticDomainTypes.LOGIC,
            },
        },
        rule_name="my_rule",
    )
    assert integer_semantic_domain_type(domain=domain)
