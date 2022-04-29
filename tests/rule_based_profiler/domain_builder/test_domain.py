import pytest

from great_expectations.rule_based_profiler.types import (
    INFERRED_SEMANTIC_TYPE_KEY,
    Domain,
    SemanticDomainTypes,
)


def test_semantic_domain_serialization():
    domain: Domain

    domain = Domain(
        rule_name="my_rule",
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "low",
        },
    )

    assert domain.to_json_dict() == {
        "rule_name": "my_rule",
        "domain_type": "column",
        "domain_kwargs": {"column": "passenger_count"},
        "details": {
            "estimator": "categorical",
            "cardinality": "low",
        },
    }

    domain = Domain(
        rule_name="my_rule",
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "low",
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC,
            },
        },
    )

    assert domain.to_json_dict() == {
        "rule_name": "my_rule",
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
    }

    domain = Domain(
        rule_name="my_rule",
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "low",
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC.value,
            },
        },
    )

    assert domain.to_json_dict() == {
        "rule_name": "my_rule",
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
    }


def test_semantic_domain_comparisons():
    domain_a: Domain
    domain_b: Domain
    domain_c: Domain

    domain_a = Domain(
        rule_name="my_rule",
        domain_type="column",
        domain_kwargs={"column": "VendorID"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "VendorID": SemanticDomainTypes.NUMERIC.value,
            },
        },
    )
    domain_b = Domain(
        rule_name="my_rule",
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC.value,
            },
        },
    )
    domain_c = Domain(
        rule_name="my_rule",
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC.value,
            },
        },
    )

    assert not (domain_a == domain_b)
    assert domain_b == domain_c

    domain_a = Domain(
        rule_name="my_rule",
        domain_type="column",
        domain_kwargs={"column": "VendorID"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "VendorID": SemanticDomainTypes.NUMERIC.value,
            },
        },
    )
    domain_b = Domain(
        rule_name="my_rule",
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC.value,
            },
        },
    )
    domain_c = Domain(
        rule_name="my_rule",
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": SemanticDomainTypes.NUMERIC.value,
            },
        },
    )

    assert not (domain_a == domain_b)
    assert domain_b == domain_c

    domain_d: Domain = Domain(
        rule_name="my_rule",
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": "unknown_semantic_type_as_string",
            },
        },
    )

    with pytest.raises(ValueError) as excinfo:
        # noinspection PyUnusedLocal
        domain_as_dict: dict = domain_d.to_json_dict()

    assert (
        "'unknown_semantic_type_as_string' is not a valid SemanticDomainTypes"
        in str(excinfo.value)
    )

    domain_e: Domain = Domain(
        rule_name="my_rule",
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "low",
            INFERRED_SEMANTIC_TYPE_KEY: {
                "passenger_count": "unknown_semantic_type_as_string",
            },
        },
    )

    with pytest.raises(ValueError) as excinfo:
        # noinspection PyUnusedLocal
        domain_as_dict: dict = domain_e.to_json_dict()

    assert (
        "'unknown_semantic_type_as_string' is not a valid SemanticDomainTypes"
        in str(excinfo.value)
    )
