import pytest

from great_expectations.rule_based_profiler.types import Domain, SemanticDomainTypes


def test_semantic_domain_serialization():
    domain: Domain

    domain = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "low",
        },
    )

    assert domain.to_json_dict() == {
        "domain_type": "column",
        "domain_kwargs": {"column": "passenger_count"},
        "details": {
            "estimator": "categorical",
            "cardinality": "low",
        },
    }

    domain = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "low",
            "inferred_semantic_domain_type": SemanticDomainTypes.CURRENCY,
        },
    )

    assert domain.to_json_dict() == {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "passenger_count",
        },
        "details": {
            "estimator": "categorical",
            "cardinality": "low",
            "inferred_semantic_domain_type": SemanticDomainTypes.CURRENCY.value,
        },
    }

    domain = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "low",
            "inferred_semantic_domain_type": "currency",
        },
    )

    assert domain.to_json_dict() == {
        "domain_type": "column",
        "domain_kwargs": {
            "column": "passenger_count",
        },
        "details": {
            "estimator": "categorical",
            "cardinality": "low",
            "inferred_semantic_domain_type": SemanticDomainTypes.CURRENCY.value,
        },
    }


def test_semantic_domain_comparisons():
    domain_a: Domain
    domain_b: Domain
    domain_c: Domain

    domain_a = Domain(
        domain_type="column",
        domain_kwargs={"column": "VendorID"},
        details={"inferred_semantic_domain_type": "numeric"},
    )
    domain_b = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={"inferred_semantic_domain_type": "numeric"},
    )
    domain_c = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={"inferred_semantic_domain_type": "numeric"},
    )

    assert not (domain_a == domain_b)
    assert domain_b == domain_c

    domain_a = Domain(
        domain_type="column",
        domain_kwargs={"column": "VendorID"},
        details={"inferred_semantic_domain_type": SemanticDomainTypes.NUMERIC},
    )
    domain_b = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={"inferred_semantic_domain_type": SemanticDomainTypes.NUMERIC},
    )
    domain_c = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={"inferred_semantic_domain_type": SemanticDomainTypes.NUMERIC},
    )

    assert not (domain_a == domain_b)
    assert domain_b == domain_c

    domain_d: Domain = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={"inferred_semantic_domain_type": "unknown_semantic_type_as_string"},
    )

    with pytest.raises(ValueError) as excinfo:
        # noinspection PyUnusedLocal
        domain_as_dict: dict = domain_d.to_json_dict()

    assert (
        "'unknown_semantic_type_as_string' is not a valid SemanticDomainTypes"
        in str(excinfo.value)
    )

    domain_e: Domain = Domain(
        domain_type="column",
        domain_kwargs={"column": "passenger_count"},
        details={
            "estimator": "categorical",
            "cardinality": "low",
            "inferred_semantic_domain_type": "unknown_semantic_type_as_string",
        },
    )

    with pytest.raises(ValueError) as excinfo:
        # noinspection PyUnusedLocal
        domain_as_dict: dict = domain_e.to_json_dict()

    assert (
        "'unknown_semantic_type_as_string' is not a valid SemanticDomainTypes"
        in str(excinfo.value)
    )
