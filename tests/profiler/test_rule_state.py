import pytest

from great_expectations.exceptions import ProfilerExecutionError
from great_expectations.profiler.domain_builder.domain import (
    Domain,
    SemanticDomainTypes,
    StorageDomainTypes,
)
from great_expectations.profiler.parameter_builder.parameter_container import (
    ParameterContainer,
)
from great_expectations.profiler.rule.rule_state import RuleState


@pytest.fixture
def simple_rule_state():
    """Simple rule_state with one domain, currently set to active"""
    return RuleState(
        active_domain=Domain(
            domain_kwargs={"column": "Age"}, domain_type=StorageDomainTypes.COLUMN
        ),
        domains=[
            Domain(
                domain_kwargs={"column": "Age"}, domain_type=StorageDomainTypes.COLUMN
            )
        ],
        parameters={},
    )


@pytest.fixture
def semantic_rule_state():
    """Simple rule_state with one domain, currently set to active"""
    return RuleState(
        active_domain=Domain(
            domain_kwargs={"column": "Age"}, domain_type=SemanticDomainTypes.NUMERIC
        ),
        domains=[
            Domain(
                domain_kwargs={"column": "Age"}, domain_type=SemanticDomainTypes.NUMERIC
            )
        ],
        parameters={
            "221524dcea2a0c06128e96256a3cad1d": ParameterContainer(
                parameters={"mean": 5.0}, details=None, descendants=None
            )
        },
        variables=ParameterContainer(
            parameters={"false_positive_threshold": 0.01},
            details=None,
            descendants=None,
        ),
    )


def test_id_property_of_active_domain(simple_rule_state, semantic_rule_state):
    assert simple_rule_state.active_domain.id == "20afbf5aa7826d5d5c378a62ccef8ded"
    assert semantic_rule_state.active_domain.id == "221524dcea2a0c06128e96256a3cad1d"


def test_get_parameter_value(semantic_rule_state):
    assert (
        semantic_rule_state.get_parameter_value(fully_qualified_parameter_name="$mean")
        == 5.0
    )
    assert (
        semantic_rule_state.get_parameter_value(
            fully_qualified_parameter_name="$variables.false_positive_threshold"
        )
        == 0.01
    )


def test_invalid_parameter_name(semantic_rule_state):
    with pytest.raises(ProfilerExecutionError) as exc:
        _ = semantic_rule_state.get_parameter_value(
            fully_qualified_parameter_name="mean"
        )
    assert "start with $" in exc.value.message
