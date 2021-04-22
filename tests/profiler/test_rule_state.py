import pytest

from great_expectations.exceptions import ProfilerExecutionError
from great_expectations.profiler.parameter_builder.parameter_tree_container_node import ParameterTreeContainerNode
from great_expectations.profiler.profiler_rule.rule_state import RuleState


@pytest.fixture
def simple_rule_state():
    """Simple rule_state with one domain, currently set to active"""
    return RuleState(
        active_domain={"domain_kwargs": {"column": "Age"}},
        domains=[{"domain_kwargs": {"column": "Age"}}],
        parameters=dict(),
    )


@pytest.fixture
def semantic_rule_state():
    """Simple rule_state with one domain, currently set to active"""
    return RuleState(
        active_domain={"domain_kwargs": {"column": "Age"}, "semantic_type": "numeric"},
        domains=[{"domain_kwargs": {"column": "Age"}, "semantic_type": "numeric"}],
        parameters={
            "f45a40fda1738351c5e67a0aa89c2c7c": ParameterTreeContainerNode(
                parameters={"mean": 5.0}, details=None, descendants=None
            )
        },
        variables=ParameterTreeContainerNode(
            parameters={"false_positive_threshold": 0.01}, details=None, descendants=None
        ),
    )


def test_active_domain_id_property(simple_rule_state, semantic_rule_state):
    assert simple_rule_state.active_domain_id == "domain_kwargs={'column': 'Age'}"
    assert semantic_rule_state.active_domain_id == "f45a40fda1738351c5e67a0aa89c2c7c"


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
