import pytest

from great_expectations.profiler.exceptions import ProfilerExecutionError
from great_expectations.profiler.rule_state import RuleState


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
        parameters={"f45a40fda1738351c5e67a0aa89c2c7c": {"mean": 5.0}},
        variables={"false_positive_threshold": 0.01},
    )


def test_get_active_domain_id(simple_rule_state, semantic_rule_state):
    assert simple_rule_state.get_active_domain_id() == "domain_kwargs={'column': 'Age'}"
    assert (
        semantic_rule_state.get_active_domain_id() == "f45a40fda1738351c5e67a0aa89c2c7c"
    )


def test_get_parameter_value(semantic_rule_state):
    assert semantic_rule_state.get_value("$mean") == 5.0


def test_get_variable_value(semantic_rule_state):
    assert semantic_rule_state.get_value("$variables.false_positive_threshold") == 0.01


def test_invalid_parameter_name(semantic_rule_state):
    with pytest.raises(ProfilerExecutionError) as exc:
        _ = semantic_rule_state.get_value("mean")
    assert "start with $" in exc.value.message
