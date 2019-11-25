import pytest

from great_expectations.core import ExpectationKwargs


@pytest.fixture
def kwargs1():
    return ExpectationKwargs(
        {
            "column": "a",
            "value_set": [1, 2, 3],
            "result_format": "BASIC"
        }
    )


@pytest.fixture
def kwargs2():
    return ExpectationKwargs(
        column="a",
        value_set=[1, 2, 3],
        result_format="BASIC"
    )


@pytest.fixture
def kwargs3():
    return ExpectationKwargs(
        column="a",
        value_set=[1, 2, 3],
        result_format="COMPLETE"
    )


def test_ignored_keys(kwargs1):
    # Codify the list of ignored keys
    assert ExpectationKwargs.ignored_keys == ['result_format', 'include_config', 'catch_exceptions']


def test_expectation_kwargs_equality(kwargs1, kwargs2, kwargs3):
    # Note that we initialized kwargs1 and kwargs2 using different means (one as a dictionary the other as kwargs)
    assert kwargs1 == kwargs2
    assert not (kwargs1 != kwargs2)
    assert kwargs1 != kwargs3


def test_expectation_kwargs_equivalence(kwargs1, kwargs2, kwargs3):
    assert kwargs1.isEquivalentTo(kwargs2)
    assert kwargs2.isEquivalentTo(kwargs1)
    assert kwargs1.isEquivalentTo(kwargs3)
