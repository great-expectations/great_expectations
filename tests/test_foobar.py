import pytest

from great_expectations.foobar import my_function


@pytest.mark.unit
def test_my_function():
    assert my_function("foo") == "bar"
