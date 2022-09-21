"""
Why does this exist?

This is a way to take H
"""
from great_expectations import DataContext
from great_expectations.util import get_context


# how do you mock the root directory?
# i think this
def test_get_DataContext():
    # when you initialize with emptyj
    assert isinstance(get_context(), DataContext)
