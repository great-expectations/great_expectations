from typing import List

import pytest

# module level markers
pytestmark = pytest.mark.filesystem


def test_list_datasources_base_data_context_no_datasources(empty_data_context) -> None:
    """What does this test and why?

    When listing datasources, we want to omit the name and id fields. This test uses DataContext.
    """

    context = empty_data_context

    # no datasources

    observed: List[dict] = context.list_datasources()

    expected: List[dict] = []

    assert observed == expected


def test_list_datasources_base_data_context_one_datasource(empty_data_context) -> None:
    """What does this test and why?

    When listing datasources, we want to omit the name and id fields. This test uses DataContext.
    """

    context = empty_data_context

    # one datasource

    datasource = context.data_sources.add_pandas("my_data_source")

    observed = context.list_datasources()

    assert observed == [datasource]


def test_list_datasources_base_data_context_two_datasources(empty_data_context) -> None:
    """What does this test and why?

    When listing datasources, we want to omit the name and id fields. This test uses DataContext.
    """

    context = empty_data_context

    data_source_a = context.data_sources.add_pandas("my_data_source")
    data_source_b = context.data_sources.add_pandas("your_data_source")

    observed = context.list_datasources()

    assert observed == [data_source_a, data_source_b]
