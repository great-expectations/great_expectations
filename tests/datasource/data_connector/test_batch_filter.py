from __future__ import annotations

from typing import List, Union

import pytest

from great_expectations.core import IDDict
from great_expectations.datasource.fluent.data_connector.batch_filter import (
    BatchFilter,
    build_batch_filter,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    "data_connector_query_dict, parsed_batch_slice, sliced_list",
    [
        pytest.param(
            {
                "index": "[4:9:2]",
            },
            slice(4, 9, 2),
            [4, 6, 8],
            id="batch_slice: str (with square brackets); (start, stop, step)",
        ),
        pytest.param(
            {
                "index": "4:9:2",
            },
            slice(4, 9, 2),
            [4, 6, 8],
            id="batch_slice: str (without square brackets); (start, stop, step)",
        ),
        pytest.param(
            {
                "index": "3:",
            },
            slice(3, None, None),
            [3, 4, 5, 6, 7, 8, 9],
            id="batch_slice: str (without square brackets, forward traversal at start); (start, stop=None, step=None)",  # noqa: E501 # FIXME CoP
        ),
        pytest.param(
            {
                "index": ":3",
            },
            slice(None, 3, None),
            [0, 1, 2],
            id="batch_slice: str (without square brackets); (start=None, stop, step=None)",
        ),
        pytest.param(
            {
                "index": "[1:4]",
            },
            slice(1, 4, None),
            [1, 2, 3],
            id="batch_slice: str (with square brackets); (start, stop, step=None)",
        ),
        pytest.param(
            {
                "index": "[-5:]",
            },
            slice(-5, None, None),
            [5, 6, 7, 8, 9],
            id="batch_slice: str (with square brackets); (start, stop=None, step=None)",
        ),
        pytest.param(
            {
                "index": (1, 7, 3),
            },
            slice(1, 7, 3),
            [1, 4],
            id="batch_slice: tuple; (start, stop, step)",
        ),
        pytest.param(
            {
                "index": 0,
            },
            slice(0, 1, None),
            [0],
            id="batch_slice: zero int; (start, stop=None, step=None)",
        ),
        pytest.param(
            {
                "index": -1,
            },
            slice(-1, None, None),
            [9],
            id="batch_slice: negative int; (start, stop=None, step=None)",
        ),
        pytest.param(
            {
                "index": "0",
            },
            slice(0, 1, None),
            [0],
            id="batch_slice: str (zero int); (start, stop=None, step=None)",
        ),
        pytest.param(
            {
                "index": "-1",
            },
            slice(-1, None, None),
            [9],
            id="batch_slice: str (negative int); (start, stop=None, step=None)",
        ),
        pytest.param(
            {
                "index": slice(-1, 0, -1),
            },
            slice(-1, 0, -1),
            [9, 8, 7, 6, 5, 4, 3, 2, 1],
            id="batch_slice: slice (reverse traversal); (negative_start, zero_stop, negative_step)",
        ),
        pytest.param(
            {
                "index": "::",
            },
            slice(None, None, None),
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            id="batch_slice: str (full forward traversal); (start=None, stop=None, step=1)",
        ),
        pytest.param(
            {
                "index": "::2",
            },
            slice(None, None, 2),
            [0, 2, 4, 6, 8],
            id="batch_slice: str (full forward traversal with step=2); (start=None, stop=None, step=2)",  # noqa: E501 # FIXME CoP
        ),
        pytest.param(
            {
                "index": "::-1",
            },
            slice(None, None, -1),
            [9, 8, 7, 6, 5, 4, 3, 2, 1, 0],
            id="batch_slice: str (full reverse traversal); (start=None, stop=None, step=-1)",
        ),
    ],
)
def test_batch_filter_parse_batch_slice(
    data_connector_query_dict: dict,
    parsed_batch_slice: slice,
    sliced_list: List[int],
):
    original_list: List[int] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    batch_filter_obj: BatchFilter = build_batch_filter(
        data_connector_query_dict=data_connector_query_dict
    )
    assert batch_filter_obj.index == parsed_batch_slice
    assert original_list[parsed_batch_slice] == sliced_list


@pytest.mark.unit
@pytest.mark.parametrize(
    "requested_value,captured_value,should_match",
    [
        pytest.param(4, "04", True, id="int filter value matches a zero-padded string capture"),
        pytest.param(4, "4", True, id="int filter value matches an unpadded string capture"),
        pytest.param(5, "04", False, id="a non-matching int does not match"),
        pytest.param(
            "04",
            "4",
            False,
            id="string filter value stays an exact, non-numeric comparison",
        ),
    ],
)
def test_batch_filter_matcher_numeric_equivalence(
    requested_value: Union[int, str], captured_value: str, should_match: bool
):
    # A batch identifier is always a string; an int filter value denotes the same
    # partition as its zero-padded capture, while a string filter value never does.
    batch_filter_obj = BatchFilter(batch_filter_parameters=IDDict({"month": requested_value}))
    matcher = batch_filter_obj.best_effort_batch_definition_matcher()

    assert matcher(batch_identifiers={"month": captured_value}) is should_match


@pytest.mark.unit
def test_batch_filter_matcher_key_absent_from_identifiers_fails():
    batch_filter_obj = BatchFilter(batch_filter_parameters=IDDict({"month": 4}))
    matcher = batch_filter_obj.best_effort_batch_definition_matcher()

    assert not matcher(batch_identifiers={"year": "2020"})
