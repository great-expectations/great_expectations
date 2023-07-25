from __future__ import annotations

from typing import List

import pytest

from great_expectations.datasource.data_connector.batch_filter import (
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
            id="batch_slice: str (without square brackets, forward traversal at start); (start, stop=None, step=None)",
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
            id="batch_slice: str (full forward traversal with step=2); (start=None, stop=None, step=2)",
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
        data_connector_query_dict=data_connector_query_dict  # type: ignore[arg-type]
    )
    assert batch_filter_obj.index == parsed_batch_slice
    assert original_list[parsed_batch_slice] == sliced_list
