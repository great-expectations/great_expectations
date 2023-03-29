from __future__ import annotations

import pytest

from great_expectations.self_check.util import (
    generate_dataset_name_from_expectation_name,
)


@pytest.mark.parametrize(
    "dataset,expectation_name,index,sub_index,expected_output",
    [
        pytest.param(
            {"dataset_name": "i_am_a_dataset"},
            "expect_things",
            1,
            None,
            "i_am_a_dataset",
            id="defined_in_dataset_dict",
        ),
        pytest.param(
            {},
            "expect_things",
            1,
            None,
            "expect_things_dataset_1",
            id="expectation_name_and_index",
        ),
        pytest.param(
            {},
            "expect_things",
            1,
            2,
            "expect_things_dataset_1_2",
            id="expectation_name_and_sub_index",
        ),
    ],
)
def test_generate_table_name_with_expectation(
    dataset: dict,
    expectation_name: str,
    expected_output: str,
    index: int,
    sub_index: int | None,
):
    """Test for helper method that automatically generates table name for tests
    Args:
        dataset (dict): dictionary of test configuration.
        expectation_name (str): name of Expectation under test.
        expected_output (str): what should the generated table name be?
        index (int): index of current dataset
        sub_index (int): optional sub_index if there dataset is part of a list.
    """
    assert (
        generate_dataset_name_from_expectation_name(
            dataset=dataset,
            expectation_type=expectation_name,
            index=index,
            sub_index=sub_index,
        )
        == expected_output
    )
