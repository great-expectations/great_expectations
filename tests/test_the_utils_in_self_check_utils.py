from __future__ import annotations

from contextlib import nullcontext as does_not_raise

import pytest

from great_expectations.exceptions import ExecutionEngineError
from great_expectations.self_check.util import (
    _check_if_valid_dataset_name,
    generate_dataset_name_from_expectation_name,
)

# module level markers
pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "dataset,expectation_name,index,sub_index,expected_output,expectation",
    [
        pytest.param(
            {"dataset_name": "i_am_a_dataset"},
            "expect_things",
            1,
            None,
            "i_am_a_dataset",
            does_not_raise(),
            id="defined_in_dataset_dict",
        ),
        pytest.param(
            {},
            "expect_things",
            1,
            None,
            "expect_things_dataset_1",
            does_not_raise(),
            id="expectation_name_and_index",
        ),
        pytest.param(
            {},
            "expect_things",
            1,
            2,
            "expect_things_dataset_1_2",
            does_not_raise(),
            id="expectation_name_and_sub_index",
        ),
        pytest.param(
            {},
            "expect_many_many_many_many_many_many_many_many_many_many_things",
            1,
            None,
            "y_many_many_many_many_many_many_many_many_many_things_dataset_1",
            does_not_raise(),
            id="expection_name_truncated_and_underscore_removed",
        ),
        pytest.param(
            {},
            "i*am*not*valid",
            1,
            None,
            "",
            pytest.raises(ExecutionEngineError),
            id="expection_name_truncated_and_underscore_removed",
        ),
    ],
)
def test_generate_table_name_with_expectation(
    dataset: dict,
    expectation_name: str,
    expected_output: str,
    index: int,
    sub_index: int | None,
    expectation,
):
    """Test for helper method that automatically generates table name for tests
    Args:
        dataset (dict): dictionary of test configuration.
        expectation_name (str): name of Expectation under test.
        expected_output (str): what should the generated table name be?
        index (int): index of current dataset
        sub_index (int): optional sub_index if there dataset is part of a list.
    """
    with expectation:
        assert (
            generate_dataset_name_from_expectation_name(
                dataset=dataset,
                expectation_type=expectation_name,
                index=index,
                sub_index=sub_index,
            )
            == expected_output
        )


@pytest.mark.parametrize(
    "dataset_name,expected_output,expectation",
    [
        pytest.param(
            "i_am_a_dataset",
            "i_am_a_dataset",
            does_not_raise(),
            id="defined_in_dataset_dict",
        ),
        pytest.param(
            "expect_many_many_many_many_many_many_many_many_many_many_many_many_many_many_things",
            "y_many_many_many_many_many_many_many_many_many_many_many_things",
            does_not_raise(),
            id="expection_name_truncated_and_underscore_removed",
        ),
        pytest.param(
            "i*am*not*valid",
            "",
            pytest.raises(ExecutionEngineError),
            id="expection_name_truncated_and_underscore_removed",
        ),
        pytest.param(
            "_________i_have_too_many_underscores",
            "i_have_too_many_underscores",
            does_not_raise(),
            id="beginning_underscores_removed",
        ),
    ],
)
def test_check_if_valid_dataset_name(dataset_name: str, expected_output: str, expectation):
    """Test for helper method that ensures table names are valid for tests
    Args:
        dataset_name(str): candidate dataset_name.
        expected_output (str): what should the final table name be?
    """
    with expectation:
        assert (
            _check_if_valid_dataset_name(
                dataset_name=dataset_name,
            )
            == expected_output
        )
