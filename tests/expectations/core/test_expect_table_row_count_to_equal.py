from typing import Optional

import pandas as pd
import pytest

import great_expectations as gx
from great_expectations.self_check.util import get_test_validator_with_data


@pytest.mark.unit
@pytest.mark.parametrize(
    "value,row_condition,success,expected_count",
    [
        # Without row_condition - should count all 10 rows
        (10, None, True, 10),
        (5, None, False, 10),
        # With row_condition filtering age >= 18 - should count 6 rows
        (6, "age >= 18", True, 6),
        (5, "age >= 18", False, 6),
        # With row_condition filtering age < 18 - should count 4 rows
        (4, "age < 18", True, 4),
        (5, "age < 18", False, 4),
        # With row_condition filtering name == "José" - should count 1 row
        (1, 'name == "José"', True, 1),
        (2, 'name == "José"', False, 1),
    ],
)
def test_expect_table_row_count_to_equal_with_row_condition(
    value: int,
    row_condition: Optional[str],
    success: bool,
    expected_count: int,
):
    """Test that row_condition properly filters rows before counting."""
    # Create test dataframe with known counts
    df = pd.DataFrame(
        {
            "name": [
                "José",
                "Bob",
                "Charlie",
                "David",
                "Eve",
                "Frank",
                "Grace",
                "Hannah",
                "Ian",
                "Jane",
            ],
            "age": [25, 30, 15, 35, 22, 16, 28, 17, 40, 12],
        }
    )

    context = gx.get_context(mode="ephemeral")
    validator = get_test_validator_with_data(
        execution_engine="pandas",
        data=df,  # type: ignore[arg-type] # pandas DataFrame is valid data type
        context=context,
    )

    result = validator.expect_table_row_count_to_equal(
        value=value,
        row_condition=row_condition,
        condition_parser="pandas",
    )

    assert result.success is success
    assert result.result["observed_value"] == expected_count
