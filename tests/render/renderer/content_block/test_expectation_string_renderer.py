"""
The real meat of these tests lives in the test_definitions directory for individual expectations
"""

import pytest

from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.render.renderer.content_block import ExpectationStringRenderer


@pytest.mark.unit
def test_expectation_string_renderer_styling():
    renderer = ExpectationStringRenderer()
    result = renderer.render(
        ExpectationConfiguration(
            type="expect_column_values_to_be_unique",
            kwargs={"column": "Name"},
        )
    )
    assert len(result) == 1
    assert result[0].string_template["template"] == "$column values must be unique."

    result = renderer.render(
        ExpectationConfiguration(
            type="expect_column_values_to_be_unique",
            kwargs={"column": "Name", "mostly": 0.3},
        )
    )
    assert len(result) == 1
    template = result[0].string_template
    assert (
        template["template"] == "$column values must be unique, at least $mostly_pct % of the time."
    )
    assert template["params"]["mostly_pct"] == "30"

    result = renderer.render(
        ExpectationConfiguration(
            type="expect_column_values_to_be_unique",
            kwargs={"column": "Name", "mostly": 0.32345},
        )
    )
    assert len(result) == 1
    template = result[0].string_template
    assert (
        template["template"] == "$column values must be unique, at least $mostly_pct % of the time."
    )
    assert template["params"]["mostly_pct"] == "32.345"
