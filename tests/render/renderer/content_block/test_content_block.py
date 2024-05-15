from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import great_expectations.expectations as gxe
from great_expectations.render.renderer.content_block.content_block import (
    ContentBlockRenderer,
)

if TYPE_CHECKING:
    from great_expectations.expectations.expectation import Expectation


@pytest.mark.parametrize(
    "expectation, expected",
    [
        pytest.param(
            gxe.ExpectColumnValuesToBeBetween(
                column="foo", min_value=1, max_value=10, notes="these are _notes_"
            ),
            [
                {
                    "content_block_type": "markdown",
                    "markdown": "these are _notes_",
                    "styling": {"parent": {"styles": {"color": "red"}}},
                },
            ],
            id="string notes",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeBetween(
                column="foo",
                min_value=1,
                max_value=10,
                notes=["I", "have", "**notes**"],
            ),
            [
                {
                    "content_block_type": "markdown",
                    "markdown": "I",
                    "styling": {"parent": {}},
                },
                {
                    "content_block_type": "markdown",
                    "markdown": "have",
                    "styling": {"parent": {}},
                },
                {
                    "content_block_type": "markdown",
                    "markdown": "**notes**",
                    "styling": {"parent": {}},
                },
            ],
            id="list notes",
        ),
    ],
)
@pytest.mark.unit
def test__render_expectation_notes_with_notes(expectation: Expectation, expected: list[dict]):
    config = expectation.configuration
    content = ContentBlockRenderer._render_expectation_notes(config)
    notes_block = content.collapse[0]
    actual = notes_block.to_json_dict()["text"]
    assert actual == expected


@pytest.mark.unit
def test__render_expectation_notes_without_notes():
    expectation = gxe.ExpectColumnValuesToBeBetween(column="foo", min_value=1, max_value=10)
    config = expectation.configuration
    assert ContentBlockRenderer._render_expectation_notes(config) is None
