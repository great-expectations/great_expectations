"""Unit coverage for the `escape` parameter shared by the four LIKE pattern Expectations.

The integration suites exercise the accepted path against real backends. What they cannot
reach is the rejection path, which never reaches SQL, the published JSON schema, which
is what a consumer validating a config without instantiating it actually sees, and the
rendered description, which is what a reader of Data Docs actually sees.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import great_expectations.expectations as gxe
from great_expectations.compatibility import pydantic
from great_expectations.expectations.core import schemas
from great_expectations.expectations.registry import get_renderer_impl
from great_expectations.render import AtomicPrescriptiveRendererType, LegacyRendererType

# Each Expectation with the kwargs it needs besides `escape`.
LIKE_PATTERN_EXPECTATIONS: list[tuple[type, dict[str, Any]]] = [
    (gxe.ExpectColumnValuesToMatchLikePattern, {"column": "c", "like_pattern": "a!_b"}),
    (gxe.ExpectColumnValuesToNotMatchLikePattern, {"column": "c", "like_pattern": "a!_b"}),
    (gxe.ExpectColumnValuesToMatchLikePatternList, {"column": "c", "like_pattern_list": ["a!_b"]}),
    (
        gxe.ExpectColumnValuesToNotMatchLikePatternList,
        {"column": "c", "like_pattern_list": ["a!_b"]},
    ),
]

EXPECTATION_IDS = [cls.__name__ for cls, _ in LIKE_PATTERN_EXPECTATIONS]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("expectation_class", "kwargs"), LIKE_PATTERN_EXPECTATIONS, ids=EXPECTATION_IDS
)
@pytest.mark.parametrize("escape", ["!!", "", "abc"], ids=["two_chars", "empty", "three_chars"])
def test_escape_must_be_exactly_one_character(
    expectation_class: type, kwargs: dict[str, Any], escape: str
) -> None:
    """SQL permits a single escape character, so anything else must be refused here.

    The empty string is the case worth pinning. `escape` is typed as a union that includes
    `SuiteParameterDict`, and `dict("")` succeeds, so a rule enforced on the string branch
    of that union rather than by a validator would let an empty escape through as an empty
    suite parameter instead of rejecting it.
    """
    with pytest.raises(pydantic.ValidationError, match="escape must be a single character"):
        expectation_class(**kwargs, escape=escape)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("expectation_class", "kwargs"), LIKE_PATTERN_EXPECTATIONS, ids=EXPECTATION_IDS
)
def test_a_single_character_escape_is_accepted(
    expectation_class: type, kwargs: dict[str, Any]
) -> None:
    assert expectation_class(**kwargs, escape="!").escape == "!"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("expectation_class", "kwargs"), LIKE_PATTERN_EXPECTATIONS, ids=EXPECTATION_IDS
)
def test_escape_is_optional(expectation_class: type, kwargs: dict[str, Any]) -> None:
    assert expectation_class(**kwargs).escape is None


@pytest.mark.unit
@pytest.mark.parametrize(
    ("expectation_class", "kwargs"), LIKE_PATTERN_EXPECTATIONS, ids=EXPECTATION_IDS
)
def test_escape_accepts_a_suite_parameter(expectation_class: type, kwargs: dict[str, Any]) -> None:
    """The length rule applies to literal strings only; a suite parameter resolves later."""
    suite_parameter = {"$PARAMETER": "my_escape"}

    assert expectation_class(**kwargs, escape=suite_parameter).escape == suite_parameter


@pytest.mark.unit
@pytest.mark.parametrize(
    "expectation_class", [cls for cls, _ in LIKE_PATTERN_EXPECTATIONS], ids=EXPECTATION_IDS
)
def test_published_schema_constrains_escape_to_one_character(expectation_class: type) -> None:
    """The schema must say what the validator enforces.

    A consumer that validates a config against the published schema without instantiating
    the Expectation would otherwise accept an escape the library then rejects.
    """
    schema_path = Path(schemas.__file__).parent / f"{expectation_class.__name__}.json"
    escape_property = json.loads(schema_path.read_text())["properties"]["escape"]

    string_branches = [
        branch for branch in escape_property["anyOf"] if branch.get("type") == "string"
    ]

    assert string_branches == [{"type": "string", "minLength": 1, "maxLength": 1}]


# The summary each Expectation renders, keyed by class, without and with `escape="!"`.
# Spelled out rather than built from the renderer's own fragments, so that a renderer
# dropping the escape, or changing the unescaped wording, fails here.
ATOMIC_SUMMARY_TEMPLATES: dict[type, tuple[str, str]] = {
    gxe.ExpectColumnValuesToMatchLikePattern: (
        "$column values must match like pattern $like_pattern.",
        "$column values must match like pattern $like_pattern, escaping wildcards with $escape.",
    ),
    gxe.ExpectColumnValuesToNotMatchLikePattern: (
        "$column values must not match like pattern $like_pattern.",
        "$column values must not match like pattern $like_pattern, "
        "escaping wildcards with $escape.",
    ),
    gxe.ExpectColumnValuesToMatchLikePatternList: (
        "$column values must match the following like patterns: $like_pattern_list_0",
        "$column values must match the following like patterns: "
        "$like_pattern_list_0, escaping wildcards with $escape",
    ),
    gxe.ExpectColumnValuesToNotMatchLikePatternList: (
        "$column values must not match the following like patterns: $like_pattern_list_0",
        "$column values must not match the following like patterns: "
        "$like_pattern_list_0, escaping wildcards with $escape",
    ),
}


def _render(expectation: Any, renderer_type: str) -> dict[str, Any]:
    renderer_impl = get_renderer_impl(
        object_name=expectation.expectation_type, renderer_type=renderer_type
    )
    assert renderer_impl is not None
    rendered = renderer_impl[1](configuration=expectation.configuration)
    if isinstance(rendered, list):  # the legacy renderer returns a list of content blocks
        (rendered,) = rendered
        template = rendered.to_json_dict()["string_template"]
    else:
        template = rendered.to_json_dict()["value"]
    assert isinstance(template, dict)
    return template


@pytest.mark.unit
@pytest.mark.parametrize(
    ("expectation_class", "kwargs"), LIKE_PATTERN_EXPECTATIONS, ids=EXPECTATION_IDS
)
def test_atomic_summary_discloses_the_escape(
    expectation_class: type, kwargs: dict[str, Any]
) -> None:
    """Without the escape an escaped pattern reads as wildcards, describing another check."""
    rendered = _render(
        expectation_class(**kwargs, escape="!"), AtomicPrescriptiveRendererType.SUMMARY
    )

    assert rendered["template"] == ATOMIC_SUMMARY_TEMPLATES[expectation_class][1]
    assert rendered["params"]["escape"]["value"] == "!"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("expectation_class", "kwargs"), LIKE_PATTERN_EXPECTATIONS, ids=EXPECTATION_IDS
)
def test_atomic_summary_is_unchanged_without_an_escape(
    expectation_class: type, kwargs: dict[str, Any]
) -> None:
    rendered = _render(expectation_class(**kwargs), AtomicPrescriptiveRendererType.SUMMARY)

    assert rendered["template"] == ATOMIC_SUMMARY_TEMPLATES[expectation_class][0]
    assert "escape" not in rendered["params"]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("expectation_class", "kwargs"), LIKE_PATTERN_EXPECTATIONS, ids=EXPECTATION_IDS
)
def test_legacy_prescriptive_renderer_discloses_the_escape(
    expectation_class: type, kwargs: dict[str, Any]
) -> None:
    rendered = _render(expectation_class(**kwargs, escape="!"), LegacyRendererType.PRESCRIPTIVE)

    assert ", escaping wildcards with $escape" in rendered["template"]
    assert rendered["params"]["escape"] == "!"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("expectation_class", "kwargs"), LIKE_PATTERN_EXPECTATIONS, ids=EXPECTATION_IDS
)
def test_legacy_prescriptive_renderer_omits_an_absent_escape(
    expectation_class: type, kwargs: dict[str, Any]
) -> None:
    rendered = _render(expectation_class(**kwargs), LegacyRendererType.PRESCRIPTIVE)

    assert "$escape" not in rendered["template"]
