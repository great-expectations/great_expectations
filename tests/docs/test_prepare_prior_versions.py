import pytest

from docs.prepare_prior_versions import (
    _update_tag_references_for_correct_version_substitution,
)


@pytest.mark.unit
def test__update_tag_references_for_correct_version_substitution():
    contents = """import data from '../term_tags/terms.json'

<span class="tooltip">
    <a href={'/docs/' + data[props.tag].url}>{props.text}</a>
    <span class="tooltiptext">{data[props.tag].definition}</span>
</span>"""

    version = "0.15.50"
    updated_contents = _update_tag_references_for_correct_version_substitution(
        contents=contents, version=version
    )
    expected_contents = """import data from '../term_tags/terms.json'

<span class="tooltip">
    <a href={'/docs/0.15.50/' + data[props.tag].url}>{props.text}</a>
    <span class="tooltiptext">{data[props.tag].definition}</span>
</span>"""
    assert updated_contents == expected_contents
