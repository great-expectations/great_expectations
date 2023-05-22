import pathlib

import pytest

from docs.prepare_prior_versions import (
    _update_tag_references_for_correct_version_substitution,
    _use_relative_imports_for_tag_references_substitution,
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


@pytest.mark.unit
def test__use_relative_imports_for_tag_references_substitution():
    contents = """import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you connect to your data stored on GCS using Pandas.
"""

    relative_path = pathlib.Path("../../../../../")
    updated_contents = _use_relative_imports_for_tag_references_substitution(
        contents, relative_path
    )

    expected_contents = """import TabItem from '@theme/TabItem';
import TechnicalTag from '../../../../../term_tags/_tag.mdx';

This guide will help you connect to your data stored on GCS using Pandas.
"""

    assert updated_contents == expected_contents
