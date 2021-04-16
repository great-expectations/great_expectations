import json
from collections import OrderedDict

import pytest

from great_expectations.data_context.util import file_relative_path
from great_expectations.render.renderer import ProfilingResultsOverviewSectionRenderer
from great_expectations.self_check.util import expectationSuiteValidationResultSchema


@pytest.fixture(scope="module")
def datetime_column_evrs():
    """hand-crafted EVRS for datetime columns"""
    with open(
        file_relative_path(__file__, "../fixtures/datetime_column_evrs.json")
    ) as infile:
        return expectationSuiteValidationResultSchema.load(
            json.load(infile, object_pairs_hook=OrderedDict)
        )


def test_ProfilingResultsOverviewSectionRenderer_render_variable_types(
    datetime_column_evrs,
):
    """Build a type table from a type expectations and assert that we correctly infer column type
    for datetime. Other types would be useful to test for as well."""

    content_blocks = []
    ProfilingResultsOverviewSectionRenderer._render_variable_types(
        datetime_column_evrs, content_blocks
    )
    assert len(content_blocks) == 1
    type_table = content_blocks[0].table
    filtered = [row for row in type_table if row[0] == "datetime"]
    assert filtered == [["datetime", "2"]]
