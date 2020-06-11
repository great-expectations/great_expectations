import glob
import json

import pytest

from great_expectations.core import ExpectationConfiguration
from great_expectations.data_context.util import file_relative_path
from great_expectations.render.renderer.content_block import (
    ExpectationSuiteBulletListContentBlockRenderer,
)
from great_expectations.render.renderer.content_block.expectation_string import (
    substitute_none_for_missing,
)


def test_substitute_none_for_missing():
    assert substitute_none_for_missing(
        kwargs={"a": 1, "b": 2}, kwarg_list=["c", "d"]
    ) == {"a": 1, "b": 2, "c": None, "d": None}

    my_kwargs = {"a": 1, "b": 2}
    assert substitute_none_for_missing(kwargs=my_kwargs, kwarg_list=["c", "d"]) == {
        "a": 1,
        "b": 2,
        "c": None,
        "d": None,
    }
    assert my_kwargs == {
        "a": 1,
        "b": 2,
    }, "substitute_none_for_missing should not change input kwargs in place."


@pytest.mark.smoketest
@pytest.mark.rendered_output
def test_all_expectations_using_test_definitions():
    test_files = glob.glob("tests/test_definitions/*/expect*.json")

    all_true = True
    failure_count, total_count = 0, 0
    types = []

    # Loop over all test_files, datasets, and tests:
    test_results = {}
    for filename in test_files:
        test_definitions = json.load(open(filename))
        types.append(test_definitions["expectation_type"])

        test_results[test_definitions["expectation_type"]] = []

        for dataset in test_definitions["datasets"]:

            for test in dataset["tests"]:
                # Construct an expectation from the test.
                if type(test["in"]) == dict:
                    fake_expectation = ExpectationConfiguration(
                        expectation_type=test_definitions["expectation_type"],
                        kwargs=test["in"],
                    )
                else:
                    # This would be a good place to put a kwarg-to-arg converter
                    continue

                # Attempt to render it
                render_result = ExpectationSuiteBulletListContentBlockRenderer.render(
                    [fake_expectation]
                ).to_json_dict()

                assert isinstance(render_result, dict)
                assert "content_block_type" in render_result
                assert render_result["content_block_type"] in render_result
                assert isinstance(
                    render_result[render_result["content_block_type"]], list
                )

                # TODO: Assert that the template is renderable, with all the right arguments, etc.
                # rendered_template = pTemplate(el["template"]).substitute(el["params"])

                test_results[test_definitions["expectation_type"]].append(
                    {
                        test["title"]: render_result,
                        # "rendered_template":rendered_template
                    }
                )

    # TODO: accommodate case where multiple datasets exist within one expectation test definition

    # We encountered unicode coding errors on Python 2, but since this is just a smoke test, review the smoke test results in python 3.

    with open(
        file_relative_path(
            __file__, "./output/test_render_bullet_list_content_block.json"
        ),
        "w",
    ) as f:
        json.dump(test_results, f, indent=2)
