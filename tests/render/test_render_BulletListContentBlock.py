import glob
import json

import pytest

from great_expectations.core import ExpectationConfiguration
from great_expectations.data_context.util import file_relative_path
from great_expectations.render.renderer.content_block import (
    ExpectationSuiteBulletListContentBlockRenderer,
)
from great_expectations.render.renderer.content_block.expectation_string import (
    parse_row_condition_string_pandas_engine,
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


def test_parse_row_condition_string_pandas_engine():
    test_condition_string = ""
    assert parse_row_condition_string_pandas_engine(test_condition_string) == (
        "if $row_condition__0",
        {"row_condition__0": "True"},
    )

    test_condition_string = "Age in [0, 42]"
    assert parse_row_condition_string_pandas_engine(test_condition_string) == (
        "if $row_condition__0",
        {"row_condition__0": "Age in [0, 42]"},
    )

    test_condition_string = "Survived == 1 and (SexCode not in (0, 7, x) | ~(Age > 50)) & not (PClass != '1st')"
    assert parse_row_condition_string_pandas_engine(test_condition_string) == (
        "if $row_condition__0 and ($row_condition__1 or not ($row_condition__2)) and not ($row_condition__3)",
        {
            "row_condition__0": "Survived == 1",
            "row_condition__1": "SexCode not in [0, 7, x]",
            "row_condition__2": "Age > 50",
            "row_condition__3": "PClass != '1st'",
        },
    )


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
