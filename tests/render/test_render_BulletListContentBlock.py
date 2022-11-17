import glob
import json
import os
from collections import defaultdict

import pytest

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context.util import file_relative_path
from great_expectations.render.renderer.content_block import (
    ExpectationSuiteBulletListContentBlockRenderer,
)
from great_expectations.render.util import (
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
@pytest.mark.slow  # 1.52s
def test_all_expectations_using_test_definitions():
    # Chetan - 20220129 - During v0.14.4, it was revealed that this test was broken.
    # The `glob` statement did not pick up any relevant tests, resulting in `test_files` being empty.
    # Without anything to iterate over, the test gave the impression of passing (when in actuality, it never tested anything).
    #
    # After some research, it seems as though this has been broken since the v0.13.0 release.
    # The 5 Expectations noted below are implemented or updated after v0.13.0 and are incompatible with this test fixture due to
    # having incomplete render methods.
    #
    # As this behavior is implemented, the `UNSUPPORTED_EXPECTATIONS` list will be updated to reflect GE's current capabilities.

    dir_path = os.path.dirname(os.path.abspath(__file__))
    pattern = os.path.join(
        dir_path, "..", "..", "tests", "test_definitions", "*", "expect*.json"
    )
    test_files = glob.glob(pattern)

    # Historically, collecting all the JSON tests was an issue - this step ensures we actually have test data.
    assert (
        len(test_files) == 60
    ), "Something went wrong when collecting JSON Expectation test fixtures"

    # The following do not work with this parameterized test due to incomplete render methods.
    UNSUPPORTED_EXPECTATIONS = {
        "expect_column_values_to_match_like_pattern",
        "expect_column_values_to_match_like_pattern_list",
        "expect_column_values_to_not_match_like_pattern",
        "expect_column_values_to_not_match_like_pattern_list",
        "expect_multicolumn_sum_to_equal",
    }

    # Loop over all test_files, datasets, and tests:
    test_results = defaultdict(list)
    for filename in test_files:
        test_definitions = json.load(open(filename))

        # Chetan -20220129 - To be removed once all expectations are supported
        if test_definitions["expectation_type"] in UNSUPPORTED_EXPECTATIONS:
            continue

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
                print(fake_expectation)
                render_result = ExpectationSuiteBulletListContentBlockRenderer.render(
                    [fake_expectation]
                )
                assert render_result is not None
                render_result = render_result.to_json_dict()

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
