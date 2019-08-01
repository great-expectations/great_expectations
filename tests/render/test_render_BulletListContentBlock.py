from great_expectations.render.renderer.content_block import (
    PrescriptiveBulletListContentBlockRenderer,
)
from great_expectations.render.renderer.content_block.bullet_list_content_block import (
    substitute_none_for_missing,
)

import glob
import json
import pytest
from string import Template as pTemplate


def test_substitute_none_for_missing():
    assert substitute_none_for_missing(
        kwargs={"a": 1, "b": 2},
        kwarg_list=["c", "d"]
    ) == {"a": 1, "b": 2, "c": None, "d": None}

    my_kwargs = {"a": 1, "b": 2}
    assert substitute_none_for_missing(
        kwargs=my_kwargs,
        kwarg_list=["c", "d"]
    ) == {"a": 1, "b": 2, "c": None, "d": None}
    assert my_kwargs == {"a": 1, "b": 2}, \
        "substitute_none_for_missing should not change input kwargs in place."


# Commenting out the test below. It is helpful during development, but is not a high confidence acceptance test.

@pytest.mark.smoketest
def test_all_expectations_using_test_definitions():
    # Fetch test_definitions for all expectations.
    # Note: as of 6/20/2019, coverage is good, but not 100%
    test_files = glob.glob(
        "tests/test_definitions/*/expect*.json"
    )

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
                    fake_expectation = {
                        "expectation_type": test_definitions["expectation_type"],
                        "kwargs": test["in"],
                    }
                else:
                    # This would be a good place to put a kwarg-to-arg converter
                    continue

                try:
                    # Attempt to render it
                    render_result = PrescriptiveBulletListContentBlockRenderer.render(
                        [fake_expectation])
                    # print(fake_expectation)
                    # Assert that the rendered result matches the intended format
                    # Note: THIS DOES NOT TEST CONTENT AT ALL.
                    # Abe 6/22/2019: For the moment, I think it's fine to not test content.
                    # I'm on the fence about the right end state for testing renderers at this level.
                    # Spot checks, perhaps?
                    assert isinstance(render_result, dict)
                    assert "content_block_type" in render_result
                    assert render_result["content_block_type"] in render_result
                    assert isinstance(render_result[render_result["content_block_type"]], list )

                    # TODO: Assert that the template is renderable, with all the right arguments, etc.
                    # rendered_template = pTemplate(el["template"]).substitute(el["params"])

                    test_results[test_definitions["expectation_type"]].append({
                        test["title"]:render_result, 
                        # "rendered_template":rendered_template
                        })
                
                except Exception:
                    print(test['title'])
                    raise

                except AssertionError:
                    raise
                #     # If the assertions fail, then print the expectation to allow debugging.
                #     # Do NOT trap other errors, so that developers can debug using the full traceback.
                #     print(fake_expectation)
                #     all_true = False
                #     failure_count += 1

                # except Exception as e:
                #     print(fake_expectation)
                #     raise(e)

                # total_count += 1

    # print(len(types))
    # print(len(set(types)))
    # print(total_count-failure_count, "of", total_count,
    #       "succeeded (", 1-failure_count*1./total_count, ")")
    # TODO: accommodate case where multiple datasets exist within one expectation test definition
    with open('./tests/render/output/test_render_bullet_list_content_block.json', 'w') as f:
       json.dump(test_results, f, indent=2)

    # assert all_true
