from great_expectations.render.renderer.content_block import BulletListContentBlock

import glob
import json


def test_all_expectations_using_test_definitions():

    test_files = glob.glob("tests/test_definitions/*/expect*.json")

    all_true = True
    failure_count, total_count = 0, 0
    types = []
    for filename in test_files:
        test_definitions = json.load(open(filename))
        types.append(test_definitions["expectation_type"])

        for dataset in test_definitions["datasets"]:

            for test in dataset["tests"]:
                fake_expectation = {
                    "expectation_type": test_definitions["expectation_type"],
                    "kwargs": test["in"],
                }

                try:
                    render_result = BulletListContentBlock.render(
                        fake_expectation)

                    assert render_result != None
                    assert set(render_result.keys()) == set(
                        'template', 'params')

                except:
                    print(fake_expectation)
                    all_true = False
                    failure_count += 1

                total_count += 1

    # print(len(types))
    # print(len(set(types)))
    print(total_count-failure_count, "of", total_count,
          "suceeded (", 1-failure_count*1./total_count, ")")
    assert all_true
