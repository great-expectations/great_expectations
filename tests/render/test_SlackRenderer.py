import pytest

from six import PY2

from freezegun import freeze_time

from great_expectations.render.renderer import (
    SlackRenderer
)


@freeze_time("09/24/19 23:18:36")
def test_SlackRenderer():

    #####
    #####
    #
    # Skipping for PY2
    #
    #####
    #####
    if PY2:
        pytest.skip("skipping test_SlackRenderer for PY2")


    validation_result_suite = {'results': [], 'success': True,
                               'statistics': {'evaluated_expectations': 0, 'successful_expectations': 0,
                                              'unsuccessful_expectations': 0, 'success_percent': None},
                               'meta': {'great_expectations.__version__': 'v0.8.0__develop',
                                        'data_asset_name': {'datasource': 'x', 'generator': 'y',
                                                            'generator_asset': 'z'},
                                        'expectation_suite_name': 'default', 'run_id': '2019-09-25T060538.829112Z'}}
    expected_renderer_output = {'blocks': [{'type': 'divider'}, {'type': 'section', 'text': {'type': 'mrkdwn',
                                                                                             'text': "*Validated batch from data asset:* `{'datasource': 'x', 'generator': 'y', 'generator_asset': 'z'}`\n*Status: Success :tada:*\n*Run ID:* 2019-09-25T060538.829112Z\n*Timestamp:* 09/24/19 23:18:36\n*Summary:* 0 of 0 expectations were met\n\n"}},
                                           {'type': 'divider'}, {'type': 'context', 'elements': [{'type': 'mrkdwn',
                                                                                                  'text': 'Learn how to review validation results at https://docs.greatexpectations.io/en/latest/features/validation.html#reviewing-validation-results'}]}]}
    renderer_output = SlackRenderer().render(validation_result_suite)
    
    assert renderer_output == expected_renderer_output
