import pytest

from six import PY2

from freezegun import freeze_time

from great_expectations.render.renderer import (
    SlackRenderer
)
from ..test_utils import modify_locale


@modify_locale
@freeze_time("09/24/2019 23:18:36")
def test_SlackRenderer():
    #####
    #
    # Skipping for PY2
    #
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
    expected_renderer_output = {
        'blocks': [
            {
                'type': 'section',
                'text': {
                    'type': 'mrkdwn',
                    'text': "*Batch Validation Status*: Success :tada:\n*Data Asset:* `{'datasource': 'x', 'generator': 'y', 'generator_asset': 'z'}`\n*Expectation suite name*: `default`\n*Run ID*: `2019-09-25T060538.829112Z`\n*Timestamp*: `09/24/2019 23:18:36`\n*Summary*: *0* of *0* expectations were met"
                }
            },
            {
                'type': 'divider'
            },
            {
                'type': 'context', 'elements': [
                    {
                        'type': 'mrkdwn',
                        'text': 'Learn how to review validation results: https://docs.greatexpectations.io/en/latest/features/validation.html#reviewing-validation-results'
                    }
                ]
            }
        ],
        'text': "{'datasource': 'x', 'generator': 'y', 'generator_asset': 'z'}: Success :tada:"
    }
    rendered_output = SlackRenderer().render(validation_result_suite)

    # We're okay with system variation in locales (OS X likes 24 hour, but not Travis)
    expected_renderer_output['blocks'][0]['text']['text'] = \
        expected_renderer_output['blocks'][0]['text']['text'].replace('09/24/2019 11:18:36 PM', 'LOCALEDATE')
    expected_renderer_output['blocks'][0]['text']['text'] = \
        expected_renderer_output['blocks'][0]['text']['text'].replace('09/24/2019 23:18:36', 'LOCALEDATE')
    rendered_output['blocks'][0]['text']['text'] = \
        rendered_output['blocks'][0]['text']['text'].replace('09/24/2019 11:18:36 PM', 'LOCALEDATE')
    rendered_output['blocks'][0]['text']['text'] = \
        rendered_output['blocks'][0]['text']['text'].replace('09/24/2019 23:18:36', 'LOCALEDATE')

    assert rendered_output == expected_renderer_output
