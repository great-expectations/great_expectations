from freezegun import freeze_time

from great_expectations.render.renderer import (
    SlackRenderer
)

@freeze_time("09/24/19 23:18:36")
def test_SlackRenderer():
    validation_result_suite = {'results': [], 'success': True,
                               'statistics': {'evaluated_expectations': 0, 'successful_expectations': 0,
                                              'unsuccessful_expectations': 0, 'success_percent': None},
                               'meta': {'great_expectations.__version__': 'v0.8.0__develop',
                                        'data_asset_name': {'datasource': 'x', 'generator': 'y',
                                                            'generator_asset': 'z'},
                                        'expectation_suite_name': 'default', 'run_id': '2019-09-25T060538.829112Z'}}
    expected_renderer_output = {'blocks': [{'type': 'section', 'text': {'type': 'mrkdwn',
                                                                        'text': "*Validated batch from data asset:* `{'datasource': 'x', 'generator': 'y', 'generator_asset': 'z'}`\n*Status: Success :tada:*\n0 of 0 expectations were met\n\n"}},
                                           {'type': 'context', 'elements': [{'type': 'mrkdwn',
                                                                             'text': 'Great Expectations run id 2019-09-25T060538.829112Z ran at 09/24/19 23:18:36'}]}]}
    renderer_output = SlackRenderer().render(validation_result_suite)
    
    assert renderer_output == expected_renderer_output
