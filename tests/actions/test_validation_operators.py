# TODO: ADD TESTS ONCE GET_BATCH IS INTEGRATED!

import pytest

from six import PY2

from freezegun import freeze_time
import pandas as pd

import great_expectations as ge
from great_expectations.validation_operators.validation_operators import (
    ActionListValidationOperator,
    WarningAndFailureExpectationSuitesValidationOperator
)

from great_expectations.data_context import (
    ConfigOnlyDataContext,
)
from great_expectations.data_context.types import DataAssetIdentifier
from ..test_utils import modify_locale


@pytest.fixture()
def basic_data_context_config_for_validation_operator():
    return {
        "plugins_directory": "plugins/",
        "evaluation_parameter_store_name" : "evaluation_parameter_store",
        "expectations_store_name": "expectations_store",
        "datasources": {},
        "stores": {
            "expectations_store" : {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "FixedLengthTupleFilesystemStoreBackend",
                    "base_directory": "expectations/",
                }
            },
            # This isn't currently used for Validation Actions, but it's required for DataContext to work.
            "evaluation_parameter_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "InMemoryStoreBackend",
            },
            "validation_result_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                }
            }
        },
        "validations_store_name": "validation_result_store",
        "data_docs_sites": {},
        "validation_operators" : {},
    }


@modify_locale
@freeze_time("09/26/2019 13:42:41")
def test_errors_warnings_validation_operator_run_slack_query(basic_data_context_config_for_validation_operator, tmp_path_factory, filesystem_csv_4):
    #####
    #####
    #
    # WARNING: PY2 SUPPORT IS UNTESTED BECAUSE OF DICTIONARY ORDER ISSUES NOT YET RESOLVED
    #
    #####
    #####
    if PY2:
        pytest.skip("skipping test_errors_warnings_validation_operator_run_slack_query in py2")

    project_path = str(tmp_path_factory.mktemp('great_expectations'))

    # NOTE: This setup is almost identical to test_DefaultDataContextAwareValidationOperator.
    # Consider converting to a single fixture.

    data_context = ConfigOnlyDataContext(
        basic_data_context_config_for_validation_operator,
        project_path,
    )

    data_context.add_datasource("my_datasource",
                                class_name="PandasDatasource",
                                base_directory=str(filesystem_csv_4))

    data_context.create_expectation_suite(data_asset_name="my_datasource/default/f1",
                                          expectation_suite_name="failure")
    df = data_context.get_batch("my_datasource/default/f1", "failure",
                                batch_kwargs=data_context.yield_batch_kwargs("my_datasource/default/f1"))
    df.expect_column_values_to_be_between(column="x", min_value=1, max_value=9)
    failure_expectations = df.get_expectation_suite(discard_failed_expectations=False)
    data_context.save_expectation_suite(failure_expectations, data_asset_name="my_datasource/default/f1",
                                        expectation_suite_name="failure")


    data_context.create_expectation_suite(data_asset_name="my_datasource/default/f1",
                                          expectation_suite_name="warning")
    df = data_context.get_batch("my_datasource/default/f1", "warning",
                                batch_kwargs=data_context.yield_batch_kwargs("my_datasource/default/f1"))
    df.expect_column_values_to_be_between(column="x", min_value=1, max_value=9)
    df.expect_column_values_to_not_be_null(column="y")
    warning_expectations = df.get_expectation_suite(discard_failed_expectations=False)
    data_context.save_expectation_suite(warning_expectations, data_asset_name="my_datasource/default/f1",
                                        expectation_suite_name="warning")

    data_context.save_expectation_suite(failure_expectations, data_asset_name="my_datasource/default/f2",
                                        expectation_suite_name="failure")
    data_context.save_expectation_suite(failure_expectations, data_asset_name="my_datasource/default/f3",
                                        expectation_suite_name="failure")
    data_context.save_expectation_suite(warning_expectations, data_asset_name="my_datasource/default/f2",
                                        expectation_suite_name="warning")
    data_context.save_expectation_suite(warning_expectations, data_asset_name="my_datasource/default/f3",
                                        expectation_suite_name="warning")

    vo = WarningAndFailureExpectationSuitesValidationOperator(
        data_context=data_context,
        action_list=[],
        slack_webhook="https://hooks.slack.com/services/test/slack/webhook"
    )

    my_df_1 = pd.DataFrame({"x": [1, 2, 3, 4, 5], "y": [1, 2, 3, 4, None]})
    my_ge_df_1 = ge.from_pandas(my_df_1)
    my_ge_df_1._expectation_suite["data_asset_name"] = DataAssetIdentifier("my_datasource","default","f1")

    my_df_2 = pd.DataFrame({"x": [1, 2, 3, 4, 99], "y": [1, 2, 3, 4, 5]})
    my_ge_df_2 = ge.from_pandas(my_df_2)
    my_ge_df_2._expectation_suite["data_asset_name"] = DataAssetIdentifier("my_datasource", "default", "f2")

    my_df_3 = pd.DataFrame({"x": [1, 2, 3, 4, 5], "y": [1, 2, 3, 4, 5]})
    my_ge_df_3 = ge.from_pandas(my_df_3)
    my_ge_df_3._expectation_suite["data_asset_name"] = DataAssetIdentifier("my_datasource", "default", "f3")

    return_obj = vo.run(
        assets_to_validate=[
            my_ge_df_1,
            my_ge_df_2,
            my_ge_df_3
        ],
        run_id="test_100"
    )
    slack_query = vo._build_slack_query(return_obj)
    expected_slack_query = {
        'blocks': [
            {'type': 'divider'},
            {'type': 'section',
             'text': {
                 'type': 'mrkdwn',
                 'text': '*FailureVsWarning Validation Operator Completed.*'}},
            {'type': 'divider'},
            {'type': 'section',
             'text': {
                 'type': 'mrkdwn',
                 'text': '*Status*: Failed :x:'
             }
             },
            {'type': 'section',
             'text': {
                 'type': 'mrkdwn',
                 'text': '*Data Asset List:* [my_datasource/default/f1, my_datasource/default/f2, my_datasource/default/f3]'
             }
             },
            {'type': 'section',
             'text': {''
                      'type': 'mrkdwn',
                      'text': '*Failed Data Assets:* [my_datasource/default/f2]'
                      }
             },
            {'type': 'section',
             'text': {
                 'type': 'mrkdwn', 'text': '*Run ID:* test_100'
             }
             },
            {'type': 'section',
             'text': {
                 'type': 'mrkdwn',
                 'text': '*Timestamp:* 09/26/2019 13:42:41'
             }
             },
            {'type': 'divider'},
            {'type': 'context', 'elements': [
                {'type': 'mrkdwn',
                 'text': 'Learn about FailureVsWarning Validation Operators at https://docs.greatexpectations.io/en/latest/reference/validation_operators/warning_and_failure_expectation_suites_validation_operator.html'
                 }
            ]
             }
        ]}

    # We're okay with system variation in locales (OS X likes 24 hour, but not Travis)
    slack_query['blocks'][7]['text']['text'] = \
        slack_query['blocks'][7]['text']['text'].replace('09/26/2019 13:42:41', 'LOCALEDATE')
    slack_query['blocks'][7]['text']['text'] = \
        slack_query['blocks'][7]['text']['text'].replace('09/26/2019 01:42:41 PM', 'LOCALEDATE')
    expected_slack_query['blocks'][7]['text']['text'] = \
        expected_slack_query['blocks'][7]['text']['text'].replace('09/26/2019 13:42:41', 'LOCALEDATE')
    expected_slack_query['blocks'][7]['text']['text'] = \
        expected_slack_query['blocks'][7]['text']['text'].replace('09/26/2019 01:42:41 PM', 'LOCALEDATE')

    import json
    print(json.dumps(slack_query, indent=2))
    print(json.dumps(expected_slack_query, indent=2))
    assert slack_query == expected_slack_query
