# TODO: ADD TESTS ONCE GET_BATCH IS INTEGRATED!

import pytest
import json
import copy

from freezegun import freeze_time
import pandas as pd

import great_expectations as ge
from great_expectations.actions.validation_operators import (
    PerformActionListValidationOperator,
    ErrorsVsWarningsValidationOperator
)

from great_expectations.data_context import (
    ConfigOnlyDataContext,
)
from great_expectations.data_context.types import (
    DataContextConfig,
    DataAssetIdentifier,
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)

@pytest.fixture()
def basic_data_context_config_for_validation_operator():
    return DataContextConfig(**{
        "plugins_directory": "plugins/",
        "evaluation_parameter_store_name" : "evaluation_parameter_store",
        "expectations_store" : {
            "class_name": "ExpectationStore",
            "store_backend": {
                "class_name": "FixedLengthTupleFilesystemStoreBackend",
                "base_directory": "expectations/",
            }
        },
        "datasources": {},
        "stores": {
            # This isn't currently used for Validation Actions, but it's required for DataContext to work.
            "evaluation_parameter_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "InMemoryStoreBackend",
            },
            "validation_result_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "ValidationResultStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                }
            }
        },
        "profiling_store_name": "validation_result_store",
        "data_docs": {
            "sites": {}
        },
        "validation_operators" : {},
    })


# def test_perform_action_list_validation_operator_run(basic_data_context_config_for_validation_operator, tmp_path_factory, filesystem_csv_4):
#     project_path = str(tmp_path_factory.mktemp('great_expectations'))
#
#     # NOTE: This setup is almost identical to test_DefaultDataContextAwareValidationOperator.
#     # Consider converting to a single fixture.
#
#     data_context = ConfigOnlyDataContext(
#         basic_data_context_config_for_validation_operator,
#         project_path,
#     )
#
#     data_context.add_datasource("my_datasource",
#                                 class_name="PandasDatasource",
#                                 base_directory=str(filesystem_csv_4))
#
#     # NOTE : It's kinda annoying that these Expectation Suites start out with expect_column_to_exist.
#     # How do I turn off that default...?
#     df = data_context.get_batch("my_datasource/default/f1")
#     df.expect_column_values_to_be_between(column="x", min_value=1, max_value=9)
#     failure_expectations = df.get_expectation_suite(discard_failed_expectations=False)
#
#     df.expect_column_values_to_not_be_null(column="y")
#     warning_expectations = df.get_expectation_suite(discard_failed_expectations=False)
#
#     df = data_context.get_batch("my_datasource/default/f1")
#     df.expect_column_values_to_be_in_set(column="x", value_set=[1,3,5,7,9])
#     quarantine_expectations = df.get_expectation_suite(discard_failed_expectations=False)
#
#     data_asset_identifier = DataAssetIdentifier("my_datasource", "default", "f1")
#     data_context.stores["expectations_store"].set(
#         ExpectationSuiteIdentifier(
#             data_asset_name=data_asset_identifier,
#             expectation_suite_name="failure"
#         ),
#         failure_expectations
#     )
#     data_context.stores["expectations_store"].set(
#         ExpectationSuiteIdentifier(
#             data_asset_name=data_asset_identifier,
#             expectation_suite_name="warning"
#         ),
#         warning_expectations
#     )
#     data_context.stores["expectations_store"].set(
#         ExpectationSuiteIdentifier(
#             data_asset_name=data_asset_identifier,
#             expectation_suite_name="quarantine"
#         ),
#         quarantine_expectations
#     )
#
#     vo = PerformActionListValidationOperator(
#         data_context=data_context,
#         action_list = [
#             {
#             "name": "store_validation_result",
#             "action" : {
#                 "module_name" : "great_expectations.actions",
#                 "class_name" : "StoreAction",
#                 "target_store_name": "validation_result_store",
#                 "summarizer":{
#                     "module_name": "tests.test_plugins.fake_actions",
#                     "class_name": "TemporaryNoOpSummarizer",
#                 }
#             }
#         },{
#             "name": "add_failures_to_store",
#             "result_key": "validation_results.failure",
#             "action" : {
#                 "module_name" : "great_expectations.actions",
#                 "class_name" : "SummarizeAndStoreAction",
#                 "target_store_name": "validation_result_store",
#                 "summarizer":{
#                     "module_name": "tests.test_plugins.fake_actions",
#                     "class_name": "TemporaryNoOpSummarizer",
#                 }
#             }
#         }
#         ],
#     )
#
#     my_df = pd.DataFrame({"x": [1,2,3,4,5], "y": [1,2,3,4,None]})
#     my_ge_df = ge.from_pandas(my_df)
#
#     assert data_context.stores["validation_result_store"].list_keys() == []
#
#     results = vo.run(
#         data_asset=my_ge_df,
#         data_asset_identifier=DataAssetIdentifier(
#             from_string="DataAssetIdentifier.my_datasource.default.f1"
#         ),
#         run_identifier="test_100"
#     )
#     # print(json.dumps(results["validation_results"], indent=2))
#
#     validation_result_store_keys = data_context.stores["validation_result_store"].list_keys()
#     print(validation_result_store_keys)
#     assert len(validation_result_store_keys) == 2
#     assert ValidationResultIdentifier(from_string="ValidationResultIdentifier.my_datasource.default.f1.warning.test_100") in validation_result_store_keys
#     assert ValidationResultIdentifier(from_string="ValidationResultIdentifier.my_datasource.default.f1.failure.test_100") in validation_result_store_keys
#
#     assert data_context.stores["validation_result_store"].get(
#         ValidationResultIdentifier(from_string="ValidationResultIdentifier.my_datasource.default.f1.warning.test_100")
#     )["success"] == False
#     assert data_context.stores["validation_result_store"].get(
#         ValidationResultIdentifier(from_string="ValidationResultIdentifier.my_datasource.default.f1.failure.test_100")
#     )["success"] == True
#
#     #TODO: One DataSnapshotStores are implemented, add a test for quarantined data


@freeze_time("09/26/19 13:42:41")
def test_errors_warnings_validation_operator_run_slack_query(basic_data_context_config_for_validation_operator, tmp_path_factory, filesystem_csv_4):
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

    data_context.create_expectation_suite(data_asset_name="my_datasource/default/f1", expectation_suite_name="failure")
    df = data_context.get_batch("my_datasource/default/f1", "failure",
                                batch_kwargs=data_context.yield_batch_kwargs("my_datasource/default/f1"))
    df.expect_column_values_to_be_between(column="x", min_value=1, max_value=9)
    failure_expectations = df.get_expectation_suite(discard_failed_expectations=False)
    data_context.save_expectation_suite(failure_expectations, data_asset_name="my_datasource/default/f1",
                                        expectation_suite_name="failure")


    data_context.create_expectation_suite(data_asset_name="my_datasource/default/f1", expectation_suite_name="warning")
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


    vo = ErrorsVsWarningsValidationOperator(
        data_context=data_context,
        action_list = [],
        slack_webhook="https://hooks.slack.com/services/test/slack/webhook"
    )

    my_df_1 = pd.DataFrame({"x": [1,2,3,4,5], "y": [1,2,3,4,None]})
    my_ge_df_1 = ge.from_pandas(my_df_1)
    my_ge_df_1._expectation_suite["data_asset_name"] = DataAssetIdentifier("my_datasource","default","f1")

    my_df_2 = pd.DataFrame({"x": [1,2,3,4,99], "y": [1,2,3,4,5]})
    my_ge_df_2 = ge.from_pandas(my_df_2)
    my_ge_df_2._expectation_suite["data_asset_name"] = DataAssetIdentifier("my_datasource", "default", "f2")
    
    my_df_3 = pd.DataFrame({"x": [1,2,3,4,5], "y": [1,2,3,4,5]})
    my_ge_df_3 = ge.from_pandas(my_df_3)
    my_ge_df_3._expectation_suite["data_asset_name"] = DataAssetIdentifier("my_datasource", "default", "f3")

    return_obj = vo.run(
        assets_to_validate=[
            my_ge_df_1,
            my_ge_df_2,
            my_ge_df_3
        ],
        run_identifier="test_100"
    )
    slack_query = vo._build_slack_query(return_obj)
    expected_slack_query = {'blocks': [{'type': 'divider'}, {'type': 'section', 'text': {'type': 'mrkdwn',
                                                                                         'text': '*FailureVsWarning Validation Operator Completed.*'}},
                                       {'type': 'divider'},
                                       {'type': 'section', 'text': {'type': 'mrkdwn', 'text': '*Status*: Failed :x:'}},
                                       {'type': 'section', 'text': {'type': 'mrkdwn',
                                                                    'text': "*Data Asset List:* [{'datasource': 'my_datasource', 'generator': 'default', 'generator_asset': 'f1'}, {'datasource': 'my_datasource', 'generator': 'default', 'generator_asset': 'f2'}, {'datasource': 'my_datasource', 'generator': 'default', 'generator_asset': 'f3'}]"}},
                                       {'type': 'section', 'text': {'type': 'mrkdwn',
                                                                    'text': "*Failed Data Assets:* [{'datasource': 'my_datasource', 'generator': 'default', 'generator_asset': 'f2'}]"}},
                                       {'type': 'section', 'text': {'type': 'mrkdwn', 'text': '*Run ID:* test_100'}},
                                       {'type': 'section',
                                        'text': {'type': 'mrkdwn', 'text': '*Timestamp:* 09/26/19 13:42:41'}},
                                       {'type': 'divider'}, {'type': 'context', 'elements': [{'type': 'mrkdwn',
                                                                                              'text': 'Learn about FailureVsWarning Validation Operators at https://docs.greatexpectations.io/en/latest/guides/failure_vs_warning_validation_operator.html'}]}]}

    assert slack_query == expected_slack_query

# TODO: replace once the updated get_batch is integrated.
# def test__get_or_convert_to_batch_from_identifiers(basic_data_context_config_for_validation_operator, filesystem_csv):
#
#     data_context = ConfigOnlyDataContext(
#         basic_data_context_config_for_validation_operator,
#         project_path,
#     )
#
#     data_context.add_datasource("my_datasource",
#                                 class_name="PandasDatasource",
#                                 base_directory=str(filesystem_csv_4))
#
#     data_asset_name = "my_datasource/default/f1"
#     data_context.create_expectation_suite(data_asset_name, "foo")
#     df = data_context.get_batch(data_asset_name,
#                                 "foo",
#                                 batch_kwargs=data_context.yield_batch_kwargs(data_asset_name))
#     df.expect_column_values_to_be_between(column="x", min_value=1, max_value=9)
#     failure_expectations = df.get_expectation_suite(discard_failed_expectations=False)
#
#     df.expect_column_values_to_not_be_null(column="y")
#     warning_expectations = df.get_expectation_suite(discard_failed_expectations=False)
#
#     data_context.create_expectation_suite(data_asset_name, "default")
#     df = data_context.get_batch(data_asset_name, "default",
#                                 batch_kwargs=data_context.yield_batch_kwargs(data_asset_name))
#     df.expect_column_values_to_be_in_set(column="x", value_set=[1,3,5,7,9])
#     quarantine_expectations = df.get_expectation_suite(discard_failed_expectations=False)
#
#     data_asset_identifier = DataAssetIdentifier("my_datasource", "default", "f1")
#     data_context.stores["expectations_store"].set(
#         ExpectationSuiteIdentifier(
#             data_asset_name=data_asset_identifier,
#             expectation_suite_name="failure"
#         ),
#         failure_expectations
#     )
#     data_context.stores["expectations_store"].set(
#         ExpectationSuiteIdentifier(
#             data_asset_name=data_asset_identifier,
#             expectation_suite_name="warning"
#         ),
#         warning_expectations
#     )
#     data_context.stores["expectations_store"].set(
#         ExpectationSuiteIdentifier(
#             data_asset_name=data_asset_identifier,
#             expectation_suite_name="quarantine"
#         ),
#         quarantine_expectations
#     )
#
#     vo = PerformActionListValidationOperator(
#         data_context=data_context,
#         action_list = [
#             {
#             "name": "store_validation_result",
#             "action" : {
#                 "module_name" : "great_expectations.actions",
#                 "class_name" : "StoreAction",
#                 "target_store_name": "validation_result_store",
#                 "summarizer":{
#                     "module_name": "tests.test_plugins.fake_actions",
#                     "class_name": "TemporaryNoOpSummarizer",
#                 }
#             }
#         },{
#             "name": "add_failures_to_store",
#             "result_key": "validation_results.failure",
#             "action" : {
#                 "module_name" : "great_expectations.actions",
#                 "class_name" : "SummarizeAndStoreAction",
#                 "target_store_name": "validation_result_store",
#                 "summarizer":{
#                     "module_name": "tests.test_plugins.fake_actions",
#                     "class_name": "TemporaryNoOpSummarizer",
#                 }
#             }
#         }
#         ],
#     )
#
#     my_df = pd.DataFrame({"x": [1,2,3,4,5], "y": [1,2,3,4,None]})
#     my_ge_df = ge.from_pandas(my_df)
#
#     assert data_context.stores["validation_result_store"].list_keys() == []
#
#     ## FIXME: This signature has changed
#     results = vo.run(
#         data_asset=my_ge_df,
#         data_asset_identifier=DataAssetIdentifier(
#             from_string="DataAssetIdentifier.my_datasource.default.f1"
#         ),
#         run_identifier="test_100"
#     )
#     # print(json.dumps(results["validation_results"], indent=2))
#
#     validation_result_store_keys = data_context.stores["validation_result_store"].list_keys()
#     print(validation_result_store_keys)
#     assert len(validation_result_store_keys) == 2
#     assert ValidationResultIdentifier(from_string="ValidationResultIdentifier.my_datasource.default.f1.warning.test_100") in validation_result_store_keys
#     assert ValidationResultIdentifier(from_string="ValidationResultIdentifier.my_datasource.default.f1.failure.test_100") in validation_result_store_keys
#
#     assert data_context.stores["validation_result_store"].get(
#         ValidationResultIdentifier(from_string="ValidationResultIdentifier.my_datasource.default.f1.warning.test_100")
#     )["success"] == False
#     assert data_context.stores["validation_result_store"].get(
#         ValidationResultIdentifier(from_string="ValidationResultIdentifier.my_datasource.default.f1.failure.test_100")
#     )["success"] == True
#
#     #TODO: One DataSnapshotStores are implemented, add a test for quarantined data
#
#
# def test_errors_warnings_validation_operator_run(basic_data_context_config_for_validation_operator, tmp_path_factory, filesystem_csv_4):
#     project_path = str(tmp_path_factory.mktemp('great_expectations'))
#
#     # NOTE: This setup is almost identical to test_DefaultDataContextAwareValidationOperator.
#     # Consider converting to a single fixture.
#
#     data_context = ConfigOnlyDataContext(
#         basic_data_context_config_for_validation_operator,
#         project_path,
#     )
#
#     data_context.add_datasource("my_datasource",
#                                 class_name="PandasDatasource",
#                                 base_directory=str(filesystem_csv_4))
#
#     # NOTE : It's kinda annoying that these Expectation Suites start out with expect_column_to_exist.
#     # How do I turn off that default...?
#     df = data_context.get_batch("my_datasource/default/f1")
#     df.expect_column_values_to_be_between(column="x", min_value=1, max_value=9)
#     failure_expectations = df.get_expectation_suite(discard_failed_expectations=False)
#
#     df.expect_column_values_to_not_be_null(column="y")
#     warning_expectations = df.get_expectation_suite(discard_failed_expectations=False)
#
#
#     data_asset_identifier_1 = DataAssetIdentifier("my_datasource", "default", "f1")
#     data_context.stores["expectations_store"].set(
#         ExpectationSuiteIdentifier(
#             data_asset_name=data_asset_identifier_1,
#             expectation_suite_name="failure"
#         ),
#         failure_expectations
#     )
#     data_context.stores["expectations_store"].set(
#         ExpectationSuiteIdentifier(
#             data_asset_name=data_asset_identifier_1,
#             expectation_suite_name="warning"
#         ),
#         warning_expectations
#     )
#
#     data_asset_identifier_2 = DataAssetIdentifier("my_datasource", "default", "f2")
#     data_context.stores["expectations_store"].set(
#         ExpectationSuiteIdentifier(
#             data_asset_name=data_asset_identifier_2,
#             expectation_suite_name="failure"
#         ),
#         failure_expectations
#     )
#     data_context.stores["expectations_store"].set(
#         ExpectationSuiteIdentifier(
#             data_asset_name=data_asset_identifier_2,
#             expectation_suite_name="warning"
#         ),
#         warning_expectations
#     )
#
#     data_asset_identifier_3 = DataAssetIdentifier("my_datasource", "default", "f3")
#     data_context.stores["expectations_store"].set(
#         ExpectationSuiteIdentifier(
#             data_asset_name=data_asset_identifier_3,
#             expectation_suite_name="failure"
#         ),
#         failure_expectations
#     )
#     data_context.stores["expectations_store"].set(
#         ExpectationSuiteIdentifier(
#             data_asset_name=data_asset_identifier_3,
#             expectation_suite_name="warning"
#         ),
#         warning_expectations
#     )
#
#     vo = ErrorsVsWarningsValidationOperator(
#         data_context=data_context,
#         action_list = [],
#         slack_webhook="https://hooks.slack.com/services/test/slack/webhook"
#     )
#
#     my_df_1 = pd.DataFrame({"x": [1,2,3,4,5], "y": [1,2,3,4,None]})
#     my_ge_df_1 = ge.from_pandas(my_df_1)
#     my_ge_df_1._expectation_suite["data_asset_name"] = DataAssetIdentifier("my_datasource","default","f1")
#
#     my_df_2 = pd.DataFrame({"x": [1,2,3,4,99], "y": [1,2,3,4,5]})
#     my_ge_df_2 = ge.from_pandas(my_df_2)
#     my_ge_df_2._expectation_suite["data_asset_name"] = DataAssetIdentifier("my_datasource", "default", "f2")
#
#     my_df_3 = pd.DataFrame({"x": [1,2,3,4,5], "y": [1,2,3,4,5]})
#     my_ge_df_3 = ge.from_pandas(my_df_3)
#     my_ge_df_3._expectation_suite["data_asset_name"] = DataAssetIdentifier("my_datasource", "default", "f3")
#
#     results = vo.run(
#         assets_to_validate=[
#             my_ge_df_1,
#             my_ge_df_2,
#             my_ge_df_3
#         ],
#         run_identifier="test_100"
#     )
#     test = 2134
#     # print(json.dumps(results["validation_results"], indent=2))
