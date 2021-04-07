# TODO: ADD TESTS ONCE GET_BATCH IS INTEGRATED!

import pandas as pd
import pytest
from freezegun import freeze_time

import great_expectations as ge
from great_expectations.data_context import BaseDataContext
from great_expectations.self_check.util import modify_locale
from great_expectations.validation_operators.validation_operators import (
    WarningAndFailureExpectationSuitesValidationOperator,
)


@pytest.fixture
def assets_to_validate():
    # succeeded "failure-level" suite
    # failed "warning-level" suite
    my_df_1 = pd.DataFrame({"x": [1, 2, 3, 4, 5], "y": [1, 2, 3, 4, None]})
    my_ge_df_1 = ge.dataset.PandasDataset(
        my_df_1, batch_kwargs={"ge_batch_id": "82a8de83-e063-11e9-8226-acde48001122"}
    )

    # failed "failure-level" suite
    # failed "warning-level" suite
    my_df_2 = pd.DataFrame({"x": [1, 2, 3, 4, 99], "y": [1, 2, 3, 4, None]})
    my_ge_df_2 = ge.dataset.PandasDataset(
        my_df_2, batch_kwargs={"ge_batch_id": "82a8de83-e063-11e9-8133-acde48001122"}
    )

    # succeeded "failure-level" suite
    # succeeded "warning-level" suite
    my_df_3 = pd.DataFrame({"x": [1, 2, 3, 4, 5], "y": [1, 2, 3, 4, 5]})
    my_ge_df_3 = ge.dataset.PandasDataset(
        my_df_3, batch_kwargs={"ge_batch_id": "82a8de83-e063-11e9-a53d-acde48001122"}
    )

    # failed "failure-level" suite
    # succeeded "warning-level" suite
    my_df_4 = pd.DataFrame({"x": [1, 2, 3, 4, 99], "y": [1, 2, 3, 4, 5]})
    my_ge_df_4 = ge.dataset.PandasDataset(
        my_df_4, batch_kwargs={"ge_batch_id": "82a8de83-e063-11e9-8133-acde48001122"}
    )
    return [my_ge_df_1, my_ge_df_2, my_ge_df_3, my_ge_df_4]


@pytest.fixture
def warning_failure_validation_operator_data_context(
    basic_data_context_config_for_validation_operator,
    tmp_path_factory,
    filesystem_csv_4,
):
    project_path = str(tmp_path_factory.mktemp("great_expectations"))

    # NOTE: This setup is almost identical to test_DefaultDataContextAwareValidationOperator.
    # Consider converting to a single fixture.

    data_context = BaseDataContext(
        basic_data_context_config_for_validation_operator,
        project_path,
    )

    data_context.add_datasource(
        "my_datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": str(filesystem_csv_4),
            }
        },
    )

    data_context.create_expectation_suite(expectation_suite_name="f1.failure")
    df = data_context.get_batch(
        expectation_suite_name="f1.failure",
        batch_kwargs=data_context.build_batch_kwargs(
            "my_datasource", "subdir_reader", "f1"
        ),
    )
    df.expect_column_values_to_be_between(column="x", min_value=1, max_value=9)
    failure_expectations = df.get_expectation_suite(discard_failed_expectations=False)
    data_context.save_expectation_suite(
        failure_expectations, expectation_suite_name="f1.failure"
    )

    data_context.create_expectation_suite(expectation_suite_name="f1.warning")
    df = data_context.get_batch(
        expectation_suite_name="f1.warning",
        batch_kwargs=data_context.build_batch_kwargs(
            "my_datasource", "subdir_reader", "f1"
        ),
    )
    df.expect_column_values_to_be_between(
        column="x", min_value=1, max_value=9, mostly=0.8
    )
    df.expect_column_values_to_not_be_null(column="y")
    warning_expectations = df.get_expectation_suite(discard_failed_expectations=False)
    data_context.save_expectation_suite(
        warning_expectations, expectation_suite_name="f1.warning"
    )

    data_context.save_expectation_suite(
        failure_expectations, expectation_suite_name="f2.failure"
    )
    data_context.save_expectation_suite(
        failure_expectations, expectation_suite_name="f3.failure"
    )
    data_context.save_expectation_suite(
        warning_expectations, expectation_suite_name="f2.warning"
    )
    data_context.save_expectation_suite(
        warning_expectations, expectation_suite_name="f3.warning"
    )
    return data_context


@modify_locale
@freeze_time("09/26/2019 13:42:41")
def test_errors_warnings_validation_operator_run_slack_query(
    warning_failure_validation_operator_data_context, assets_to_validate
):
    data_context = warning_failure_validation_operator_data_context

    vo = WarningAndFailureExpectationSuitesValidationOperator(
        data_context=data_context,
        action_list=[],
        name="test",
        slack_webhook="https://hooks.slack.com/services/test/slack/webhook",
    )

    return_obj = vo.run(
        assets_to_validate=assets_to_validate,
        run_id="test_100",
        base_expectation_suite_name="f1",
    )
    slack_query = vo._build_slack_query(return_obj)
    expected_slack_query = {
        "blocks": [
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*FailureVsWarning Validation Operator Completed.*",
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Status*: Failed :x:"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Batch Id List:* ['ge_batch_id=82a8de83-e063-11e9-8133-acde48001122', "
                    "'ge_batch_id=82a8de83-e063-11e9-8226-acde48001122', "
                    "'ge_batch_id=82a8de83-e063-11e9-a53d-acde48001122']",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Failed Batches:* ['f1.failure-ge_batch_id=82a8de83-e063-11e9-8133-acde48001122']",
                },
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Run Name:* test_100"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Run Time:* LOCALEDATE"},
            },
            {"type": "divider"},
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Learn about FailureVsWarning Validation Operators at https://docs.greatexpectations.i"
                        "o/en/latest/reference/validation_operators/warning_and_failure_expectation_suites_val"
                        "idation_operator.html",
                    }
                ],
            },
        ]
    }

    # We're okay with system variation in locales (OS X likes 24 hour, but not Travis)
    slack_query["blocks"][7]["text"]["text"] = slack_query["blocks"][7]["text"][
        "text"
    ].replace("09/26/2019 13:42:41", "LOCALEDATE")
    slack_query["blocks"][7]["text"]["text"] = slack_query["blocks"][7]["text"][
        "text"
    ].replace("09/26/2019 01:42:41 PM", "LOCALEDATE")
    expected_slack_query["blocks"][7]["text"]["text"] = expected_slack_query["blocks"][
        7
    ]["text"]["text"].replace("09/26/2019 13:42:41", "LOCALEDATE")
    expected_slack_query["blocks"][7]["text"]["text"] = expected_slack_query["blocks"][
        7
    ]["text"]["text"].replace("09/26/2019 01:42:41 PM", "LOCALEDATE")

    import json

    print(json.dumps(slack_query, indent=2))
    print(json.dumps(expected_slack_query, indent=2))
    assert slack_query == expected_slack_query


def test_errors_warnings_validation_operator_failed_vo_result(
    warning_failure_validation_operator_data_context, assets_to_validate
):
    # this tests whether the WarningAndFailureExpectationSuitesValidationOperator properly returns
    # a failed ValidationOperatorResult if there is a failed validation with a suite severity level of "failure"

    data_context = warning_failure_validation_operator_data_context

    vo = WarningAndFailureExpectationSuitesValidationOperator(
        data_context=data_context,
        action_list=[],
        name="test",
    )

    # only pass asset that yields failed "failure-level" suite and succeeded "warning-level" suite
    return_obj = vo.run(
        assets_to_validate=[assets_to_validate[3]],
        run_id="test_100",
        base_expectation_suite_name="f1",
    )
    run_results = list(return_obj.run_results.values())

    # make sure there is at least one failed validation with a "failure-level" suite
    assert any(
        [
            run_result
            for run_result in run_results
            if run_result["expectation_suite_severity_level"] == "failure"
            and not run_result["validation_result"].success
        ]
    )
    # no failed warning suites
    assert not any(
        [
            run_result
            for run_result in run_results
            if run_result["expectation_suite_severity_level"] == "warning"
            and not run_result["validation_result"].success
        ]
    )
    assert not return_obj.success

    # only pass asset that yields failed "failure-level" suite and failed "warning-level" suite
    return_obj_2 = vo.run(
        assets_to_validate=[assets_to_validate[1]],
        run_id="test_100",
        base_expectation_suite_name="f1",
    )
    run_results_2 = list(return_obj_2.run_results.values())

    # make sure there is at least one failed validation with a "failure-level" suite
    assert any(
        [
            run_result
            for run_result in run_results_2
            if run_result["expectation_suite_severity_level"] == "failure"
            and not run_result["validation_result"].success
        ]
    )
    # with at least one failed warning suite
    assert any(
        [
            run_result
            for run_result in run_results_2
            if run_result["expectation_suite_severity_level"] == "warning"
            and not run_result["validation_result"].success
        ]
    )
    assert not return_obj_2.success


def test_errors_warnings_validation_operator_succeeded_vo_result_with_only_failed_warning_suite(
    warning_failure_validation_operator_data_context, assets_to_validate
):
    # this tests whether the WarningAndFailureExpectationSuitesValidationOperator properly returns
    # a failed ValidationOperatorResult if there is a failed validation with a suite severity level of "failure"

    data_context = warning_failure_validation_operator_data_context

    vo = WarningAndFailureExpectationSuitesValidationOperator(
        data_context=data_context,
        action_list=[],
        name="test",
    )

    # only pass asset that yields succeeded "failure-level" suite and failed "warning-level" suite
    return_obj = vo.run(
        assets_to_validate=[assets_to_validate[0]],
        run_id="test_100",
        base_expectation_suite_name="f1",
    )
    run_results = list(return_obj.run_results.values())

    # make sure there are no failed validations with suite severity of failure
    assert not any(
        [
            run_result
            for run_result in run_results
            if run_result["expectation_suite_severity_level"] == "failure"
            and not run_result["validation_result"].success
        ]
    )
    # make sure there is at least one failed validation with suite severity of warning
    assert any(
        [
            run_result
            for run_result in run_results
            if run_result["expectation_suite_severity_level"] == "warning"
            and not run_result["validation_result"].success
        ]
    )
    assert return_obj.success

    # only pass asset that yields succeeded "failure-level" suite and succeeded "warning-level" suite
    return_obj_2 = vo.run(
        assets_to_validate=[assets_to_validate[2]],
        run_id="test_100",
        base_expectation_suite_name="f1",
    )
    run_results_2 = list(return_obj_2.run_results.values())

    # make sure there are no failed validations with suite severity of failure
    assert not any(
        [
            run_result
            for run_result in run_results_2
            if run_result["expectation_suite_severity_level"] == "failure"
            and not run_result["validation_result"].success
        ]
    )
    # make sure there are no failed validation with suite severity of warning
    assert not any(
        [
            run_result
            for run_result in run_results_2
            if run_result["expectation_suite_severity_level"] == "warning"
            and not run_result["validation_result"].success
        ]
    )
    assert return_obj_2.success
