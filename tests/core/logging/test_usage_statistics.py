import mock
import pytest

from great_expectations.core.logging.usage_statistics import run_validation_operator_usage_statistics
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, \
    DEFAULT_USAGE_STATISTICS_URL
from tests.integration.usage_statistics.test_integration_usage_statistics import USAGE_STATISTICS_QA_URL


@pytest.fixture
def in_memory_data_context_config():
    return DataContextConfig(**{
        "commented_map": {},
        "config_version": 1,
        "plugins_directory": None,
        "evaluation_parameter_store_name": "evaluation_parameter_store",
        "validations_store_name": "validations_store",
        "expectations_store_name": "expectations_store",
        "config_variables_file_path": None,
        "datasources": {},
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
            },
            "validations_store": {
                "class_name": "ValidationsStore",
            },
            "evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore",
            },
        },
        "data_docs_sites": {},
        "validation_operators": {
            "default": {
                "class_name": "ActionListValidationOperator",
                "action_list": []
            }
        },
        "anonymous_usage_statistics": {
            "enabled": True,
            "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
            "usage_statistics_url": USAGE_STATISTICS_QA_URL
        }
    })


def test_consistent_name_anonymization(in_memory_data_context_config):
    context = BaseDataContext(in_memory_data_context_config)
    assert context.data_context_id == "6a52bdfa-e182-455b-a825-e69f076e67d6"
    payload = run_validation_operator_usage_statistics(
        context, "action_list_operator",
        assets_to_validate=[({"__fake_batch_kwargs": "mydatasource"}, "__fake_expectation_suite_name")], run_id="foo")
    assert payload["n_assets"] == 1
    # For a *specific* data_context_id, all names will be consistently anonymized
    assert payload["validation_operator_name"] == '5bb011891aa7d41401e57759d5f5cb01'


# TODO lots more tests like this for various events and payloads
@mock.patch("great_expectations.DataContext.build_data_docs")
@mock.patch("requests.Response")
@mock.patch("requests.Session")
def test_usage_statistics_sent(mock_build_data_docs, mock_response, mock_session, in_memory_data_context_config):
    # TODO this isn't working because messages are being refactored.
    mock_build_data_docs.return_value = {"local_site": "file:///data_docs"}
    mock_session.post.return_value = True
    mock_response.status_code = 200

    context = BaseDataContext(in_memory_data_context_config)
    assert context.anonymous_usage_statistics.enabled == True
    assert mock_session.post.call_count == 1

    context.build_data_docs()
    assert mock_session.post.call_count == 2
    # TODO assertions about payloads something like...
    assert mock_session.post.assert_has_calls(
        [
            mock_session.post(DEFAULT_USAGE_STATISTICS_URL, "init payload"),
            mock_session.post(DEFAULT_USAGE_STATISTICS_URL, "data docs payload"),
        ]
    )


# Test opt outs
# TODO parameterize this test for 3-4 types of False's
def test_opt_out_environment_variable(in_memory_data_context_config):
    """Set the env variable GE_USAGE_STATS value to any of the following: FALSE, False, false, 0"""
    assert False


# TODO parameterize this test for 3-4 types of False's
def test_opt_out_etc(in_memory_data_context_config):
    """Create /etc/great_expectations.conf file and set the ‘enabled’ option in the [anonymous_usage_statistics] section to any of the following: FALSE, False, false, 0"""
    assert False


# TODO parameterize this test for 3-4 types of False's
def test_opt_out_home_folder(in_memory_data_context_config):
    """Create ~/.great_expectations/great_expectations.conf file and set the ‘enabled’ option in the [anonymous_usage_statistics] section to any of the following: FALSE, False, false, 0"""
    assert False


# TODO parameterize this test for 3-4 types of False's
@mock.patch("DataContext.build_data_docs")
def test_opt_out_yml(in_memory_data_context_config):
    """
    The feature is turned on for a particular Data Context when the property
    anonymous_usage_statistics.enabled in the DataContextConfig file is:
    - true (the default).
"""
    assert False


# Test precedence: environment variable > /etc > home folder > yml
def test_opt_out_env_var_overrides_home_folder(in_memory_data_context_config):
    assert False


def test_opt_out_env_var_overrides_etc(in_memory_data_context_config):
    assert False


def test_opt_out_env_var_overrides_yml(in_memory_data_context_config):
    assert False


def test_opt_out_home_folder_overrides_etc(in_memory_data_context_config):
    assert False


def test_opt_out_home_folder_overrides_yml(in_memory_data_context_config):
    assert False


def test_opt_out_etc_overrides_yml(in_memory_data_context_config):
    assert False
