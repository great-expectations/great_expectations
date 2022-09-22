"""TODO: Add docstring"""
import pytest
from great_expectations.checkpoint import Checkpoint

from great_expectations.core import ExpectationSuite

from great_expectations.data_context.types.base import DataContextConfig

from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.cloud_migrator import ConfigurationBundle


@pytest.mark.integration
def test_configuration_bundle_init(in_memory_runtime_context: BaseDataContext):
    """What does this test and why?

    This test is an integration test using a real DataContext to prove out the
    bundling. We can replace this test with unit tests.
    """

    expectation_suite: ExpectationSuite = ExpectationSuite(expectation_suite_name="my_suite")
    in_memory_runtime_context.save_expectation_suite(expectation_suite)
    # checkpoint: Checkpoint = in_memory_runtime_context.add_checkpoint(name="my_checkpoint")

    config_bundle = ConfigurationBundle(in_memory_runtime_context)

    assert not config_bundle.is_usage_statistics_key_set(in_memory_runtime_context)

    assert config_bundle._data_context_config.checkpoint_store_name == "checkpoint_store"


    assert config_bundle._expectation_suites == [expectation_suite]
    # TODO: Add these assets:
    # assert config_bundle._checkpoints == [checkpoint.config]
    assert config_bundle._checkpoints == []
    assert config_bundle._profilers == []
    assert config_bundle._validation_results == []



@pytest.mark.integration
def test_configuration_bundle_serialization(in_memory_runtime_context: BaseDataContext):
    """What does this test and why?

    This test is an integration test using a real DataContext to prove out the
    ConfigurationBundle serialization. We can replace this test with unit tests.
    """

    raise NotImplementedError


