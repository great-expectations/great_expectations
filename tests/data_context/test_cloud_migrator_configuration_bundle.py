"""TODO: Add docstring"""
import pytest
from great_expectations.data_context.types.base import DataContextConfig

from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.cloud_migrator import ConfigurationBundle


@pytest.mark.integration
def test_configuration_bundle_init(in_memory_runtime_context: BaseDataContext):
    """What does this test and why?

    This test is an integration test using a real DataContext to prove out the
    bundling. We can replace this test with unit tests.
    """

    config_bundle = ConfigurationBundle(in_memory_runtime_context)

    assert not config_bundle.is_usage_statistics_key_set(in_memory_runtime_context)

    assert config_bundle._data_context_config.checkpoint_store_name == "checkpoint_store"

    assert config_bundle._expectations == []
    assert config_bundle._checkpoints == []
    assert config_bundle._profilers == []
    assert config_bundle._validation_results == []



