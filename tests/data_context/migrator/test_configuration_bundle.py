"""These tests exercise ConfigurationBundle including Serialization."""
import pytest

from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.migrator.configuration_bundle import (
    ConfigurationBundle,
    ConfigurationBundleJsonSerializer,
    ConfigurationBundleSchema,
)
from tests.data_context.migrator.conftest import StubBaseDataContext


@pytest.mark.cloud
@pytest.mark.unit
class TestConfigurationBundleCreate:
    def test_configuration_bundle_created(
        self,
        stub_base_data_context: StubBaseDataContext,
    ):
        """What does this test and why?

        Make sure the configuration bundle is created successfully from a data context.
        """

        context: BaseDataContext = stub_base_data_context

        config_bundle = ConfigurationBundle(context)

        assert config_bundle.is_usage_stats_enabled()
        assert config_bundle._data_context_variables is not None
        assert len(config_bundle.expectation_suites) == 1
        assert len(config_bundle.checkpoints) == 1
        assert len(config_bundle.profilers) == 1
        assert len(config_bundle.validation_results) == 1
        assert len(config_bundle.datasources) == 1

    def test_is_usage_statistics_key_set_if_key_not_present(
        self, stub_base_data_context_no_anonymous_usage_stats: StubBaseDataContext
    ):
        """What does this test and why?

        The ConfigurationBundle should handle a context that has not set the config for
         anonymous_usage_statistics.
        """
        context: BaseDataContext = stub_base_data_context_no_anonymous_usage_stats

        config_bundle = ConfigurationBundle(context)

        # If not supplied, an AnonymizedUsageStatisticsConfig is created in a
        # DataContextConfig and enabled by default.
        assert config_bundle.is_usage_stats_enabled()

    def test_configuration_bundle_created_usage_stats_disabled(
        self,
        stub_base_data_context_anonymous_usage_stats_present_but_disabled: StubBaseDataContext,
    ):
        """What does this test and why?

        Make sure the configuration bundle successfully parses the usage stats settings.
        """

        context: BaseDataContext = (
            stub_base_data_context_anonymous_usage_stats_present_but_disabled
        )

        config_bundle = ConfigurationBundle(context)

        assert not config_bundle.is_usage_stats_enabled()


@pytest.mark.cloud
@pytest.mark.unit
class TestConfigurationBundleSerialization:
    def test_configuration_bundle_serialization_all_fields(
        self,
        stub_base_data_context: StubBaseDataContext,
        stub_serialized_configuration_bundle: dict,
    ):
        """What does this test and why?

        Ensure configuration bundle is serialized correctly.
        """

        context: BaseDataContext = stub_base_data_context

        config_bundle = ConfigurationBundle(context)

        serializer = ConfigurationBundleJsonSerializer(
            schema=ConfigurationBundleSchema()
        )

        serialized_bundle: dict = serializer.serialize(config_bundle)

        expected_serialized_bundle = stub_serialized_configuration_bundle

        # Remove meta before comparing since it contains the GX version
        serialized_bundle["expectation_suites"][0].pop("meta", None)
        expected_serialized_bundle["expectation_suites"][0].pop("meta", None)

        assert serialized_bundle == expected_serialized_bundle

    def test_configuration_bundle_serialization_empty_fields(
        self,
        empty_serialized_configuration_bundle: dict,
    ):
        """What does this test and why?

        Ensure configuration bundle is serialized correctly.
        """

        context = StubBaseDataContext(
            checkpoint_names=tuple(),
            expectation_suite_names=tuple(),
            profiler_names=tuple(),
            validation_results_keys=tuple(),
            datasource_names=tuple(),
        )

        config_bundle = ConfigurationBundle(context)

        serializer = ConfigurationBundleJsonSerializer(
            schema=ConfigurationBundleSchema()
        )

        serialized_bundle: dict = serializer.serialize(config_bundle)

        assert serialized_bundle == empty_serialized_configuration_bundle

    def test_anonymous_usage_statistics_removed_during_serialization(
        self,
        stub_base_data_context: StubBaseDataContext,
    ):
        """What does this test and why?
        When serializing a ConfigurationBundle we need to remove the
        anonymous_usage_statistics key.
        """

        context: StubBaseDataContext = stub_base_data_context

        assert context.anonymous_usage_statistics is not None

        config_bundle = ConfigurationBundle(context)

        serializer = ConfigurationBundleJsonSerializer(
            schema=ConfigurationBundleSchema()
        )

        serialized_bundle: dict = serializer.serialize(config_bundle)

        assert (
            serialized_bundle["data_context_variables"].get(
                "anonymous_usage_statistics"
            )
            is None
        )
