"""These tests exercise ConfigurationBundle including Serialization."""
from typing import Dict, List, Optional, Tuple, Union

import pytest

from great_expectations.core import ExpectationSuite, ExpectationSuiteValidationResult
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.data_context_variables import (
    DataContextVariables,
    EphemeralDataContextVariables,
)
from great_expectations.data_context.migrator.configuration_bundle import (
    ConfigurationBundle,
    ConfigurationBundleJsonSerializer,
    ConfigurationBundleSchema,
)
from great_expectations.data_context.types.base import (
    AnonymizedUsageStatisticsConfig,
    CheckpointConfig,
    DataContextConfig,
    DatasourceConfig,
)
from great_expectations.datasource import BaseDatasource, LegacyDatasource
from great_expectations.rule_based_profiler import RuleBasedProfiler


class StubUsageStats:
    def __init__(
        self, anonymized_usage_statistics_config: AnonymizedUsageStatisticsConfig
    ):
        self._anonymized_usage_statistics_config = anonymized_usage_statistics_config

    @property
    def anonymous_usage_statistics(self) -> AnonymizedUsageStatisticsConfig:
        return self._anonymized_usage_statistics_config


class StubCheckpointStore:
    def get_checkpoint(self, name: str, ge_cloud_id: Optional[str]) -> CheckpointConfig:
        return CheckpointConfig(name=name, class_name="Checkpoint")


class StubValidationsStore:
    def __init__(self, keys: Tuple[Optional[str]] = ("some_key",)):
        self._keys = keys

    def list_keys(self):
        # Note: Key just has to return an iterable here
        return list(self._keys)

    def get(self, key):
        # Note: Key is unused
        return ExpectationSuiteValidationResult(
            success=True,
        )


class StubDatasourceStore:
    def retrieve_by_name(self, datasource_name: str) -> DatasourceConfig:
        datasource_config_dict: dict = {
            "name": datasource_name,
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": "PandasExecutionEngine",
            },
            "data_connectors": {},
        }
        return DatasourceConfig(**datasource_config_dict)


class DummyDatasource:
    pass


class StubBaseDataContext:
    """Stub for testing ConfigurationBundle."""

    DATA_CONTEXT_ID = "27517569-1500-4127-af68-b5bad960a492"

    def __init__(
        self,
        anonymized_usage_statistics_config: Optional[
            AnonymizedUsageStatisticsConfig
        ] = AnonymizedUsageStatisticsConfig(enabled=True),
        checkpoint_names: Tuple[Optional[str]] = ("my_checkpoint",),
        expectation_suite_names: Tuple[Optional[str]] = ("my_suite",),
        profiler_names: Tuple[Optional[str]] = ("my_profiler",),
        validation_results_keys: Tuple[Optional[str]] = ("some_key",),
        datasource_names: Tuple[Optional[str]] = ("my_datasource",),
    ):
        """Set the configuration of the stub data context.

        Args:
            anonymized_usage_statistics_config: Config to use for anonymous usage statistics
        """
        self._anonymized_usage_statistics_config = anonymized_usage_statistics_config
        self._checkpoint_names = checkpoint_names
        self._expectation_suite_names = expectation_suite_names
        self._profiler_names = profiler_names
        self._validation_results_keys = validation_results_keys
        self._datasource_names = datasource_names

    @property
    def _data_context_variables(self) -> StubUsageStats:
        return StubUsageStats(
            anonymized_usage_statistics_config=self._anonymized_usage_statistics_config
        )

    @property
    def anonymous_usage_statistics(self) -> AnonymizedUsageStatisticsConfig:
        return self.variables.anonymous_usage_statistics

    @property
    def data_context_id(self) -> str:
        return self.DATA_CONTEXT_ID

    @property
    def variables(self) -> DataContextVariables:

        config = DataContextConfig(
            anonymous_usage_statistics=self._anonymized_usage_statistics_config
        )
        return EphemeralDataContextVariables(config=config)

    @property
    def _datasource_store(self):
        return StubDatasourceStore()

    @property
    def datasources(self) -> Dict[str, Union[LegacyDatasource, BaseDatasource]]:
        # Datasource is a dummy since we just want the DatasourceConfig from the store, not an
        # actual initialized datasource.
        return {
            datasource_name: DummyDatasource()
            for datasource_name in self._datasource_names
        }

    @property
    def checkpoint_store(self) -> StubCheckpointStore:
        return StubCheckpointStore()

    @property
    def validations_store(self) -> StubValidationsStore:
        return StubValidationsStore(keys=self._validation_results_keys)

    def list_expectation_suite_names(self) -> List[str]:
        return list(self._expectation_suite_names)

    def get_expectation_suite(self, name: str) -> ExpectationSuite:
        return ExpectationSuite(expectation_suite_name=name)

    def list_checkpoints(self) -> List[str]:
        return list(self._checkpoint_names)

    def list_profilers(self) -> List[str]:
        return list(self._profiler_names)

    def get_profiler(self, name: str) -> RuleBasedProfiler:
        return RuleBasedProfiler(name, config_version=1.0, rules={})


@pytest.fixture
def stub_base_data_context() -> StubBaseDataContext:
    return StubBaseDataContext(
        anonymized_usage_statistics_config=AnonymizedUsageStatisticsConfig(enabled=True)
    )


@pytest.fixture
def stub_base_data_context_anonymous_usage_stats_present_but_disabled() -> StubBaseDataContext:
    return StubBaseDataContext(
        anonymized_usage_statistics_config=AnonymizedUsageStatisticsConfig(
            enabled=False
        )
    )


@pytest.fixture
def stub_base_data_context_no_anonymous_usage_stats() -> StubBaseDataContext:
    return StubBaseDataContext(anonymized_usage_statistics_config=None)


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


@pytest.fixture
def stub_serialized_configuration_bundle(serialized_configuration_bundle: dict) -> dict:
    """Configuration bundle based on StubBaseDataContext."""
    assert "data_context_id" in serialized_configuration_bundle
    serialized_configuration_bundle[
        "data_context_id"
    ] = StubBaseDataContext.DATA_CONTEXT_ID
    return serialized_configuration_bundle


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
