from typing import Dict, List, Optional, Tuple, Union

import pytest

from great_expectations.core import ExpectationSuite, ExpectationSuiteValidationResult
from great_expectations.data_context.data_context_variables import (
    DataContextVariables,
    EphemeralDataContextVariables,
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


class StubConfigurationProvider:
    def substitute_config(self, config):
        return config


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
        return EphemeralDataContextVariables(
            config=config, config_provider=StubConfigurationProvider()
        )

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


@pytest.fixture
def empty_serialized_configuration_bundle() -> dict:
    return {
        "data_context_id": "27517569-1500-4127-af68-b5bad960a492",
        "checkpoints": [],
        "data_context_variables": {
            "config_variables_file_path": None,
            "config_version": 3.0,
            "data_docs_sites": None,
            "evaluation_parameter_store_name": None,
            "expectations_store_name": None,
            "include_rendered_content": {
                "expectation_suite": False,
                "expectation_validation_result": False,
                "globally": False,
            },
            "notebooks": None,
            "plugins_directory": None,
            "stores": None,
            "validations_store_name": None,
        },
        "datasources": [],
        "expectation_suites": [],
        "profilers": [],
        "validation_results": {},
    }


@pytest.fixture
def serialized_configuration_bundle() -> dict:
    return {
        "data_context_id": "877166bd-08f2-4d7b-b473-a2b97ab5e36f",
        "checkpoints": [
            {
                "class_name": "Checkpoint",
                "config_version": None,
                "module_name": "great_expectations.checkpoint",
                "name": "my_checkpoint",
            }
        ],
        "data_context_variables": {
            "config_variables_file_path": None,
            "config_version": 3.0,
            "data_docs_sites": None,
            "evaluation_parameter_store_name": None,
            "expectations_store_name": None,
            "include_rendered_content": {
                "expectation_suite": False,
                "expectation_validation_result": False,
                "globally": False,
            },
            "notebooks": None,
            "plugins_directory": None,
            "stores": None,
            "validations_store_name": None,
        },
        "datasources": [
            {
                "class_name": "Datasource",
                "data_connectors": {},
                "execution_engine": {
                    "class_name": "PandasExecutionEngine",
                    "module_name": "great_expectations.execution_engine",
                },
                "module_name": "great_expectations.datasource",
                "name": "my_datasource",
            }
        ],
        "expectation_suites": [
            {
                "data_asset_type": None,
                "expectation_suite_name": "my_suite",
                "expectations": [],
                "ge_cloud_id": None,
            }
        ],
        "profilers": [
            {
                "class_name": "RuleBasedProfiler",
                "config_version": 1.0,
                "module_name": "great_expectations.rule_based_profiler",
                "name": "my_profiler",
                "rules": {},
                "variables": {},
            }
        ],
        "validation_results": {
            "some_key": {
                "evaluation_parameters": {},
                "meta": {},
                "results": [],
                "statistics": {},
                "success": True,
            }
        },
    }


@pytest.fixture
def stub_serialized_configuration_bundle(
    serialized_configuration_bundle: dict,
) -> dict:
    """Configuration bundle based on StubBaseDataContext."""
    assert "data_context_id" in serialized_configuration_bundle
    serialized_configuration_bundle[
        "data_context_id"
    ] = StubBaseDataContext.DATA_CONTEXT_ID
    return serialized_configuration_bundle
