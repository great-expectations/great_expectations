from typing import Callable, Dict, List, Optional, Union

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
    @property
    def anonymous_usage_statistics(self) -> AnonymizedUsageStatisticsConfig:
        return AnonymizedUsageStatisticsConfig(enabled=True)


class StubCheckpointStore:
    def get_checkpoint(self, name: str, ge_cloud_id: Optional[str]) -> CheckpointConfig:
        return CheckpointConfig(name=name, class_name="Checkpoint")


class StubValidationsStore:
    def list_keys(self):
        # Note: Key just has to return an iterable here
        return ["some_key"]

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
        anonymous_usage_stats_enabled: bool = True,
        anonymous_usage_stats_is_none: bool = False,
        include_datasources: bool = True,
    ):
        """Set the anonymous usage statistics configuration.

        Args:
            anonymous_usage_stats_enabled: Set usage stats "enabled" flag in config.
            anonymous_usage_stats_is_none: Set usage stats to None, overrides anonymous_usage_stats_enabled.
        """
        self._anonymous_usage_stats_enabled = anonymous_usage_stats_enabled
        self._anonymous_usage_stats_is_none = anonymous_usage_stats_is_none
        self._include_datasources = include_datasources

    @property
    def _data_context_variables(self) -> StubUsageStats:
        return StubUsageStats()

    @property
    def anonymous_usage_statistics(self) -> AnonymizedUsageStatisticsConfig:
        return self.variables.anonymous_usage_statistics

    @property
    def data_context_id(self) -> str:
        return self.DATA_CONTEXT_ID

    @property
    def variables(self) -> DataContextVariables:

        # anonymous_usage_statistics set based on constructor parameters.
        anonymous_usage_statistics: Optional[AnonymizedUsageStatisticsConfig]
        if self._anonymous_usage_stats_is_none:
            anonymous_usage_statistics = None
        else:
            anonymous_usage_statistics = AnonymizedUsageStatisticsConfig(
                enabled=self._anonymous_usage_stats_enabled
            )

        config = DataContextConfig(
            anonymous_usage_statistics=anonymous_usage_statistics
        )
        return EphemeralDataContextVariables(config=config)

    @property
    def _datasource_store(self):
        return StubDatasourceStore()

    @property
    def datasources(self) -> Dict[str, Union[LegacyDatasource, BaseDatasource]]:
        # Datasource is a dummy since we just want the DatasourceConfig from the store, not an
        # actual initialized datasource.
        if self._include_datasources:
            return {"my_datasource": DummyDatasource()}
        else:
            return {}

    @property
    def checkpoint_store(self) -> StubCheckpointStore:
        return StubCheckpointStore()

    @property
    def validations_store(self) -> StubValidationsStore:
        return StubValidationsStore()

    def list_expectation_suite_names(self) -> List[str]:
        return ["my_suite"]

    def get_expectation_suite(self, name: str) -> ExpectationSuite:
        return ExpectationSuite(expectation_suite_name=name)

    def list_checkpoints(self) -> List[str]:
        return ["my_checkpoint"]

    def list_profilers(self) -> List[str]:
        return ["my_profiler"]

    def get_profiler(self, name: str) -> RuleBasedProfiler:
        return RuleBasedProfiler(name, config_version=1.0, rules={})


@pytest.fixture
def stub_base_data_context_factory() -> Callable:
    def _create_stub_base_data_context(
        anonymous_usage_stats_enabled: bool = True,
        anonymous_usage_stats_is_none: bool = False,
        include_datasources: bool = True,
    ) -> StubBaseDataContext:
        return StubBaseDataContext(
            anonymous_usage_stats_enabled=anonymous_usage_stats_enabled,
            anonymous_usage_stats_is_none=anonymous_usage_stats_is_none,
            include_datasources=include_datasources,
        )

    return _create_stub_base_data_context


@pytest.fixture
def stub_base_data_context(
    stub_base_data_context_factory: Callable,
) -> StubBaseDataContext:
    return stub_base_data_context_factory()


@pytest.fixture
def stub_base_data_context_anonymous_usage_stats_present_but_disabled(
    stub_base_data_context_factory: Callable,
) -> StubBaseDataContext:
    return stub_base_data_context_factory(anonymous_usage_stats_enabled=False)


@pytest.fixture
def stub_base_data_context_no_anonymous_usage_stats(
    stub_base_data_context_factory: Callable,
) -> StubBaseDataContext:
    return stub_base_data_context_factory(anonymous_usage_stats_is_none=True)


@pytest.fixture
def serialized_configuration_bundle() -> dict:
    return {
        "data_context_id": "877166bd-08f2-4d7b-b473-a2b97ab5e36f",
        "datasources": [
            {
                "class_name": "Datasource",
                "data_connectors": {
                    "my_default_data_connector": {
                        "assets": {
                            "": {
                                "base_directory": "data",
                                "class_name": "Asset",
                                "glob_directive": "*.csv",
                                "group_names": ["batch_num", "total_batches"],
                                "pattern": "csv_batch_(\\d.+)_of_(\\d.+)\\.csv",
                            }
                        },
                        "base_directory": "data",
                        "class_name": "ConfiguredAssetFilesystemDataConnector",
                    }
                },
                "execution_engine": {
                    "class_name": "PandasExecutionEngine",
                    "module_name": "great_expectations.execution_engine",
                },
                "name": "generic_csv_generator",
            }
        ],
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
def stub_serialized_configuration_bundle(serialized_configuration_bundle: dict) -> dict:
    """Configuration bundle based on StubBaseDataContext."""
    assert "data_context_id" in serialized_configuration_bundle
    serialized_configuration_bundle[
        "data_context_id"
    ] = StubBaseDataContext.DATA_CONTEXT_ID
    return serialized_configuration_bundle
