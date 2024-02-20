from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations.data_context.store.store import Store
from great_expectations.data_context.types.base import DataContextConfigDefaults

if TYPE_CHECKING:
    from great_expectations.data_context.store import (
        CheckpointStore,
        EvaluationParameterStore,
        ExpectationsStore,
        ProfilerStore,
        ValidationsStore,
    )


class StoreManager:
    def __init__(
        self,
        raw_config: dict,
        root_directory: str,
    ):
        self._root_directory = root_directory
        self._checkpoints = self._build_store_from_config(
            store_name=DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_NAME.value,
            config=raw_config,
        )
        self._evaluation_parameters = self._build_store_from_config(
            store_name=DataContextConfigDefaults.DEFAULT_EVALUATION_PARAMETER_STORE_NAME.value,
            config=raw_config,
        )
        self._expectation_suites = self._build_store_from_config(
            store_name=DataContextConfigDefaults.DEFAULT_EXPECTATIONS_STORE_NAME.value,
            config=raw_config,
        )
        self._profilers = self._build_store_from_config(
            store_name=DataContextConfigDefaults.DEFAULT_PROFILER_STORE_NAME.value,
            config=raw_config,
        )
        self._validations = self._build_store_from_config(
            store_name=DataContextConfigDefaults.DEFAULT_VALIDATIONS_STORE_NAME.value,
            config=raw_config,
        )

    @property
    def checkpoints(self) -> CheckpointStore:
        return self._checkpoints

    @property
    def evaluation_parameters(self) -> EvaluationParameterStore:
        return self._evaluation_parameters

    @property
    def expectation_suites(self) -> ExpectationsStore:
        return self._expectation_suites

    @property
    def profilers(self) -> ProfilerStore:
        return self._profilers

    @property
    def validations(self) -> ValidationsStore:
        return self._validations

    def _build_store_from_config(
        self,
        store_name: str,
        config: dict,
    ) -> Store:
        store_config = config.get(
            store_name, DataContextConfigDefaults.DEFAULT_STORES.value[store_name]
        )
        return Store.build_store_from_config(
            store_name=store_name,
            store_config=store_config,
            module_name="great_expectations.data_context.store",
            runtime_environment={
                "root_directory": self._root_directory,
            },
        )
