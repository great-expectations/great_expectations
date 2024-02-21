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
    CHECKPOINTS_STORE_NAME = (
        DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_NAME.value
    )
    EVALUATION_PARAMETERS_STORE_NAME = (
        DataContextConfigDefaults.DEFAULT_EVALUATION_PARAMETER_STORE_NAME.value
    )
    EXPECTATION_SUITES_STORE_NAME = (
        DataContextConfigDefaults.DEFAULT_EXPECTATIONS_STORE_NAME.value
    )
    PROFILERS_STORE_NAME = DataContextConfigDefaults.DEFAULT_PROFILER_STORE_NAME.value
    VALIDATIONS_STORE_NAME = (
        DataContextConfigDefaults.DEFAULT_VALIDATIONS_STORE_NAME.value
    )

    def __init__(
        self,
        store_configs: dict,
        root_directory: str,
    ):
        self._root_directory = root_directory

        self._checkpoints = self._build_store_from_config(
            store_name=self.CHECKPOINTS_STORE_NAME,
            config=store_configs,
        )
        self._evaluation_parameters = self._build_store_from_config(
            store_name=self.EVALUATION_PARAMETERS_STORE_NAME,
            config=store_configs,
        )
        self._expectaton_suites = self._build_store_from_config(
            store_name=self.EXPECTATION_SUITES_STORE_NAME,
            config=store_configs,
        )
        self._profilers = self._build_store_from_config(
            store_name=self.PROFILERS_STORE_NAME,
            config=store_configs,
        )
        self._validations = self._build_store_from_config(
            store_name=self.VALIDATIONS_STORE_NAME,
            config=store_configs,
        )

    def __getitem__(self, key: str) -> Store:
        data = self.dict()
        if key not in data:
            raise ValueError(
                f"Store '{key}' not found; must be one of {list(data.keys())}"
            )

        return data[key]

    def dict(self) -> dict[str, Store]:
        return {
            self.CHECKPOINTS_STORE_NAME: self._checkpoints,
            self.EVALUATION_PARAMETERS_STORE_NAME: self._evaluation_parameters,
            self.EXPECTATION_SUITES_STORE_NAME: self._expectaton_suites,
            self.PROFILERS_STORE_NAME: self._profilers,
            self.VALIDATIONS_STORE_NAME: self._validations,
        }

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

    def _update_store(self, name: str, store: Store | dict) -> Store:
        if isinstance(store, dict):
            store = self._build_store_from_config(store_name=name, config=store)
        return store

    @property
    def checkpoints(self) -> CheckpointStore:
        return self._checkpoints

    @checkpoints.setter
    def checkpoints(self, store: CheckpointStore | dict) -> None:
        self._checkpoints = self._update_store(
            name=self.CHECKPOINTS_STORE_NAME, store=store
        )

    @property
    def evaluation_parameters(self) -> EvaluationParameterStore:
        return self._evaluation_parameters

    @evaluation_parameters.setter
    def evaluation_parameters(self, store: EvaluationParameterStore | dict) -> None:
        self._evaluation_parameters = self._update_store(
            name=self.EVALUATION_PARAMETERS_STORE_NAME, store=store
        )

    @property
    def expectation_suites(self) -> ExpectationsStore:
        return self._expectaton_suites

    @expectation_suites.setter
    def expectation_suites(self, store: ExpectationsStore | dict) -> None:
        self._expectaton_suites = self._update_store(
            name=self.EXPECTATION_SUITES_STORE_NAME, store=store
        )

    @property
    def profilers(self) -> ProfilerStore:
        return self._profilers

    @profilers.setter
    def profilers(self, store: ProfilerStore | dict) -> None:
        self._profilers = self._update_store(
            name=self.EXPECTATION_SUITES_STORE_NAME, store=store
        )

    @property
    def validations(self) -> ValidationsStore:
        return self._validations

    @validations.setter
    def validations(self, store: ValidationsStore | dict) -> None:
        self._validations = self._update_store(
            name=self.VALIDATIONS_STORE_NAME, store=store
        )
