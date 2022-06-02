import enum
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

from great_expectations.data_context.types.base import DataContextConfig


class VariableSchema(enum.Enum):
    CONFIG_VERSION = "config_version"
    DATASOURCES = "datasources"
    EXPECTATIONS_STORE_NAME = "expectations_store_name"
    VALIDATIONS_STORE_NAME = "validations_store_name"
    EVALUATION_PARAMETER_STORE_NAME = "evaluation_parameter_store_name"
    CHECKPOINT_STORE_NAME = "checkpoint_store_name"
    PROFILER_STORE_NAME = "profiler_store_name"

    # plugins_directory: Optional[str]
    # validation_operators: Optional[dict]
    # stores: Optional[dict]
    # data_docs_sites: Optional[dict]
    # notebooks: Optional[dict]
    # config_variables_file_path: Optional[str]
    # anonymous_usage_statistics: Optional[dict]
    # store_backend_defaults: Optional[dict]
    # concurrency: Optional[dict]
    # progress_bars: Optional[dict]


@dataclass
class DataContextVariables:
    config_version: Optional[float]
    datasources: Optional[dict]
    expectations_store_name: Optional[str]
    validations_store_name: Optional[str]
    evaluation_parameter_store_name: Optional[str]
    checkpoint_store_name: Optional[str]
    profiler_store_name: Optional[str]
    plugins_directory: Optional[str]
    validation_operators: Optional[dict]
    stores: Optional[dict]
    data_docs_sites: Optional[dict]
    notebooks: Optional[dict]
    config_variables_file_path: Optional[str]
    anonymous_usage_statistics: Optional[dict]
    store_backend_defaults: Optional[dict]
    concurrency: Optional[dict]
    progress_bars: Optional[dict]

    @abstractmethod
    def _persist_change(self, attr: VariableSchema) -> None:
        raise NotImplementedError

    def _set(self, attr: VariableSchema, value: Any) -> None:
        setattr(self, attr.value, value)
        self._persist_change(attr)

    def _get(self, attr: VariableSchema) -> Any:
        res: Any = getattr(self, attr.value)
        self._persist_change(attr)
        return res

    def set_config_version(self, config_version: float) -> None:
        self._set(VariableSchema.CONFIG_VERSION, config_version)

    def get_config_version(self) -> Optional[float]:
        return self._get(VariableSchema.CONFIG_VERSION)


@dataclass
class EphemeralDataContextVariables(DataContextVariables):
    def _persist_change(self) -> None:
        pass  # Changes are only made in memory so no side effects need to occur


@dataclass
class FileDataContextVariables(DataContextVariables):
    project_config: DataContextConfig

    def _persist_change(self) -> None:
        raise NotImplementedError


@dataclass
class CloudDataContextVariables(DataContextVariables):
    def _persist_change(self) -> None:
        raise NotImplementedError
