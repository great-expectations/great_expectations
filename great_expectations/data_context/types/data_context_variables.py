import enum
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

import requests


class VariableSchema(enum.Enum):
    CONFIG_VERSION = "config_version"
    DATASOURCES = "datasources"
    EXPECTATIONS_STORE_NAME = "expectations_store_name"
    VALIDATIONS_STORE_NAME = "validations_store_name"
    EVALUATION_PARAMETER_STORE_NAME = "evaluation_parameter_store_name"
    CHECKPOINT_STORE_NAME = "checkpoint_store_name"
    PROFILER_STORE_NAME = "profiler_store_name"
    PLUGINS_DIRECTORY = "plugins_directory"
    VALIDATION_OPERATORS = "validation_operators"
    STORES = "stores"
    DATA_DOCS_SITES = "data_docs_sites"
    NOTEBOOKS = "notebooks"
    CONFIG_VARIABLES_FILE_PATH = "config_variables_file_path"
    ANONYMIZED_USAGE_STATISTICS = "anonymous_usage_statistics"
    STORE_BACKEND_DEFAULTS = "store_backend_defaults"
    CONCURRENCY = "concurrency"
    PROGRESS_BARS = "progress_bars"


@dataclass
class DataContextVariables:
    config_version: Optional[float] = None
    datasources: Optional[dict] = None
    expectations_store_name: Optional[str] = None
    validations_store_name: Optional[str] = None
    evaluation_parameter_store_name: Optional[str] = None
    checkpoint_store_name: Optional[str] = None
    profiler_store_name: Optional[str] = None
    plugins_directory: Optional[str] = None
    validation_operators: Optional[dict] = None
    stores: Optional[dict] = None
    data_docs_sites: Optional[dict] = None
    notebooks: Optional[dict] = None
    config_variables_file_path: Optional[str] = None
    anonymous_usage_statistics: Optional[dict] = None
    store_backend_defaults: Optional[dict] = None
    concurrency: Optional[dict] = None
    progress_bars: Optional[dict] = None

    @abstractmethod
    def _write_to_backend(self, attr: VariableSchema, value: Any) -> None:
        raise NotImplementedError

    @abstractmethod
    def _read_from_backend(self, attr: VariableSchema) -> Any:
        raise NotImplementedError

    def _set(self, attr: VariableSchema, value: Any) -> None:
        setattr(self, attr.value, value)
        self._write_to_backend(attr, value)

    def _get(self, attr: VariableSchema) -> Any:
        val: Any = self._read_from_backend(attr)
        return val

    def set_config_version(self, config_version: float) -> None:
        self._set(VariableSchema.CONFIG_VERSION, config_version)

    def get_config_version(self) -> Optional[float]:
        return self._get(VariableSchema.CONFIG_VERSION)


@dataclass
class EphemeralDataContextVariables(DataContextVariables):
    def _write_to_backend(self, attr: VariableSchema, value: Any) -> None:
        pass  # Changes are only made in memory so no side effects need to occur

    def _read_from_backend(self, attr: VariableSchema) -> Any:
        val: Any = getattr(self, attr.value)
        return val


@dataclass
class FileDataContextVariables(DataContextVariables):
    def __post_init__(self, base_data_context: "BaseDataContext") -> None:  # noqa: F821
        self.base_data_context = base_data_context

    def _write_to_backend(self, attr: VariableSchema, value: Any) -> None:
        self.base_data_context.save_config_variable(
            config_variable_name=attr.value, value=value
        )

    def _read_from_backend(self, attr: VariableSchema) -> Any:
        val: Any = getattr(
            self.base_data_context.project_config_with_variables_substituted, attr.value
        )
        return val


@dataclass
class CloudDataContextVariables(DataContextVariables):
    def __post_init__(self, base_url: str) -> None:
        self.base_url = base_url

    def _write_to_backend(self, attr: VariableSchema, value: Any) -> None:
        endpoint: str = f"{self.base_url}/{attr.value}"
        requests.put(endpoint)

    def _read_from_backend(self, attr: VariableSchema) -> Any:
        endpoint: str = f"{self.base_url}/{attr.value}"
        response = requests.get(endpoint)
        return response
