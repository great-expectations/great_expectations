from __future__ import annotations

import importlib
import itertools
import json
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from marshmallow import ValidationError

if TYPE_CHECKING:
    import requests


class GreatExpectationsError(Exception):
    def __init__(self, message) -> None:
        self.message = message
        super().__init__(message)


class GreatExpectationsValidationError(ValidationError, GreatExpectationsError):
    def __init__(self, message, validation_error=None) -> None:
        self.message = message
        self.messages: Union[List[str], List[Any], Dict] = []
        if validation_error is not None:
            self.messages = validation_error.messages

    def __str__(self) -> str:
        if self.message is None:
            return str(self.messages)
        return self.message


class SuiteEditNotebookCustomTemplateModuleNotFoundError(ModuleNotFoundError):
    def __init__(self, custom_module) -> None:
        message = f"The custom module '{custom_module}' could not be found"
        super().__init__(message)


class DataContextError(GreatExpectationsError):
    pass


class ExpectationSuiteError(DataContextError):
    pass


class CheckpointError(DataContextError):
    pass


class CheckpointNotFoundError(CheckpointError):
    pass


class StoreBackendError(DataContextError):
    pass


class StoreBackendTransientError(StoreBackendError):
    """The result of a timeout or other networking issues"""

    pass


class ParserError(GreatExpectationsError):
    pass


class InvalidConfigurationYamlError(DataContextError):
    pass


class InvalidTopLevelConfigKeyError(GreatExpectationsError):
    pass


class MissingTopLevelConfigKeyError(GreatExpectationsValidationError):
    pass


class RenderingError(GreatExpectationsError):
    pass


class InvalidBaseYamlConfigError(GreatExpectationsValidationError):
    def __init__(self, message, validation_error=None, field_name=None) -> None:
        if validation_error is not None:
            if (
                validation_error
                and validation_error.messages
                and isinstance(validation_error.messages, dict)
                and all(key is None for key in validation_error.messages.keys())
            ):
                validation_error.messages = list(
                    itertools.chain.from_iterable(validation_error.messages.values())
                )
        super().__init__(message=message, validation_error=validation_error)
        self.field_name = field_name


class InvalidDataContextConfigError(InvalidBaseYamlConfigError):
    pass


class InvalidCheckpointConfigError(InvalidBaseYamlConfigError):
    pass


class InvalidBatchKwargsError(GreatExpectationsError):
    pass


class InvalidBatchSpecError(GreatExpectationsError):
    pass


class InvalidBatchRequestError(GreatExpectationsError):
    pass


class InvalidBatchIdError(GreatExpectationsError):
    pass


class InvalidDataContextKeyError(DataContextError):
    pass


class UnsupportedConfigVersionError(DataContextError):
    pass


class EvaluationParameterError(GreatExpectationsError):
    pass


class ProfilerError(GreatExpectationsError):
    pass


class ProfilerConfigurationError(ProfilerError):
    """A configuration error for a "RuleBasedProfiler" class."""

    pass


class ProfilerExecutionError(ProfilerError):
    """A runtime error for a "RuleBasedProfiler" class."""

    pass


class ProfilerNotFoundError(ProfilerError):
    pass


class DataAssistantError(ProfilerError):
    pass


class DataAssistantExecutionError(DataAssistantError):
    """A runtime error for a "DataAssistant" class."""

    pass


class DataAssistantResultExecutionError(DataAssistantError):
    """A runtime error for a "DataAssistantResult" class."""

    pass


class InvalidConfigError(DataContextError):
    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)


class MissingConfigVariableError(InvalidConfigError):
    def __init__(self, message, missing_config_variable=None) -> None:
        if not missing_config_variable:
            missing_config_variable = []
        self.message = message
        self.missing_config_variable = missing_config_variable
        super().__init__(self.message)


class AmbiguousDataAssetNameError(DataContextError):
    def __init__(self, message, candidates=None) -> None:
        self.message = message
        self.candidates = candidates
        super().__init__(self.message)


class StoreConfigurationError(DataContextError):
    pass


class InvalidExpectationKwargsError(GreatExpectationsError):
    pass


class InvalidExpectationConfigurationError(GreatExpectationsError):
    pass


class ExpectationNotFoundError(GreatExpectationsError):
    pass


class InvalidValidationResultError(GreatExpectationsError):
    pass


class GreatExpectationsTypeError(TypeError):
    pass


class StoreError(DataContextError):
    pass


class InvalidKeyError(StoreError):
    pass


class InvalidCacheValueError(GreatExpectationsError):
    def __init__(self, result_dict) -> None:
        template = """\
Invalid result values were found when trying to instantiate an ExpectationValidationResult.
- Invalid result values are likely caused by inconsistent cache values.
- Great Expectations enables caching by default.
- Please ensure that caching behavior is consistent between the underlying Dataset (e.g. Spark) and Great Expectations.
Result: {}
"""
        self.message = template.format(json.dumps(result_dict, indent=2))
        super().__init__(self.message)


class ConfigNotFoundError(DataContextError):
    """The great_expectations dir could not be found."""

    def __init__(self) -> None:
        self.message = """Error: No great_expectations directory was found here!
    - Please check that you are in the correct directory or have specified the correct directory.
    - If you have never run Great Expectations in this project, please run `great_expectations init` to get started.
"""
        super().__init__(self.message)


class PluginModuleNotFoundError(GreatExpectationsError):
    """A module import failed."""

    def __init__(self, module_name) -> None:
        template = """\
No module named `{}` could be found in your plugins directory.
    - Please verify your plugins directory is configured correctly.
    - Please verify you have a module named `{}` in your plugins directory.
"""
        self.message = template.format(module_name, module_name)

        colored_template = f"<red>{template}</red>"
        module_snippet = f"</red><yellow>{module_name}</yellow><red>"
        self.cli_colored_message = colored_template.format(
            module_snippet, module_snippet
        )
        super().__init__(self.message)


class PluginClassNotFoundError(DataContextError, AttributeError):
    """A module import failed."""

    def __init__(self, module_name, class_name) -> None:
        class_name_changes = {
            "FixedLengthTupleFilesystemStoreBackend": "TupleFilesystemStoreBackend",
            "FixedLengthTupleS3StoreBackend": "TupleS3StoreBackend",
            "FixedLengthTupleGCSStoreBackend": "TupleGCSStoreBackend",
            "InMemoryEvaluationParameterStore": "EvaluationParameterStore",
            "DatabricksTableGenerator": "DatabricksTableBatchKwargsGenerator",
            "GlobReaderGenerator": "GlobReaderBatchKwargsGenerator",
            "SubdirReaderGenerator": "SubdirReaderBatchKwargsGenerator",
            "QueryGenerator": "QueryBatchKwargsGenerator",
            "TableGenerator": "TableBatchKwargsGenerator",
            "S3Generator": "S3GlobReaderBatchKwargsGenerator",
            "ExtractAndStoreEvaluationParamsAction": "StoreEvaluationParametersAction",
            "StoreAction": "StoreValidationResultAction",
            "PartitionDefinitionSubset": "IDDict",
            "PartitionRequest": "IDDict",
            "PartitionDefinition": "IDDict",
            "PartitionQuery": "BatchFilter",
        }

        if class_name_changes.get(class_name):
            template = """The module: `{}` does not contain the class: `{}`.
            The class name `{}` has changed to `{}`."""
            self.message = template.format(
                module_name, class_name, class_name, class_name_changes.get(class_name)
            )
        else:
            template = """The module: `{}` does not contain the class: `{}`.
        - Please verify that the class named `{}` exists."""
            self.message = template.format(module_name, class_name, class_name)

        colored_template = f"<red>{template}</red>"
        module_snippet = f"</red><yellow>{module_name}</yellow><red>"
        class_snippet = f"</red><yellow>{class_name}</yellow><red>"
        if class_name_changes.get(class_name):
            new_class_snippet = (
                f"</red><yellow>{class_name_changes.get(class_name)}</yellow><red>"
            )
            self.cli_colored_message = colored_template.format(
                module_snippet, class_snippet, class_snippet, new_class_snippet
            )
        else:
            self.cli_colored_message = colored_template.format(
                module_snippet,
                class_snippet,
                class_snippet,
            )
        super().__init__(self.message)


class ClassInstantiationError(GreatExpectationsError):
    def __init__(self, module_name, package_name, class_name) -> None:
        # noinspection PyUnresolvedReferences
        module_spec: Optional[
            importlib.machinery.ModuleSpec
        ] = importlib.util.find_spec(module_name, package=package_name)
        if not module_spec:
            if not package_name:
                package_name = ""
            self.message = f"""No module named "{package_name + module_name}" could be found in the repository.  \
Please make sure that the file, corresponding to this package and module, exists and that dynamic loading of code \
modules, templates, and assets is supported in your execution environment.  This error is unrecoverable.
            """
        else:
            self.message = f"""The module "{module_name}" exists; however, the system is unable to create an instance \
of the class "{class_name}", searched for inside this module.  Please make sure that the class named "{class_name}" is \
properly defined inside its intended module and declared correctly by the calling entity.  This error is unrecoverable.
            """
        super().__init__(self.message)


class ExpectationSuiteNotFoundError(GreatExpectationsError):
    def __init__(self, data_asset_name) -> None:
        self.data_asset_name = data_asset_name
        self.message = (
            f"No expectation suite found for data_asset_name {data_asset_name}"
        )
        super().__init__(self.message)


class BatchKwargsError(DataContextError):
    def __init__(self, message, batch_kwargs=None) -> None:
        self.message = message
        self.batch_kwargs = batch_kwargs
        super().__init__(self.message)


class BatchDefinitionError(DataContextError):
    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)


class BatchSpecError(DataContextError):
    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)


class DatasourceError(DataContextError):
    def __init__(self, datasource_name: str, message: str) -> None:
        self.message = (
            f"Cannot initialize datasource {datasource_name}, error: {message}"
        )
        super().__init__(self.message)


class DatasourceConfigurationError(DatasourceError):
    pass


class DatasourceInitializationError(DatasourceError):
    pass


class DatasourceKeyPairAuthBadPassphraseError(DatasourceInitializationError):
    pass


class DatasourceNotFoundError(DataContextError):
    pass


class InvalidConfigValueTypeError(DataContextError):
    pass


class DataConnectorError(DataContextError):
    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)


class ExecutionEngineError(DataContextError):
    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)


class BatchFilterError(DataContextError):
    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)


class SorterError(DataContextError):
    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)


class SamplerError(DataContextError):
    pass


class MetricError(GreatExpectationsError):
    pass


class UnavailableMetricError(MetricError):
    pass


class MetricProviderError(MetricError):
    pass


class MetricComputationError(MetricError):
    pass


class InvalidMetricAccessorDomainKwargsKeyError(MetricError):
    pass


class MetricResolutionError(MetricError):
    def __init__(self, message, failed_metrics) -> None:
        super().__init__(message)
        if not isinstance(failed_metrics, Iterable):
            failed_metrics = (failed_metrics,)
        self.failed_metrics = failed_metrics


class GXCloudError(GreatExpectationsError):
    """
    Generic error used to provide additional context around Cloud-specific issues.
    """

    response: requests.Response

    def __init__(self, message: str, response: requests.Response) -> None:
        super().__init__(message)
        self.response = response


class GXCloudConfigurationError(GreatExpectationsError):
    """
    Error finding and verifying the required configuration values when preparing to connect to GX Cloud
    """


class DatabaseConnectionError(GreatExpectationsError):
    """Error connecting to a database including during an integration test."""


class MigrationError(GreatExpectationsError):
    """Error when using the migration tool."""
