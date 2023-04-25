"""Methods, classes and files to be included as part of the public API, even if they are not mentioned in docs snippets.
"""
from __future__ import annotations

import pathlib

from docs.sphinx_api_docs_source.include_exclude_definition import (
    IncludeExcludeDefinition,
)

DEFAULT_INCLUDES: list[IncludeExcludeDefinition] = [
    IncludeExcludeDefinition(
        reason="Referenced via legacy docs, will likely need to be included in the public API. Added here as an example include.",
        name="remove_expectation",
        filepath=pathlib.Path("great_expectations/core/expectation_suite.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validation Actions are used within Checkpoints but are part of our Public API and can be overridden via plugins.",
        name="ValidationAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validation Actions are used within Checkpoints but are part of our Public API and can be overridden via plugins.",
        name="run",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validation Actions are used within Checkpoints but are part of our Public API and can be overridden via plugins.",
        name="_run",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validation Actions are used within Checkpoints but are part of our Public API and can be overridden via plugins.",
        name="SlackNotificationAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validation Actions are used within Checkpoints but are part of our Public API and can be overridden via plugins.",
        name="PagerdutyAlertAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validation Actions are used within Checkpoints but are part of our Public API and can be overridden via plugins.",
        name="MicrosoftTeamsNotificationAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validation Actions are used within Checkpoints but are part of our Public API and can be overridden via plugins.",
        name="OpsgenieAlertAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validation Actions are used within Checkpoints but are part of our Public API and can be overridden via plugins.",
        name="EmailAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validation Actions are used within Checkpoints but are part of our Public API and can be overridden via plugins.",
        name="StoreValidationResultAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validation Actions are used within Checkpoints but are part of our Public API and can be overridden via plugins.",
        name="StoreEvaluationParametersAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validation Actions are used within Checkpoints but are part of our Public API and can be overridden via plugins.",
        name="StoreMetricsAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validation Actions are used within Checkpoints but are part of our Public API and can be overridden via plugins.",
        name="UpdateDataDocsAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validation Actions are used within Checkpoints but are part of our Public API and can be overridden via plugins.",
        name="CloudNotificationAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validation Actions are used within Checkpoints but are part of our Public API and can be overridden via plugins.",
        name="SNSNotificationAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="DataAssistantResult is returned from running a DataAssistant",
        name="DataAssistantResult",
        filepath=pathlib.Path(
            "great_expectations/rule_based_profiler/data_assistant_result/data_assistant_result.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="InferredAssetFilePathDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/inferred_asset_file_path_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="InferredAssetDBFSDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/inferred_asset_dbfs_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="InferredAssetGCSDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/inferred_asset_gcs_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="ConfiguredAssetDBFSDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/configured_asset_dbfs_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="FilePathDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/file_path_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="InferredAssetAzureDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/inferred_asset_azure_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="RuntimeDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/runtime_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="ConfiguredAssetAWSGlueDataCatalogDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/configured_asset_aws_glue_data_catalog_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="InferredAssetS3DataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/inferred_asset_s3_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="ConfiguredAssetFilesystemDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/configured_asset_filesystem_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="ConfiguredAssetS3DataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/configured_asset_s3_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="ConfiguredAssetAzureDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/configured_asset_azure_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="InferredAssetSqlDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/inferred_asset_sql_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="ConfiguredAssetSqlDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/configured_asset_sql_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="DataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="ConfiguredAssetFilePathDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/configured_asset_file_path_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="InferredAssetAWSGlueDataCatalogDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/inferred_asset_aws_glue_data_catalog_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="InferredAssetFilesystemDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/inferred_asset_filesystem_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="DataConnectors are part of the public API",
        name="ConfiguredAssetGCSDataConnector",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/configured_asset_gcs_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Data Context types are part of the public API",
        name="EphemeralDataContext",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/ephemeral_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Data Context types are part of the public API",
        name="FileDataContext",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/file_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Data Context types are part of the public API",
        name="CloudDataContext",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/cloud_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Map metric providers are part of the public API",
        name="MapMetricProvider",
        filepath=pathlib.Path(
            "great_expectations/expectations/metrics/map_metric_provider.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Map metric providers are part of the public API",
        name="MetricProvider",
        filepath=pathlib.Path(
            "great_expectations/expectations/metrics/metric_provider.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Checkpoint CRUD is part of the public API",
        name="delete_checkpoint",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/abstract_data_context.py"
        ),
    ),
]
