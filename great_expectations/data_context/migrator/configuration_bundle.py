"""TODO: Add docstring"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Dict, List, cast

from marshmallow import Schema, fields, post_dump

from great_expectations.core.expectation_suite import (
    ExpectationSuite,
    ExpectationSuiteSchema,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationSuiteValidationResultSchema,
)
from great_expectations.data_context.data_context_variables import (
    DataContextVariables,  # noqa: TCH001
)
from great_expectations.data_context.types.base import DataContextConfigSchema
from great_expectations.util import convert_to_json_serializable  # noqa: TID251

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.datasource.fluent import Datasource as FluentDatasource


class ConfigurationBundle:
    def __init__(self, context: AbstractDataContext) -> None:
        self._context = context
        self._context_id = context.data_context_id

        self._data_context_variables: DataContextVariables = context.variables

        self._datasources = self._get_all_datasources()
        self._expectation_suites = self._get_all_expectation_suites()

        # Treated slightly differently as we require the keys downstream when printing migration status.  # noqa: E501
        self._validation_results = self._get_all_validation_results()

    @property
    def data_context_id(self) -> uuid.UUID | None:
        return self._context_id

    def is_usage_stats_enabled(self) -> bool:
        """Determine whether usage stats are enabled.

        Also returns false if there are no usage stats settings provided.

        Returns: Boolean of whether the usage statistics are enabled.

        """
        enabled = self._data_context_variables.analytics_enabled
        if enabled is None:
            enabled = True
        return enabled

    @property
    def data_context_variables(self) -> DataContextVariables:
        return self._data_context_variables

    @property
    def datasources(self) -> List[FluentDatasource]:
        return self._datasources

    @property
    def expectation_suites(self) -> List[ExpectationSuite]:
        return self._expectation_suites

    @property
    def validation_results(self) -> Dict[str, ExpectationSuiteValidationResult]:
        return self._validation_results

    def _get_all_datasources(self) -> List[FluentDatasource]:
        datasource_names: List[str] = list(self._context.datasources.keys())

        # Note: we are accessing the protected _datasource_store to not add a public property
        # to all Data Contexts.
        datasource_configs: List[FluentDatasource] = []
        for datasource_name in datasource_names:
            datasource_config = self._context._datasource_store.retrieve_by_name(
                datasource_name=datasource_name
            )
            datasource_config.name = datasource_name
            datasource_configs.append(datasource_config)

        return datasource_configs

    def _get_all_expectation_suites(self) -> List[ExpectationSuite]:
        return list(self._context.suites.all())

    def _get_all_validation_results(
        self,
    ) -> Dict[str, ExpectationSuiteValidationResult]:
        validation_results = {
            str(key): cast(
                ExpectationSuiteValidationResult,
                self._context.validation_results_store.get(key),
            )
            for key in self._context.validation_results_store.list_keys()
        }
        return validation_results


class ConfigurationBundleSchema(Schema):
    """Marshmallow Schema for the Configuration Bundle."""

    data_context_id = fields.String(allow_none=False, required=True)
    data_context_variables = fields.Nested(DataContextConfigSchema, allow_none=False)
    expectation_suites = fields.List(
        fields.Nested(ExpectationSuiteSchema, allow_none=True, required=True),
        required=True,
    )
    validation_results = fields.Dict(
        keys=fields.String(
            required=True,
            allow_none=False,
        ),
        values=fields.Nested(
            ExpectationSuiteValidationResultSchema, allow_none=True, required=True
        ),
        required=True,
    )

    @post_dump
    def clean_up(self, data, **kwargs) -> dict:
        data_context_variables = data.get("data_context_variables", {})
        data_context_variables.pop("anonymous_usage_statistics", None)
        return data


class ConfigurationBundleJsonSerializer:
    def __init__(self, schema: Schema) -> None:
        """

        Args:
            schema: Marshmallow schema defining raw serialized version of object.
        """
        self.schema = schema

    def serialize(self, obj: ConfigurationBundle) -> dict:
        """Serialize config to json dict.

        Args:
            obj: AbstractConfig object to serialize.

        Returns:
            Representation of object as a dict suitable for serializing to json.
        """
        config: dict = self.schema.dump(obj)

        json_serializable_dict: dict = convert_to_json_serializable(data=config)

        return json_serializable_dict
