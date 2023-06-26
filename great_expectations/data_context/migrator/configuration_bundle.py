"""TODO: Add docstring"""
from __future__ import annotations

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
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.data_context_variables import (
    DataContextVariables,  # noqa: TCH001
)
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    CheckpointConfigSchema,
    DataContextConfigSchema,
    DatasourceConfig,
    DatasourceConfigSchema,
)
from great_expectations.rule_based_profiler.config.base import (
    RuleBasedProfilerConfig,
    RuleBasedProfilerConfigSchema,
    ruleBasedProfilerConfigSchema,
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )


class ConfigurationBundle:
    def __init__(self, context: AbstractDataContext) -> None:
        self._context = context
        self._context_id = context.data_context_id

        self._data_context_variables: DataContextVariables = context.variables

        self._datasources: List[DatasourceConfig] = self._get_all_datasources()
        self._expectation_suites: List[
            ExpectationSuite
        ] = self._get_all_expectation_suites()
        self._checkpoints: List[CheckpointConfig] = self._get_all_checkpoints()
        self._profilers: List[RuleBasedProfilerConfig] = self._get_all_profilers()

        # Treated slightly differently as we require the keys downstream when printing migration status.
        self._validation_results: Dict[
            str, ExpectationSuiteValidationResult
        ] = self._get_all_validation_results()

    @property
    def data_context_id(self) -> str:
        return self._context_id

    def is_usage_stats_enabled(self) -> bool:
        """Determine whether usage stats are enabled.

        Also returns false if there are no usage stats settings provided.

        Returns: Boolean of whether the usage statistics are enabled.

        """
        if self._data_context_variables.anonymous_usage_statistics:
            return self._data_context_variables.anonymous_usage_statistics.enabled
        else:
            return False

    @property
    def data_context_variables(self) -> DataContextVariables:
        return self._data_context_variables

    @property
    def datasources(self) -> List[DatasourceConfig]:
        return self._datasources

    @property
    def expectation_suites(self) -> List[ExpectationSuite]:
        return self._expectation_suites

    @property
    def checkpoints(self) -> List[CheckpointConfig]:
        return self._checkpoints

    @property
    def profilers(self) -> List[RuleBasedProfilerConfig]:
        return self._profilers

    @property
    def validation_results(self) -> Dict[str, ExpectationSuiteValidationResult]:
        return self._validation_results

    def _get_all_datasources(self) -> List[DatasourceConfig]:
        datasource_names: List[str] = list(self._context.datasources.keys())

        # Note: we are accessing the protected _datasource_store to not add a public property
        # to all Data Contexts.
        datasource_configs: List[DatasourceConfig] = []
        for datasource_name in datasource_names:
            datasource_config = self._context._datasource_store.retrieve_by_name(
                datasource_name=datasource_name
            )
            datasource_config.name = datasource_name
            datasource_configs.append(datasource_config)

        return datasource_configs

    def _get_all_expectation_suites(self) -> List[ExpectationSuite]:
        return [
            self._context.get_expectation_suite(name)
            for name in self._context.list_expectation_suite_names()
        ]

    def _get_all_checkpoints(self) -> List[CheckpointConfig]:
        return [
            self._context.checkpoint_store.get_checkpoint(name=checkpoint_name, id=None)
            for checkpoint_name in self._context.list_checkpoints()
        ]

    def _get_all_profilers(self) -> List[RuleBasedProfilerConfig]:
        def round_trip_profiler_config(
            profiler_config: RuleBasedProfilerConfig,
        ) -> RuleBasedProfilerConfig:
            return ruleBasedProfilerConfigSchema.load(
                ruleBasedProfilerConfigSchema.dump(profiler_config)
            )

        return [
            round_trip_profiler_config(self._context.get_profiler(name).config)
            for name in self._context.list_profilers()
        ]

    def _get_all_validation_results(
        self,
    ) -> Dict[str, ExpectationSuiteValidationResult]:
        validation_results = {
            str(key): cast(
                ExpectationSuiteValidationResult,
                self._context.validations_store.get(key),
            )
            for key in self._context.validations_store.list_keys()
        }
        return validation_results


class ConfigurationBundleSchema(Schema):
    """Marshmallow Schema for the Configuration Bundle."""

    data_context_id = fields.String(allow_none=False, required=True)
    data_context_variables = fields.Nested(DataContextConfigSchema, allow_none=False)
    datasources = fields.List(
        fields.Nested(DatasourceConfigSchema, allow_none=True, required=True),
        required=True,
    )
    expectation_suites = fields.List(
        fields.Nested(ExpectationSuiteSchema, allow_none=True, required=True),
        required=True,
    )
    checkpoints = fields.List(
        fields.Nested(CheckpointConfigSchema, allow_none=True, required=True),
        required=True,
    )
    profilers = fields.List(
        fields.Nested(RuleBasedProfilerConfigSchema, allow_none=True, required=True),
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
