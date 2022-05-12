from typing import Any, Dict, List, Optional

from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.rule_based_profiler.data_assistant import DataAssistant
from great_expectations.rule_based_profiler.data_assistant.data_assistant import (
    set_parameter_builders_json_serialize,
)
from great_expectations.rule_based_profiler.parameter_builder import ParameterBuilder
from great_expectations.rule_based_profiler.rule import Rule
from great_expectations.rule_based_profiler.types import Domain
from great_expectations.rule_based_profiler.types.data_assistant_result import (
    DataAssistantResult,
    VolumeDataAssistantResult,
)
from great_expectations.validator.validator import Validator


class VolumeDataAssistant(DataAssistant):
    """
    VolumeDataAssistant provides exploration and validation of "Data Volume" aspects of specified data Batch objects.

    Self-Initializing Expectations relevant for assessing "Data Volume" include:
        - "expect_table_row_count_to_be_between";
        - "expect_column_unique_value_count_to_be_between";
        - Others in the future.
    """

    __alias__: str = "volume"

    def __init__(
        self,
        name: str,
        validator: Validator,
    ) -> None:
        super().__init__(
            name=name,
            validator=validator,
        )

    @property
    def expectation_kwargs_by_expectation_type(self) -> Dict[str, Dict[str, Any]]:
        return {
            "expect_table_row_count_to_be_between": {
                "auto": True,
                "profiler_config": None,
            },
            "expect_column_unique_value_count_to_be_between": {
                "auto": True,
                "profiler_config": None,
            },
        }

    @property
    def metrics_parameter_builders_by_domain(
        self,
    ) -> Dict[Domain, List[ParameterBuilder]]:
        table_row_count_metric_multi_batch_parameter_builder: ParameterBuilder = (
            DataAssistant.COMMONLY_USED_PARAMETER_BUILDERS.table_row_count_metric_multi_batch_parameter_builder
        )
        column_distinct_values_count_metric_multi_batch_parameter_builder: ParameterBuilder = (
            DataAssistant.COMMONLY_USED_PARAMETER_BUILDERS.column_distinct_values_count_metric_multi_batch_parameter_builder
        )

        set_parameter_builders_json_serialize(
            parameter_builders=[
                table_row_count_metric_multi_batch_parameter_builder,
                column_distinct_values_count_metric_multi_batch_parameter_builder,
            ],
            json_serialize=True,
        )

        return {
            Domain(domain_type=MetricDomainTypes.TABLE,): [
                table_row_count_metric_multi_batch_parameter_builder,
            ],
            Domain(domain_type=MetricDomainTypes.COLUMN,): [
                column_distinct_values_count_metric_multi_batch_parameter_builder,
            ],
        }

    @property
    def variables(self) -> Optional[Dict[str, Any]]:
        return None

    @property
    def rules(self) -> Optional[List[Rule]]:
        return None

    def _build_data_assistant_result(
        self, data_assistant_result: DataAssistantResult
    ) -> DataAssistantResult:
        return VolumeDataAssistantResult(
            batch_id_to_batch_identifier_display_name_map=data_assistant_result.batch_id_to_batch_identifier_display_name_map,
            profiler_config=data_assistant_result.profiler_config,
            metrics_by_domain=data_assistant_result.metrics_by_domain,
            expectation_configurations=data_assistant_result.expectation_configurations,
            citation=data_assistant_result.citation,
            execution_time=data_assistant_result.execution_time,
        )
