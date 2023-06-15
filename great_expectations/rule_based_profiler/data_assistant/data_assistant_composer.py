from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Dict,
    Final,
    List,
    Mapping,
)

from great_expectations.core.metric_domain_types import MetricDomainTypes

if TYPE_CHECKING:
    from great_expectations.core.domain import Domain
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.datasource.fluent.batch_request import BatchRequest
    from great_expectations.rule_based_profiler.data_assistant.data_assistant_runner import (
        DataAssistantRunner,
    )
    from great_expectations.rule_based_profiler.data_assistant_result import (
        DataAssistantResult,
        DomainBuilderDataAssistantResult,
    )
    from great_expectations.rule_based_profiler.rule import Rule

logger = logging.getLogger(__name__)


class DataAssistantComposer:
    ANALYSIS_TYPE_TO_ANALYSIS_CATEGORY_MAP: Final[Dict[str, str]] = {
        "uniqueness": "column_value_uniqueness",
        "nullity": "column_value_nullity",
        "nonnullity": "column_value_nonnullity",
        "categorical_two": "categorical",
        "numeric": "numeric",
        "text": "text",
    }

    def __init__(
        self,
        batch_request: BatchRequest,
        data_context: AbstractDataContext,
    ) -> None:
        self._batch_request: BatchRequest = batch_request
        self._data_context: AbstractDataContext = data_context

        self._analysis_type_to_domain_list_map: Mapping[str, List[Domain]] | None = None

    def build_domains_for_data_assistant_runners(self) -> None:
        data_assistant = self._data_context.assistants.domains._build_data_assistant()
        rules: List[Rule] = data_assistant.get_rules()

        rule: Rule
        rule_names: List[str] = [rule.name for rule in rules]

        data_assistant_result: DomainBuilderDataAssistantResult = (
            self._data_context.assistants.domains.run(
                batch_request=self._batch_request,
            )
        )

        domains: List[Domain] = list(data_assistant_result.metrics_by_domain.keys())

        rule_name: str
        domain: Domain
        domains_by_rule_name = {
            rule_name: list(
                filter(
                    lambda domain: domain.rule_name == rule_name,
                    domains,
                )
            )
            for rule_name in rule_names
        }

        raw_table_domains: List[Domain] = domains_by_rule_name["table_domain_rule"]
        if raw_table_domains:
            logger.info(
                f"""Raw "{MetricDomainTypes.TABLE}" type "Domain" objects:\n{raw_table_domains}"""
            )
        raw_column_value_uniqueness_domains: List[Domain] = domains_by_rule_name[
            "column_value_uniqueness_domain_rule"
        ]
        raw_column_value_nullity_domains: List[Domain] = domains_by_rule_name[
            "column_value_nullity_domain_rule"
        ]
        raw_column_value_nonnullity_domains: List[Domain] = domains_by_rule_name[
            "column_value_nonnullity_domain_rule"
        ]
        raw_numeric_columns_domains: List[Domain] = domains_by_rule_name[
            "numeric_columns_domain_rule"
        ]
        raw_datetime_columns_domains: List[Domain] = domains_by_rule_name[
            "datetime_columns_domain_rule"
        ]
        raw_text_columns_domains: List[Domain] = domains_by_rule_name[
            "text_columns_domain_rule"
        ]
        raw_categorical_columns_domains_zero: List[Domain] = domains_by_rule_name[
            "categorical_columns_domain_rule_zero"
        ]
        raw_categorical_columns_domains_one: List[Domain] = domains_by_rule_name[
            "categorical_columns_domain_rule_one"
        ]
        raw_categorical_columns_domains_two: List[Domain] = domains_by_rule_name[
            "categorical_columns_domain_rule_two"
        ]
        raw_categorical_columns_domains_very_few: List[Domain] = domains_by_rule_name[
            "categorical_columns_domain_rule_very_few"
        ]
        raw_categorical_columns_domains_few: List[Domain] = domains_by_rule_name[
            "categorical_columns_domain_rule_few"
        ]
        if raw_categorical_columns_domains_few:
            logger.info(
                f"""Raw "{MetricDomainTypes.COLUMN}" type "categorical_values_few" "Domain" objects:\n{raw_categorical_columns_domains_few}"""
            )
        raw_categorical_columns_domains_some: List[Domain] = domains_by_rule_name[
            "categorical_columns_domain_rule_some"
        ]
        if raw_categorical_columns_domains_some:
            logger.info(
                f"""Raw "{MetricDomainTypes.COLUMN}" type "categorical_values_some" "Domain" objects:\n{raw_categorical_columns_domains_some}"""
            )

        numeric_columns_domains: List[Domain] = list(
            filter(
                lambda domain: domain.domain_kwargs["column"]
                not in [
                    domain_key.domain_kwargs["column"]
                    for domain_key in (
                        raw_column_value_nullity_domains
                        + raw_categorical_columns_domains_zero
                        + raw_categorical_columns_domains_one
                        + raw_categorical_columns_domains_two
                        + raw_categorical_columns_domains_very_few
                    )
                ],
                raw_numeric_columns_domains,
            )
        )

        datetime_columns_domains: List[Domain] = list(
            filter(
                lambda domain: domain.domain_kwargs["column"]
                not in [
                    domain_key.domain_kwargs["column"]
                    for domain_key in (
                        raw_column_value_nullity_domains
                        + raw_categorical_columns_domains_zero
                        + raw_categorical_columns_domains_one
                        + raw_categorical_columns_domains_two
                        + raw_categorical_columns_domains_very_few
                    )
                ],
                raw_datetime_columns_domains,
            )
        )
        if datetime_columns_domains:
            logger.info(
                f"""Actual "{MetricDomainTypes.COLUMN}" type "datetime" "Domain" objects:\n{datetime_columns_domains}"""
            )

        text_columns_domains: List[Domain] = list(
            filter(
                lambda domain: domain.domain_kwargs["column"]
                not in [
                    domain_key.domain_kwargs["column"]
                    for domain_key in (
                        raw_column_value_nullity_domains
                        + raw_categorical_columns_domains_zero
                        + raw_categorical_columns_domains_one
                        + raw_categorical_columns_domains_two
                        + raw_categorical_columns_domains_very_few
                    )
                ],
                raw_text_columns_domains,
            )
        )

        categorical_columns_domains_two: List[Domain] = list(
            filter(
                lambda domain: domain.domain_kwargs["column"]
                not in [
                    domain_key.domain_kwargs["column"]
                    for domain_key in (raw_column_value_nullity_domains)
                ],
                raw_categorical_columns_domains_two,
            )
        )

        column_value_uniqueness_domains: List[Domain] = list(
            filter(
                lambda domain: domain.domain_kwargs["column"]
                not in [
                    domain_key.domain_kwargs["column"]
                    for domain_key in (raw_column_value_nullity_domains)
                ],
                raw_column_value_uniqueness_domains,
            )
        )

        column_value_nullity_domains: List[Domain] = raw_column_value_nullity_domains

        column_value_nonnullity_domains: List[
            Domain
        ] = raw_column_value_nonnullity_domains

        self._analysis_type_to_domain_list_map = {
            "uniqueness": column_value_uniqueness_domains,
            "nullity": column_value_nullity_domains,
            "nonnullity": column_value_nonnullity_domains,
            "categorical_two": categorical_columns_domains_two,
            "numeric": numeric_columns_domains,
            "text": text_columns_domains,
        }

    def execute_data_assistant(
        self,
        analysis_type: str,
    ) -> DataAssistantResult:
        domains: list[Domain] = self._analysis_type_to_domain_list_map[analysis_type]

        domains: Domain
        include_column_names = [domain.domain_kwargs["column"] for domain in domains]

        analysis_category: str = (
            DataAssistantComposer.ANALYSIS_TYPE_TO_ANALYSIS_CATEGORY_MAP[analysis_type]
        )
        data_assistant_runner: DataAssistantRunner = getattr(
            self._data_context.assistants, analysis_category
        )

        data_assistant_result: DataAssistantResult = data_assistant_runner.run(
            batch_request=self._batch_request,
            include_column_names=include_column_names,
        )

        return data_assistant_result
