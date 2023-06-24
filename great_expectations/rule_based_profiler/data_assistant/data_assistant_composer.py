from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Dict,
    Final,
    List,
    Mapping,
    MutableMapping,
)

logger = logging.getLogger(__name__)


from great_expectations.core.domain import Domain
from great_expectations.core.metric_domain_types import MetricDomainTypes

if TYPE_CHECKING:
    from great_expectations.core import ExpectationConfiguration
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.datasource.fluent.batch_request import BatchRequest
    from great_expectations.rule_based_profiler.data_assistant.data_assistant_runner import (
        DataAssistant,
        DataAssistantRunner,
    )
    from great_expectations.rule_based_profiler.data_assistant_result import (
        DataAssistantResult,
    )

    # TODO: <Alex>ALEX</Alex>
    # DomainBuilderDataAssistantResult,
    # TODO: <Alex>ALEX</Alex>
    from great_expectations.rule_based_profiler.rule import Rule


class DataAssistantComposer:
    TASK_NAME_TO_JOB_CATEGORY_MAP: Final[Dict[str, str]] = {
        "uniqueness": "column_value_uniqueness",
        # TODO: <Alex>06/21/2023: This approach is currently disfavored, because it determines domains automatically.</Alex>
        # TODO: <Alex>ALEX</Alex>
        # "nullity": "column_value_nullity",
        # "nonnullity": "column_value_nonnullity",
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        "missingness": "column_value_missing",
        # TODO: <Alex>ALEX</Alex>
        "categorical_two": "categorical",
        "categorical_very_few": "categorical",
        "numeric": "numeric",
        "text": "text",
    }

    def __init__(
        self,
        batch_request: BatchRequest,
        expectation_suite_name: str,
        data_context: AbstractDataContext,
    ) -> None:
        self._batch_request: BatchRequest = batch_request
        self._expectation_suite_name: str = expectation_suite_name
        self._data_context: AbstractDataContext = data_context

        self._task_name_to_domain_list_map: Mapping[str, List[Domain]] | None = None

        task_name: str
        self._task_name_to_data_assistant_result_map: MutableMapping[
            str, DataAssistantResult
        ] = {
            task_name: None
            for task_name in DataAssistantComposer.TASK_NAME_TO_JOB_CATEGORY_MAP
        }

    def build_domains(self) -> None:
        data_assistant: DataAssistant = (
            self._data_context.assistants.domains._build_data_assistant()
        )
        rules: List[Rule] = data_assistant.get_rules()

        rule: Rule
        rule_names: List[str] = [rule.name for rule in rules]

        data_assistant_result: DataAssistantResult

        data_assistant_result = self._data_context.assistants.domains.run(
            batch_request=self._batch_request,
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

        candidate_table_domains: List[Domain] = domains_by_rule_name[
            "table_domain_rule"
        ]
        if candidate_table_domains:
            logger.info(
                f"""Candidate "{MetricDomainTypes.TABLE}" type "Domain" objects:\n{candidate_table_domains}"""
            )
        candidate_column_value_uniqueness_domains: List[Domain] = domains_by_rule_name[
            "column_value_uniqueness_domain_rule"
        ]
        # TODO: <Alex>06/21/2023: This approach is currently disfavored, because it determines domains automatically.</Alex>
        # TODO: <Alex>ALEX</Alex>
        # candidate_column_value_nullity_domains: List[Domain] = domains_by_rule_name[
        #     "column_value_nullity_domain_rule"
        # ]
        # candidate_column_value_nonnullity_domains: List[Domain] = domains_by_rule_name[
        #     "column_value_nonnullity_domain_rule"
        # ]
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        # candidate_column_value_nullity_domains: List[Domain] = []
        # candidate_column_value_nonnullity_domains: List[Domain] = []
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        data_assistant_result = self._data_context.assistants.column_value_missing.run(
            batch_request=self._batch_request,
        )
        self._task_name_to_data_assistant_result_map[
            "missingness"
        ] = data_assistant_result

        expectation_configurations: List[
            ExpectationConfiguration
        ] = data_assistant_result.get_expectation_suite(
            expectation_suite_name=self._expectation_suite_name
        ).get_column_expectations()
        expectation_configuration: ExpectationConfiguration
        candidate_column_value_nullity_domains: List[Domain] = [
            Domain(
                domain_type=MetricDomainTypes.COLUMN,
                domain_kwargs={"column": expectation_configuration.kwargs["column"]},
                details=None,
                rule_name=None,
            )
            for expectation_configuration in expectation_configurations
            if expectation_configuration["expectation_type"]
            == "expect_column_values_to_be_null"
        ]
        candidate_column_value_nonnullity_domains: List[Domain] = [
            Domain(
                domain_type=MetricDomainTypes.COLUMN,
                domain_kwargs={"column": expectation_configuration.kwargs["column"]},
                details=None,
                rule_name=None,
            )
            for expectation_configuration in expectation_configurations
            if expectation_configuration["expectation_type"]
            == "expect_column_values_to_not_be_null"
        ]
        # TODO: <Alex>ALEX</Alex>
        candidate_numeric_columns_domains: List[Domain] = domains_by_rule_name[
            "numeric_columns_domain_rule"
        ]
        candidate_datetime_columns_domains: List[Domain] = domains_by_rule_name[
            "datetime_columns_domain_rule"
        ]
        candidate_text_columns_domains: List[Domain] = domains_by_rule_name[
            "text_columns_domain_rule"
        ]
        candidate_categorical_columns_domains_zero: List[Domain] = domains_by_rule_name[
            "categorical_columns_domain_rule_zero"
        ]
        candidate_categorical_columns_domains_one: List[Domain] = domains_by_rule_name[
            "categorical_columns_domain_rule_one"
        ]
        candidate_categorical_columns_domains_two: List[Domain] = domains_by_rule_name[
            "categorical_columns_domain_rule_two"
        ]
        candidate_categorical_columns_domains_very_few: List[
            Domain
        ] = domains_by_rule_name["categorical_columns_domain_rule_very_few"]
        candidate_categorical_columns_domains_few: List[Domain] = domains_by_rule_name[
            "categorical_columns_domain_rule_few"
        ]
        if candidate_categorical_columns_domains_few:
            logger.info(
                f"""Candidate "{MetricDomainTypes.COLUMN}" type "categorical_values_few" "Domain" objects:\n{candidate_categorical_columns_domains_few}"""
            )
        candidate_categorical_columns_domains_some: List[Domain] = domains_by_rule_name[
            "categorical_columns_domain_rule_some"
        ]
        if candidate_categorical_columns_domains_some:
            logger.info(
                f"""Candidate "{MetricDomainTypes.COLUMN}" type "categorical_values_some" "Domain" objects:\n{candidate_categorical_columns_domains_some}"""
            )

        numeric_columns_domains: List[Domain] = list(
            filter(
                lambda domain: domain.domain_kwargs["column"]
                not in [
                    domain_key.domain_kwargs["column"]
                    for domain_key in (
                        candidate_column_value_nullity_domains
                        + candidate_categorical_columns_domains_zero
                        + candidate_categorical_columns_domains_one
                        + candidate_categorical_columns_domains_two
                        + candidate_categorical_columns_domains_very_few
                    )
                ],
                candidate_numeric_columns_domains,
            )
        )

        datetime_columns_domains: List[Domain] = list(
            filter(
                lambda domain: domain.domain_kwargs["column"]
                not in [
                    domain_key.domain_kwargs["column"]
                    for domain_key in (
                        candidate_column_value_nullity_domains
                        + candidate_categorical_columns_domains_zero
                        + candidate_categorical_columns_domains_one
                        + candidate_categorical_columns_domains_two
                        + candidate_categorical_columns_domains_very_few
                    )
                ],
                candidate_datetime_columns_domains,
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
                        candidate_column_value_nullity_domains
                        + candidate_categorical_columns_domains_zero
                        + candidate_categorical_columns_domains_one
                        + candidate_categorical_columns_domains_two
                        + candidate_categorical_columns_domains_very_few
                    )
                ],
                candidate_text_columns_domains,
            )
        )

        categorical_columns_domains_two: List[Domain] = list(
            filter(
                lambda domain: domain.domain_kwargs["column"]
                not in [
                    domain_key.domain_kwargs["column"]
                    for domain_key in (candidate_column_value_nullity_domains)
                ],
                candidate_categorical_columns_domains_two,
            )
        )

        categorical_columns_domains_very_few: List[Domain] = list(
            filter(
                lambda domain: domain.domain_kwargs["column"]
                not in [
                    domain_key.domain_kwargs["column"]
                    for domain_key in (candidate_column_value_nullity_domains)
                ],
                candidate_categorical_columns_domains_very_few,
            )
        )

        column_value_uniqueness_domains: List[Domain] = list(
            filter(
                lambda domain: domain.domain_kwargs["column"]
                not in [
                    domain_key.domain_kwargs["column"]
                    for domain_key in (candidate_column_value_nullity_domains)
                ],
                candidate_column_value_uniqueness_domains,
            )
        )

        # TODO: <Alex>ALEX</Alex>
        column_value_nullity_domains: List[
            Domain
        ] = candidate_column_value_nullity_domains

        column_value_nonnullity_domains: List[
            Domain
        ] = candidate_column_value_nonnullity_domains

        column_value_missing_domains: List[Domain] = (
            column_value_nullity_domains + column_value_nonnullity_domains
        )
        # TODO: <Alex>ALEX</Alex>

        self._task_name_to_domain_list_map = {
            "uniqueness": column_value_uniqueness_domains,
            # TODO: <Alex>06/21/2023: This approach is currently disfavored, because it determines domains automatically.</Alex>
            # TODO: <Alex>ALEX</Alex>
            # "nullity": column_value_nullity_domains,
            # "nonnullity": column_value_nonnullity_domains,
            # TODO: <Alex>ALEX</Alex>
            # TODO: <Alex>ALEX</Alex>
            "missingness": column_value_missing_domains,
            # TODO: <Alex>ALEX</Alex>
            "categorical_two": categorical_columns_domains_two,
            "categorical_very_few": categorical_columns_domains_very_few,
            "numeric": numeric_columns_domains,
            "text": text_columns_domains,
        }

    def execute_data_assistant(
        self,
        task_name: str,
    ) -> DataAssistantResult:
        # TODO: <Alex>ALEX</Alex>
        domains: list[Domain] = self._task_name_to_domain_list_map[task_name]

        domains: Domain
        include_column_names = [domain.domain_kwargs["column"] for domain in domains]

        job_category: str = DataAssistantComposer.TASK_NAME_TO_JOB_CATEGORY_MAP[
            task_name
        ]
        data_assistant_runner: DataAssistantRunner = getattr(
            self._data_context.assistants, job_category
        )

        if self._task_name_to_data_assistant_result_map.get(task_name):
            return self._task_name_to_data_assistant_result_map[task_name]

        data_assistant_result: DataAssistantResult = data_assistant_runner.run(
            batch_request=self._batch_request,
            include_column_names=include_column_names,
        )
        self._task_name_to_data_assistant_result_map[task_name] = data_assistant_result
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>06/21/2023: This approach is currently disfavored, because it determines domains automatically.</Alex>
        # TODO: <Alex>ALEX</Alex>
        # data_assistant_result: DataAssistantResult = self._data_context.assistants.column_value_nonnullity.run(
        #     batch_request=self._batch_request,
        #     # TODO: <Alex>ALEX</Alex>
        #     # include_column_names=include_column_names,
        #     # TODO: <Alex>ALEX</Alex>
        # )
        # TODO: <Alex>ALEX</Alex>

        return data_assistant_result
