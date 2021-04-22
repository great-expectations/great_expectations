import logging
from typing import Iterable, List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.profiler.parameter_builder.parameter_tree_container_node import ParameterTreeContainerNode
from great_expectations.profiler.parameter_builder.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.profiler.profiler_rule.rule_state import RuleState
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)


class SimpleDateFormatStringParameterBuilder(ParameterBuilder):
    """Returns the best matching strftime format string for a provided domain."""

    CANDIDATE_DATE_FORMAT_STRINGS = {
        "YYYY-MM-DD",
        "MM-DD-YYYY",
        "YY-MM-DD",
        "YYYY-mm-DDTHH:MM:SSS",
    }

    def __init__(
        self,
        *,
        parameter_name: str,
        domain_kwargs: str,
        threshold: Optional[float] = 1.0,
        candidate_strings: Optional[Iterable[str]] = None,
        additional_candidate_strings: Optional[Iterable[str]] = None,
        data_context: Optional[DataContext] = None,
    ):
        """
        Configure this SimpleDateFormatStringParameterBuilder

        Args:
            threshold: the ratio of values that must match a format string for it to be accepted
            candidate_strings: a list of candidate date format strings that will REPLACE the default
            additional_candidate_strings: a list of candidate date format strings that will SUPPLEMENT the default
        """
        super().__init__(parameter_name=parameter_name, data_context=data_context)

        self._threshold = threshold
        if candidate_strings is not None:
            self._candidate_strings = candidate_strings
        else:
            self._candidate_strings = self.CANDIDATE_DATE_FORMAT_STRINGS

        if additional_candidate_strings is not None:
            self._candidate_strings += additional_candidate_strings

        self._domain_kwargs = domain_kwargs

    # TODO: <Alex>ALEX -- This looks like a single-Batch case.</Alex>
    def _build_parameters(
        self,
        *,
        rule_state: Optional[RuleState] = None,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
        **kwargs,
    ) -> ParameterTreeContainerNode:
        """Check the percentage of values matching each string, and return the best fit, or None if no
        string exceeds the configured threshold."""
        if batch_ids is None:
            batch_ids = [validator.active_batch_id]

        if len(batch_ids) > 1:
            # By default, the validator will use active batch id (the most recently loaded batch)
            logger.warning(
                f"Rule {self.parameter_name} received {len(batch_ids)} batches but can only process one."
            )
            if batch_ids[0] not in validator.execution_engine.loaded_batch_data_ids:
                raise ge_exceptions.ProfilerExecutionError(
                    f"Parameter Builder {self.parameter_name} cannot build parameters because batch {batch_ids[0]} is not "
                    f"currently loaded in the validator."
                )

        metric_domain_kwargs: dict = rule_state.active_domain["domain_kwargs"]
        metric_domain_kwargs.update({"batch_id": batch_ids[0]})

        count: int = validator.get_metric(
            metric=MetricConfiguration(
                metric_name="column_values.not_null.count",
                metric_domain_kwargs=metric_domain_kwargs,
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )
        format_string_success_ratios = dict()
        for fmt_string in self._candidate_strings:
            format_string_success_ratios[fmt_string] = (
                validator.get_metric(
                    metric=MetricConfiguration(
                        metric_name="column_values.match_strftime_format.unexpected_count",
                        metric_domain_kwargs=metric_domain_kwargs,
                        metric_value_kwargs={"strftime_format": fmt_string},
                        metric_dependencies=None,
                    )
                )
                / count
            )

        best = None
        best_ratio = 0
        for fmt_string, ratio in format_string_success_ratios.items():
            if ratio > best_ratio and ratio >= self._threshold:
                best = fmt_string
                best_ratio = ratio

        return ParameterTreeContainerNode(
            parameters={"date_format_string": best},
            details={"success_ratio": best_ratio},
            descendants=None
        )
