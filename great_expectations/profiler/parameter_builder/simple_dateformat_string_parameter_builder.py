import logging
from typing import Iterable, Optional

from ...validator.validation_graph import MetricConfiguration
from ..exceptions import ProfilerExecutionError
from .parameter_builder import ParameterBuilder

logger = logging.getLogger(__name__)


class SimpleDateFormatStringParameterBuilder(ParameterBuilder):
    """Returns the best matching strftime format string for a provided domain."""

    CANDIDATE_STRINGS = {"YYYY-MM-DD", "MM-DD-YYYY", "YY-MM-DD", "YYYY-mm-DDTHH:MM:SSS"}

    def __init__(
        self,
        *,
        parameter_id,
        data_context,
        threshold: float = 1.0,
        candidate_strings: Optional[Iterable[str]] = None,
        additional_candidate_strings: Optional[Iterable[str]] = None,
    ):
        """
        Configure this SimpleDateFormatStringParameterBuilder

        Args:
            threshold: the ratio of values that must match a format string for it to be accepted
            candidate_strings: a list of candidate date format strings that will REPLACE the default
            additional_candidate_strings: a list of candidate date format strings that will SUPPLEMENT the default
        """
        super().__init__(parameter_id=parameter_id, data_context=data_context)
        self._threshold = threshold
        if candidate_strings is not None:
            self._candidate_strings = candidate_strings
        else:
            self._candidate_strings = self.CANDIDATE_STRINGS

        if additional_candidate_strings is not None:
            self._candidate_strings += additional_candidate_strings

    def _build_parameters(self, *, rule_state, validator, batch_ids, **kwargs):
        """Check the percentage of values matching each string, and return the best fit, or None if no
        string exceeds the configured threshold."""
        if batch_ids is None:
            batch_ids = [validator.active_batch_id]

        if len(batch_ids) > 1:
            # By default, the validator will use active batch id (the most recently loaded batch)
            logger.warning(
                f"Rule {self.parameter_id} received {len(batch_ids)} batches but can only process one."
            )
            if batch_ids[0] not in validator.execution_engine.loaded_batch_data_ids:
                raise ProfilerExecutionError(
                    f"Parameter Builder {self.parameter_id} cannot build parameters because batch {batch_ids[0]} is not "
                    f"currently loaded in the validator."
                )

        domain = rule_state.active_domain["domain_kwargs"]
        domain.update({"batch_id": batch_ids[0]})

        count = validator.get_metric(
            MetricConfiguration(
                "column_values.not_null.count",
                domain,
            )
        )
        format_string_success_ratios = dict()
        for fmt_string in self._candidate_strings:
            format_string_success_ratios[fmt_string] = (
                validator.get_metric(
                    MetricConfiguration(
                        "column_values.match_strftime_format.unexpected_count",
                        domain,
                        {"strftime_format": fmt_string},
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

        return {"parameters": best, "details": {"success_ratio": best_ratio}}
