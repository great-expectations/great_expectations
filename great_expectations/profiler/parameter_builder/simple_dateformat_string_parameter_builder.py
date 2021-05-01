import copy
import logging
from typing import Any, Dict, Iterable, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core.domain_types import MetricDomainTypes
from great_expectations.profiler.domain_builder.domain import Domain
from great_expectations.profiler.parameter_builder.parameter_builder import (
    ParameterBuilder,
)
from great_expectations.profiler.parameter_builder.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_NAME,
    ParameterContainer,
    build_parameter_container,
)
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)


class SimpleDateFormatStringParameterBuilder(ParameterBuilder):
    """Returns the best matching strftime format string for a provided domain."""

    CANDIDATE_DATE_FORMAT_STRINGS: str = {
        "YYYY-MM-DD",
        "MM-DD-YYYY",
        "YY-MM-DD",
        "YYYY-mm-DDTHH:MM:SSS",
    }

    def __init__(
        self,
        parameter_name: str,
        validator: Validator,
        domain: Domain,
        domain_kwargs: str,
        threshold: Optional[float] = 1.0,
        candidate_strings: Optional[Iterable[str]] = None,
        additional_candidate_strings: Optional[Iterable[str]] = None,
        rule_variables: Optional[ParameterContainer] = None,
        rule_domain_parameters: Optional[Dict[str, ParameterContainer]] = None,
        data_context: Optional[DataContext] = None,
    ):
        """
        Configure this SimpleDateFormatStringParameterBuilder

        Args:
            threshold: the ratio of values that must match a format string for it to be accepted
            candidate_strings: a list of candidate date format strings that will REPLACE the default
            additional_candidate_strings: a list of candidate date format strings that will SUPPLEMENT the default
        """

        super().__init__(
            parameter_name=parameter_name,
            validator=validator,
            domain=domain,
            rule_variables=rule_variables,
            rule_domain_parameters=rule_domain_parameters,
            data_context=data_context,
        )

        self._domain_kwargs = domain_kwargs

        self._threshold = threshold

        if candidate_strings is not None:
            self._candidate_strings = candidate_strings
        else:
            self._candidate_strings = self.CANDIDATE_DATE_FORMAT_STRINGS

        if additional_candidate_strings is not None:
            self._candidate_strings += additional_candidate_strings

    # TODO: <Alex>ALEX -- This looks like a single-Batch case.</Alex>
    def _build_parameters(
        self,
        *,
        batch_ids: Optional[List[str]] = None,
    ) -> ParameterContainer:
        """Check the percentage of values matching each string, and return the best fit, or None if no
        string exceeds the configured threshold."""
        if batch_ids is None:
            batch_ids = [self.validator.active_batch_id]

        if len(batch_ids) > 1:
            # By default, the validator will use active batch id (the most recently loaded batch)
            logger.warning(
                f"Rule {self.parameter_name} received {len(batch_ids)} batches but can only process one."
            )
            if (
                batch_ids[0]
                not in self.validator.execution_engine.loaded_batch_data_ids
            ):
                raise ge_exceptions.ProfilerExecutionError(
                    f"Parameter Builder {self.parameter_name} cannot build parameters because batch {batch_ids[0]} is not "
                    f"currently loaded in the validator."
                )

        # TODO: <Alex>ALEX -- type overloading is generally a poor practice; the caller should decide on the type of "metric_domain_kwargs" and call this method accordingly.</Alex>
        # Using "__getitem__" (bracket) notation instead of "__getattr__" (dot) notation in order to insure the
        # compatibility of field names (e.g., "domain_kwargs") with user-facing syntax (as governed by the value of
        # the DOMAIN_KWARGS_PARAMETER_NAME constant, which may change, requiring the same change to the field name).
        metric_domain_kwargs: Union[
            str, Dict[str, Union[str, MetricDomainTypes, Dict[str, Any]]]
        ] = copy.deepcopy(self.domain[DOMAIN_KWARGS_PARAMETER_NAME])
        metric_domain_kwargs.update({"batch_id": batch_ids[0]})

        count: int = self.validator.get_metric(
            metric=MetricConfiguration(
                metric_name="column_values.not_null.count",
                metric_domain_kwargs=metric_domain_kwargs,
                metric_value_kwargs=None,
                metric_dependencies=None,
            )
        )

        format_string: str
        format_string_success_ratios: Dict[str, str] = {
            format_string: float(
                self.validator.get_metric(
                    metric=MetricConfiguration(
                        metric_name="column_values.match_strftime_format.unexpected_count",
                        metric_domain_kwargs=metric_domain_kwargs,
                        metric_value_kwargs={"strftime_format": format_string},
                        metric_dependencies=None,
                    )
                )
            )
            / count
            for format_string in self._candidate_strings
        }

        best_fit_date_format_estimate: Optional[str] = None
        best_success_ratio: float = 0.0
        current_success_ratio: float
        for (
            format_string,
            current_success_ratio,
        ) in format_string_success_ratios.items():
            if (
                current_success_ratio > best_success_ratio
                and current_success_ratio >= self._threshold
            ):
                best_fit_date_format_estimate = format_string
                best_success_ratio = current_success_ratio

        parameter_values: Dict[str, Dict[str, Any]] = {
            "$parameter.date_format_string": {
                "value": best_fit_date_format_estimate,
                "details": {"success_ratio": best_success_ratio},
            },
        }
        return build_parameter_container(parameter_values=parameter_values)
