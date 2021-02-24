"""
Defines the MultiBatchBootstrappedMetricDistributionParameterBuilder.
"""
from copy import copy

from ...validator.validation_graph import MetricConfiguration
from .multi_batch_parameter_builder import MultiBatchParameterBuilder


class MultiBatchBootstrappedMetricDistributionParameterBuilder(
    MultiBatchParameterBuilder
):
    """
    Builds parameters from the p_values of the distribution of a metric observed from a
    set of batches identified in the batch_ids.
    """

    def __init__(
        self,
        *,
        parameter_id,
        batch_request,
        metric_name,
        metric_value_kwargs,
        p_values,
        data_context
    ):
        """
        Create a MultiBatchBootstrappedMetricDistributionParameterBuilder.

        The ParameterBuilder will build parameters for the active domain from the rule.

        Args:
            parameter_id: the id for this ParameterBuilder
            batch_request: BatchRequest elements that should be used to obtain additional batch_ids
            metric_name: metric from which to build the parameters
            metric_value_kwargs: value kwargs for the metric to be built
            p_values: the p_values for which to return metric value estimates
        """
        super().__init__(
            parameter_id=parameter_id,
            data_context=data_context,
            batch_request=batch_request,
        )
        self._metric_name = metric_name
        self._metric_value_kwargs = metric_value_kwargs
        self._p_values = p_values

    def _build_parameters(self, *, rule_state, validator, batch_ids, **kwargs):
        samples = []
        for batch_id in batch_ids:
            metric_domain_kwargs = copy(rule_state.active_domain.domain_kwargs)
            metric_domain_kwargs.update({"batch_id": batch_id})

            if self._metric_value_kwargs.startswith("$"):
                metric_value_kwargs = rule_state.get_value(self._metric_value_kwargs)
            else:
                metric_value_kwargs = self._metric_value_kwargs

            samples.append(
                validator.get_metric(
                    MetricConfiguration(
                        self._metric_name, metric_domain_kwargs, metric_value_kwargs
                    )
                )
            )

        return {"parameters": samples[0]}
