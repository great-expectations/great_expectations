from ...validator.validation_graph import MetricConfiguration
from .parameter_builder import ParameterBuilder


class MetricParameterBuilder(ParameterBuilder):
    def __init__(
        self,
        *,
        parameter_id,
        data_context,
        metric_name,
        metric_domain_kwargs="$domain.domain_kwargs",
        metric_value_kwargs=None
    ):
        super().__init__(parameter_id=parameter_id, data_context=data_context)
        self._metric_name = metric_name
        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

    def _build_parameters(
        self, *, rule_state=None, validator=None, batch_ids=None, **kwargs
    ):
        if self._metric_domain_kwargs.startswith("$"):
            metric_domain_kwargs = rule_state.get_value(self._metric_domain_kwargs)
        else:
            metric_domain_kwargs = self._metric_domain_kwargs

        if self._metric_value_kwargs.startswith("$"):
            metric_value_kwargs = rule_state.get_value(self._metric_value_kwargs)
        else:
            metric_value_kwargs = self._metric_value_kwargs

        return {
            "parameters": validator.get_metric(
                MetricConfiguration(
                    self._metric_name, metric_domain_kwargs, metric_value_kwargs
                )
            )
        }
