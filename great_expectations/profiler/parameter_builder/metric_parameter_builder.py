from ..rule_state import RuleState
from ...validator.validation_graph import MetricConfiguration
from .parameter_builder import ParameterBuilder


class MetricParameterBuilder(ParameterBuilder):
    """Class utilized for obtaining a resolved metric (which is labeled a 'parameter') using domain kwargs, value
    kwargs, and a metric name"""
    def __init__(
        self,
        *,
        parameter_id,
        data_context=None,
        metric_name,
        metric_domain_kwargs="$domain.domain_kwargs",
        metric_value_kwargs=None
    ):
        super().__init__(parameter_id=parameter_id, data_context=data_context)
        self._metric_name = metric_name
        self._metric_domain_kwargs = metric_domain_kwargs
        self._metric_value_kwargs = metric_value_kwargs

    def _build_parameters(
        self, *, rule_state: RuleState = None, validator=None, batch_ids=None, **kwargs
    ):
        """
        Builds a dictionary of format {'parameters': A given resolved metric}
            Args:
            :param rule_state: An object keeping track of the state information necessary for rule validation, such as domain,
                    metric parameters, and necessary variables
            :param validator: A Validator object used to obtain metrics
        :return: a dictionary of format {'parameters': A given resolved metric}
        """
        # Obtaining any necessary domain kwargs from rule state, otherwise using instance var
        if self._metric_domain_kwargs.startswith("$"):
            metric_domain_kwargs = rule_state.get_value(self._metric_domain_kwargs)
        else:
            metric_domain_kwargs = self._metric_domain_kwargs

        # Obtaining any necessary value kwargs from rule state, otherwise using instance var
        if self._metric_value_kwargs.startswith("$"):
            metric_value_kwargs = rule_state.get_value(self._metric_value_kwargs)
        else:
            metric_value_kwargs = self._metric_value_kwargs

        # Building a parameter dictionary with single entry {"parameters": Metric},
        # feeding in a MetricConfiguration with obtained domain and value kwargs
        return {
            "parameters": validator.get_metric(
                MetricConfiguration(
                    self._metric_name, metric_domain_kwargs, metric_value_kwargs
                )
            )
        }
