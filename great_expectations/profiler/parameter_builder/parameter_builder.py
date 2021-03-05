from abc import ABC, abstractmethod


class ParameterBuilder(ABC):
    """
    A ParameterBuilder implementation provides support for building Expectation
    Configuration Parameters suitable for use in
    other ParameterBuilders or in ConfigurationBuilders as part of profiling.

    A ParameterBuilder is configured as part of a ProfilerRule. Its primary interface is the
    `build_parameters` method.

    As part of a ProfilerRule, the following configuration will create a new parameter
    for each domain returned by the domain_builder, with an associated id.

        ```
        parameter_builders:
          - id: mean
            class_name: MetricParameterBuilder
            metric_name: column.mean
            metric_domain_kwargs: $domain.domain_kwargs
        ```
    """

    def __init__(self, *, parameter_id: str, data_context=None):
        self._parameter_id = parameter_id
        self._data_context = data_context

    @property
    def parameter_id(self):
        return self._parameter_id

    def build_parameters(
        self, *, rule_state=None, validator=None, batch_ids=None, **kwargs
    ):
        """Build the parameters for the specified domain_kwargs."""
        return self._build_parameters(
            rule_state=rule_state, validator=validator, batch_ids=batch_ids, **kwargs
        )

    @abstractmethod
    def _build_parameters(self, *, rule_state, validator, batch_ids, **kwargs):
        pass
