from great_expectations.profiler.parameter_builder.multi_batch_parameter_builder import (
    MultiBatchParameterBuilder,
)
from great_expectations.validator.validation_graph import MetricConfiguration


class MultiBatchAggregateMetricParameterBuilder(MultiBatchParameterBuilder):
    """
    This ParameterBuilder takes a metric name and list of batches, and computes the aggregate metric over all batches by either: 1. merging them into one batch or 2. if the metric is linear computes it from single batch aggregate metrics / metadata. E.g. mean of means
    """

    pass
    # def __init__(
    #     self,
    #     parameter_name: str,
    #     validator: Validator,
    #     domain: Domain,
    #     batch_request: BatchRequest,
    #     metric_configuration: MetricConfiguration,
    #     rule_variables: Optional[ParameterContainer] = None,
    #     rule_domain_parameters: Optional[Dict[str, ParameterContainer]] = None,
    #     data_context: Optional[DataContext] = None,
    # ):
    #     super().__init__(
    #
    #     )
