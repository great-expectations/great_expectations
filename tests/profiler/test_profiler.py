import pytest

from great_expectations.profiler.profiler import Profiler


@pytest.fixture(scope="module")
def simple_multibatch_profiler_configuration_yaml():
    config = """
name: BasicSuiteBuilderProfiler
variables:
  alert_threshold: 0.01
rules:
  datetime:
    domain_builder:
      class_name: SimpleSemanticTypeColumnDomainBuilder
      type: datetime
    parameter_builders:
      - id: my_dateformat
        class_name: SimpleDateFormatStringParameterBuilder
        domain_kwargs: $domain.domain_kwargs
    configuration_builders:
        - expectation: expect_column_values_to_match_strftime_format
          column: $domain.domain_kwargs.column
          date_fmt: $my_dateformat.parameter.date_string
  numeric:
    class_name: SemanticTypeColumnDomainBuilder
    type: numeric
    parameter_builders:
      - id: quantile_ranges
        class_name: MultiBatchBootstrappedMetricDistributionParameterBuilder
        batch_request:
          partition_request:
            partition_index: "-10:"
        metric_configuration:
          metric_name: column.quantile_values
          metric_domain_kwargs: $domain.domain_kwargs
          metric_value_kwargs:
            quantiles:
              - 0.05
              - 0.25
              - 0.50
              - 0.75
              - 0.95
        p_values:
          min_value: ($alert_threshold / 2)
          max_value: 1 - ($alert_threshold / 2)
    configuration_builders:
      - expectation: expect_column_quantile_values_to_be_between
        value_ranges: $quantile_ranges
"""
    return config


def test_profiler_init_manual(
    taxicab_context, simple_multibatch_profiler_configuration_yaml
):
    profiler = Profiler(
        rule_configs=simple_multibatch_profiler_configuration_yaml,
        data_context=taxicab_context,
    )
    assert False


def test_profiler_rule_init_helper(
    taxicab_context, simple_multibatch_profiler_configuration_yaml
):
    assert False
