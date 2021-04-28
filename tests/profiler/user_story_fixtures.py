import pytest


@pytest.fixture(scope="module")
def alice_columnar_table_single_batch():
    config = """
variables:
  max_user_id: 999999999999
  min_timestamp: 2004-10-19 10:23:54
rules:
  my_rule_for_user_ids:
    domain_builder:
      class_name: SimpleSemanticTypeColumnDomainBuilder
      semantic_types:
        - user_id
    parameter_builders:
      - name: my_min_user_id
        class_name: MetricParameterBuilder
        metric_name: column.min
    expectation_configuration_builders:
      - expectation: expect_column_values_to_be_between
        min_value: $my_min_user_id
        max_value: $max_user_id
      - expectation: expect_column_values_to_not_be_null
      - expectation: expect_column_values_to_be_of_type
        type: INTEGER
  my_rule_for_timestamps:
    domain_builder:
      class_name: SimpleColumnSuffixDomainBuilder
      column_name_suffixes:
        - _ts
      user_input_list_of_domain_names:
        - event_ts
        - server_ts
        - device_ts
    parameter_builders:
      - name: my_max_event_ts
        class_name: MetricParameterBuilder
        metric_name: column.max
        metric_domain_kwargs: $event_ts.domain_kwargs
    expectation_configuration_builders:
      - expectation: expect_column_values_to_be_of_type
        type: TIMESTAMP
      - expectation: expect_column_values_to_be_increasing
      - expectation: expect_column_values_to_be_dateutil_parseable
      - expectation: expect_column_min_to_be_between
        min_value: $min_timestamp
        max_value: $min_timestamp
        meta:
          notes:
            format: markdown
            content:
              - ### This expectation confirms no events occur before tracking started **2004-10-19 10:23:54**
      - expectation: expect_column_max_to_be_between
        min_value: $min_timestamp
        max_value: $my_max_event_ts
        meta:
          notes:
            format: markdown
            content:
              - ### This expectation confirms that the event_ts contains the latest timestamp of all domains
"""
    return config
