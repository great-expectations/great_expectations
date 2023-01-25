from docs.sphinx_api_docs_source.conf import convert_code_blocks


def test_convert_code_blocks():
    name = "great_expectations.checkpoint.StoreMetricsAction"
    lines = [
        "Extract metrics from a Validation Result and store them in a metrics store.",
        "",
        "Typical usage example:",
        "    ```yaml",
        "    - name: store_evaluation_params",
        "    action:",
        "     class_name: StoreMetricsAction",
        "      # the name must refer to a store that is configured in the great_expectations.yml file",
        "      target_store_name: my_metrics_store",
        "    ```",
        "",
        ":param data_context: GX Data Context.",
        ":param requested_metrics: Dictionary of metrics to store.",
        "",
        "                          Dictionary should have the following structure:",
        "                                  ```yaml",
        "                                  expectation_suite_name:",
        "                                      metric_name:",
        "                                          - metric_kwargs_id",
        "                                  ```",
        '                          You may use "*" to denote that any expectation suite should match.',
        ":param target_store_name: The name of the store where the action will store the metrics.",
        "",
        ":raises DataContextError: Unable to find store {} in your DataContext configuration.",
        ":raises DataContextError: StoreMetricsAction must have a valid MetricsStore for its target store.",
        ":raises TypeError: validation_result_id must be of type ValidationResultIdentifier or GeCloudIdentifier, not {}.",
        "",
    ]

    convert_code_blocks(lines=lines, name=name)

    assert lines == [
        "Extract metrics from a Validation Result and store them in a metrics store.",
        "",
        "Typical usage example:",
        '<CodeBlock language="yaml">{`{    - name: store_evaluation_params\n'
        "    action:\n"
        "     class_name: StoreMetricsAction\n"
        "      # the name must refer to a store that is configured in the "
        "great_expectations.yml file\n"
        "      target_store_name: my_metrics_store}`}</CodeBlock>",
        "",
        ":param data_context: GX Data Context.",
        ":param requested_metrics: Dictionary of metrics to store.",
        "",
        "                          Dictionary should have the following structure:",
        '<CodeBlock language="yaml">{`{                                  '
        "expectation_suite_name:\n"
        "                                      metric_name:\n"
        "                                          - metric_kwargs_id}`}</CodeBlock>",
        '                          You may use "*" to denote that any expectation suite should match.',
        ":param target_store_name: The name of the store where the action will store the metrics.",
        "",
        ":raises DataContextError: Unable to find store {} in your DataContext configuration.",
        ":raises DataContextError: StoreMetricsAction must have a valid MetricsStore for its target store.",
        ":raises TypeError: validation_result_id must be of type ValidationResultIdentifier or GeCloudIdentifier, not {}.",
        "",
    ]
