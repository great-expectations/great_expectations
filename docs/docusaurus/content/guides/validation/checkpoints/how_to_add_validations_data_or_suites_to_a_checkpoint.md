---
title: How to add validations data or suites to a Checkpoint
---

import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you add validation data or <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> to an existing <TechnicalTag tag="checkpoint" text="Checkpoint" />. This is useful if you want to aggregate individual validations (across Expectation Suites or <TechnicalTag tag="datasource" text="Datasources" />) into a single Checkpoint.


<Prerequisites>

- [Configured a Data Context](../../../tutorials/getting_started/tutorial_setup.md#create-a-data-context)
- [Configured an Expectations Suite](../../../tutorials/getting_started/tutorial_create_expectations.md)
- [Configured a Checkpoint](./how_to_create_a_new_checkpoint.md)

</Prerequisites>

## Steps

### 1. Open your existing Checkpoint in a text editor

It will look similar to this:

```yaml
name: my_checkpoint
config_version: 1
class_name: Checkpoint
run_name_template: "%Y-%m-foo-bar-template-$VAR"
validations:
  - batch_request:
      datasource_name: my_datasource
      data_connector_name: my_data_connector
      data_asset_name: users
      data_connector_query:
        index: -1
    expectation_suite_name: users.warning
    action_list:
        - name: store_validation_result
          action:
            class_name: StoreValidationResultAction
        - name: store_evaluation_params
          action:
            class_name: StoreEvaluationParametersAction
        - name: update_data_docs
          action:
            class_name: UpdateDataDocsAction
    evaluation_parameters:
      param1: "$MY_PARAM"
      param2: 1 + "$OLD_PARAM"
    runtime_configuration:
      result_format:
        result_format: BASIC
        partial_unexpected_count: 20
```

### 2. Edit the existing Checkpoint configuration to add an Expectation Suite 

To add a second Expectation Suite (in this example we add ``users.error``) to your Checkpoint configuration, modify the file to add an additional `batch_request` key and corresponding information, including `evaluation_parameters`, `action_list`, `runtime_configuration`, and `expectation_suite_name`.  In fact, the simplest way to run a different Expectation Suite on the same <TechnicalTag tag="batch" text="Batch" /> of data is to make a copy of the original `batch_request` entry and then edit the `expectation_suite_name` value to correspond to a different Expectation Suite.  The resulting configuration will look like this:

```yaml
name: my_checkpoint
config_version: 1
class_name: Checkpoint
run_name_template: "%Y-%m-foo-bar-template-$VAR"
validations:
  - batch_request:
      datasource_name: my_datasource
      data_connector_name: my_data_connector
      data_asset_name: users
      data_connector_query:
        index: -1
    expectation_suite_name: users.warning
    action_list:
        - name: store_validation_result
          action:
            class_name: StoreValidationResultAction
        - name: store_evaluation_params
          action:
            class_name: StoreEvaluationParametersAction
        - name: update_data_docs
          action:
            class_name: UpdateDataDocsAction
    evaluation_parameters:
      param1: "$MY_PARAM"
      param2: 1 + "$OLD_PARAM"
    runtime_configuration:
      result_format:
        result_format: BASIC
        partial_unexpected_count: 20
  - batch_request:
      datasource_name: my_datasource
      data_connector_name: my_data_connector
      data_asset_name: users
      data_connector_query:
        index: -1
    expectation_suite_name: users.error
    action_list:
        - name: store_validation_result
          action:
            class_name: StoreValidationResultAction
        - name: store_evaluation_params
          action:
            class_name: StoreEvaluationParametersAction
        - name: update_data_docs
          action:
            class_name: UpdateDataDocsAction
    evaluation_parameters:
      param1: "$MY_PARAM"
      param2: 1 + "$OLD_PARAM"
    runtime_configuration:
      result_format:
        result_format: BASIC
        partial_unexpected_count: 20
```

### 3. Edit the existing Checkpoint configuration to add new validation data

In the above example, the entry we added with our Expectation Suite was paired with the same Batch of data as the original Expectation Suite.  However, you may also specify different <TechnicalTag tag="batch_request" text="Batch Requests" /> (and thus different Batches of data) when you add an Expectation Suite.  The flexibility of easily adding multiple Validations of Batches of data with different Expectation Suites and specific <TechnicalTag tag="action" text="Actions" /> can be demonstrated using the following example of a Checkpoint configuration file:

```yaml
name: my_fancy_checkpoint
config_version: 1
class_name: Checkpoint
run_name_template: "%Y-%m-foo-bar-template-$VAR"
expectation_suite_name: users.delivery
action_list:
    - name: store_validation_result
      action:
        class_name: StoreValidationResultAction
    - name: store_evaluation_params
      action:
        class_name: StoreEvaluationParametersAction
    - name: update_data_docs
      action:
        class_name: UpdateDataDocsAction
validations:
  - batch_request:
      datasource_name: my_datasource
      data_connector_name: my_data_connector
      data_asset_name: users
      data_connector_query:
        index: 0
    expectation_suite_name: users.warning
  - batch_request:
      datasource_name: my_datasource
      data_connector_name: my_special_data_connector
      data_asset_name: users
      data_connector_query:
        index: -1
    expectation_suite_name: users.error
  - batch_request:
      datasource_name: my_datasource
      data_connector_name: my_other_data_connector
      data_asset_name: users
      data_connector_query:
        batch_filter_parameters:
          name: Titanic
    action_list:
      - name: quarantine_failed_data
        action:
          class_name: CreateQuarantineData
      - name: advance_passed_data
        action:
          class_name: CreateQuarantineData
evaluation_parameters:
  param1: "$MY_PARAM"
  param2: 1 + "$OLD_PARAM"
runtime_configuration:
  result_format:
    result_format: BASIC
    partial_unexpected_count: 20
```

According to this configuration, the locally-specified Expectation Suite ``users.warning`` is run against the ``batch_request`` that employs ``my_data_connector`` with the results processed by the Actions specified in the top-level ``action_list``. Similarly, the locally-specified Expectation Suite ``users.error`` is run against the ``batch_request`` that employs ``my_special_data_connector`` with the results also processed by the actions specified in the top-level ``action_list``. In addition, the top-level Expectation Suite ``users.delivery`` is run against the ``batch_request`` that employs ``my_other_data_connector`` with the results processed by the union of actions in the locally-specified ``action_list`` and in the top-level ``action_list``.

Please see [How to configure a new Checkpoint using test_yaml_config](./how_to_configure_a_new_checkpoint_using_test_yaml_config.md) for additional Checkpoint configuration examples (including the convenient templating mechanism).


## Additional notes

:::tip
This is a good way to aggregate Validations in a complex pipeline. You could use this feature to **<TechnicalTag tag="validation" text="Validate" /> multiple source files before and after their ingestion into your data lake**.
:::

