---
title: Checkpoints and Actions
---


import Tabs from '@theme/Tabs'; import TabItem from '@theme/TabItem';

## Introduction

:::caution API note

As part of the new modular expectations API in Great Expectations, Validation Operators are evolving into
Checkpoints. At some point in the future Validation Operators will be fully deprecated.
:::

The `batch.validate()` method evaluates one Batch of data against one Expectation Suite and returns a dictionary of
Validation Results. This is sufficient when you explore your data and get to know Great Expectations. When deploying
Great Expectations in a real data pipeline, you will typically discover additional needs:

* Validating a group of Batches that are logically related (for example, a Checkpoint for all staging tables).
* Validating a Batch against several Expectation Suites (for example, run three suites to protect a machine learning
  model `churn.critical`, `churn.warning`, `churn.drift`).
* Doing something with the Validation Results (for example, saving them for later review, sending notifications in case
  of failures, etc.).

Checkpoints provide a convenient abstraction for bundling the validation of a Batch (or Batches) of data against an
Expectation Suite (or several), as well as the actions that should be taken after the validation. Like Expectation
Suites and Validation Results, Checkpoints are managed using a Data Context, and have their own Store which is used to
persist their configurations to YAML files. These configurations can be committed to version control and shared with
your team.

The classes that implement Checkpoints are in the `great_expectations.checkpoint` module.

## Validation Actions

Actions are Python classes with a `run` method that takes the result of validating a Batch against an Expectation Suite
and does something with it (e.g., save Validation Results to disk, or send a Slack notification). Classes that implement
this API can be configured to be added to the list of actions used by a particular Checkpoint.

Classes that implement Actions can be found in the `great_expectations.checkpoint.actions` module.

## Checkpoint configuration

A Checkpoint uses its configuration to determine what data to validate against which Expectation Suite(s), and what
actions to perform on the Validation Results - these validations and actions are executed by calling a
Checkpoint's `run` method (analogous to calling `validate` with a single Batch). Checkpoint configurations are very
flexible. At one end of the spectrum, you can specify a complete configuration in a Checkpoint's YAML file, and simply
call `my_checkpoint.run()`. At the other end, you can specify a minimal configuration in the YAML file and provide
missing keys as kwargs when calling `run`.

At runtime, a Checkpoint configuration has three required and three optional keys, and is built using a combination of
the YAML configuration and any kwargs passed in at runtime:

#### Required keys

1. `name`: user-selected Checkpoint name (e.g. "staging_tables")
1. `config_version`: version number of the Checkpoint configuration
1. `validations`: a list of dictionaries that describe each validation that is to be executed, including any actions.
   Each validation dictionary has three required and three optional keys:
    * #### Required keys
        1. `batch_request`: a dictionary describing the batch of data to validate (learn more about specifying Batches
           here: [Dividing data assets into Batches](./dividing_data_assets_into_batches))
        1. `expectation_suite_name`: the name of the Expectation Suite to validate the batch of data against
        1. `action_list`: a list of actions to perform after each batch is validated

    * #### Optional keys
        1. `name`: providing a name will allow referencing the validation inside the run by name (e.g. "
           user_table_validation")
        1. `evaluation_parameters`: used to define named parameters using Great
           Expectations [Evaluation Parameter syntax](./evaluation_parameters)
        1. `runtime_configuration`: provided to the Validator's `runtime_configuration` (e.g. `result_format`)

#### Optional keys

1. `class_name`: the class of the Checkpoint to be instantiated, defaults to `Checkpoint`
1. `template_name`: the name of another Checkpoint to use as a base template
1. `run_name_template`: a template to create run names, using environment variables and datetime-template syntax (e.g. "
   %Y-%M-staging-$MY_ENV_VAR")

### Configuration defaults and parameter override behavior

Checkpoint configurations follow a nested pattern, where more general keys provide defaults for more specific ones. For
instance, any required validation dictionary keys (e.g. `expectation_suite_name`) can be specified at the top-level (
i.e. at the same level as the validations list), serving as runtime defaults. Starting at the earliest reference
template, if a configuration key is re-specified, its value can be appended, updated, replaced, or cause an error when
redefined.

#### Replaced

* `name`
* `module_name`
* `class_name`
* `run_name_template`
* `expectation_suite_name`

#### Updated

* `batch_request`: at runtime, if a key is re-defined, an error will be thrown
* `action_list`: actions that share the same user-defined name will be updated, otherwise a new action will be appended
* `evaluation_parameters`
* `runtime_configuration`

#### Appended

* `action_list`: actions that share the same user-defined name will be updated, otherwise a new action will be appended
* `validations`

## SimpleCheckpoint class

For many use cases, the SimpleCheckpoint class can be used to simplify the process of specifying a Checkpoint
configuration. SimpleCheckpoint provides a basic set of actions - store Validation Result, store evaluation parameters,
update Data Docs, and optionally, send a Slack notification - allowing you to omit an `action_list` from your
configuration and at runtime.

Configurations using the SimpleCheckpoint class can optionally specify four additional top-level keys that customize and
extend the basic set of default actions:

* `site_names`: a list of Data Docs site names to update as part of the update Data Docs action - defaults to "all"
* `slack_webhook`: if provided, an action will be added that sends a Slack notification to the provided webhook
* `notify_on`: used to define when a notification is fired, according to Validation Result outcome - `all`, `failure`,
  or `success`. Defaults to `all`.
* `notify_with`: a list of Data Docs site names for which to include a URL in any notifications - defaults to `all`

## CheckpointResult

The return object of a Checkpoint run is a CheckpointResult object. The `run_results` attribute forms the backbone of
this type and defines the basic contract for what a Checkpoint's `run` method returns. It is a dictionary where the
top-level keys are the ValidationResultIdentifiers of the Validation Results generated in the run. Each value is a
dictionary having at minimum, a `validation_result` key containing an ExpectationSuiteValidationResult and
an `actions_results` key containing a dictionary where the top-level keys are names of actions performed after that
particular validation, with values containing any relevant outputs of that action (at minimum and in many cases, this
would just be a dictionary with the action's `class_name`).

The `run_results` dictionary can contain other keys that are relevant for a specific Checkpoint implementation. For
example, the `run_results` dictionary from a WarningAndFailureExpectationSuiteCheckpoint might have an extra key named "
expectation_suite_severity_level" to indicate if the suite is at either a "warning" or "failure" level.

CheckpointResult objects include many convenience methods (e.g. `list_data_asset_names`) that make working with
Checkpoint results easier. You can learn more about these methods in the documentation for class: `great_expectations.checkpoint.types.checkpoint_result.CheckpointResult`.

#### Example CheckpointResult:

```python file=file=../../../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L105-L119
```

## Checkpoint configuration examples

<Tabs defaultValue="tab0"
values={[
{label: 'No nesting', value: 'tab0'}, {label: 'Nesting with defaults', value: 'tab1'}, {label: 'Keys passed at runtime', value: 'tab2'}, {label: 'Using template', value: 'tab3'}, {label: 'Using SimpleCheckpoint', value: 'tab4'}
]}>

<TabItem value="tab0">
This configuration specifies full validation dictionaries - no nesting (defaults) are used. When run, this Checkpoint will perform one validation of a single batch of data, against a single Expectation Suite ("users.delivery").

#### YAML:

  ```yaml
  name: my_fancy_checkpoint
  config_version: 1
  class_name: Checkpoint
  run_name_template: %Y-%M-foo-bar-template-$VAR
  validations:
    - batch_request:
        datasource_name: my_datasource
        data_connector_name: my_special_data_connector
        data_asset_name: users
        data_connector_query:
          index: -1
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
      evaluation_parameters:
        param1: $MY_PARAM
        param2: 1 + $OLD_PARAM
      runtime_configuration:
        result_format:
          result_format: BASIC
          partial_unexpected_count: 20
  ```

#### runtime:

  ```python
  context.run_checkpoint(
    checkpoint_name="my_fancy_checkpoint"
)
  ```

  </TabItem>
  <TabItem value="tab1">
  This configuration specifies four top-level keys (`expectation_suite_name`, `action_list`, `evaluation_parameters`, and `runtime_configuration`) that can serve as defaults for each validation, allowing the keys to be omitted from the validation dictionaries. When run, this Checkpoint will perform two validations of two different batches of data, both against the same Expectation Suite ("users.delivery"). Each validation will trigger the same set of actions and use the same evaluation parameters and runtime configuration.

#### YAML:

  ```yaml
  name: my_fancy_checkpoint
  config_version: 1
  class_name: Checkpoint
  run_name_template: %Y-%M-foo-bar-template-$VAR
  validations:
    - batch_request:
        datasource_name: my_datasource
        data_connector_name: my_special_data_connector
        data_asset_name: users
        data_connector_query:
          index: -1
    - batch_request:
        datasource_name: my_datasource
        data_connector_name: my_other_data_connector
        data_asset_name: users
        data_connector_query:
          index: -2
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
  evaluation_parameters:
    param1: $MY_PARAM
    param2: 1 + $OLD_PARAM
  runtime_configuration:
    result_format:
      result_format: BASIC
      partial_unexpected_count: 20
  ```

#### runtime:

  ```python
  context.run_checkpoint(
    checkpoint_name="my_fancy_checkpoint"
)
  ```

  </TabItem>
  <TabItem value="tab2">
This configuration omits the `validations` key from the YAML, which means a `validations` list must be provided when the Checkpoint is run. Because `action_list`, `evaluation_parameters`, and `runtime_configuration` appear as top-level keys in the YAML configuration, these keys may be omitted from the validation dictionaries, unless a non-default value is desired. When run, this Checkpoint will perform two validations of two different batches of data, with each batch of data validated against a different Expectation Suite ("users.delivery" and "users.diagnostic", respectively). Each validation will trigger the same set of actions and use the same evaluation parameters and runtime configuration.

#### YAML:

  ```yaml
  name: my_base_checkpoint
  config_version: 1
  class_name: Checkpoint
  run_name_template: %Y-%M-foo-bar-template-$VAR
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
    param1: $MY_PARAM
    param2: 1 + $OLD_PARAM
  runtime_configuration:
    result_format:
      result_format: BASIC
      partial_unexpected_count: 20
  ```

#### runtime:

  ```python
  context.run_checkpoint(
    checkpoint_name="my_base_checkpoint",
    validations=[
        {
            "batch_request": {
                "datasource_name": "my_datasource"
                                   "data_connector_name": "my_special_data_connector"
                                                          "data_asset_name": "users"
                                                                             "data_connector_query": {
    "index": -1
}
},
"expectation_suite_name": "users.delivery"
},
{
    "batch_request": {
        "datasource_name": "my_datasource"
                           "data_connector_name": "my_other_data_connector"
                                                  "data_asset_name": "users"
                                                                     "data_connector_query": {
    "index": -2
}
},
"expectation_suite_name": "users.diagnostic"
}
]
)
  ```

  </TabItem>
  <TabItem value="tab3">
  This configuration references the Checkpoint detailed in the previous example ("Keys passed at runtime"), allowing the runtime call to `run_checkpoint` to be much slimmer.

#### YAML:

  ```yaml
  name: my_fancy_checkpoint
  config_version: 1
  class_name: Checkpoint
  template_name: my_base_checkpoint
  validations:
    - batch_request:
        datasource_name: my_datasource
        data_connector_name: my_special_data_connector
        data_asset_name: users
        data_connector_query:
          index: -1
      expectation_suite_name: users.delivery
    - batch_request:
        datasource_name: my_datasource
        data_connector_name: my_other_data_connector
        data_asset_name: users
        data_connector_query:
          index: -2
      expectation_suite_name: users.diagnostic
  ```

#### runtime:

  ```python
  # Same as the parameterized run of previous "my_base_checkpoint" example
context.run_checkpoint(
    checkpoint_name="my_fancy_checkpoint"
)
  ```

  </TabItem>
  <TabItem value="tab4">
  This configuration specifies the SimpleCheckpoint class under the `class_name` key, allowing for a much slimmer configuration.

#### YAML, using SimpleCheckpoint:

  ```yaml
  name: my_checkpoint
  config_version: 1
  class_name: SimpleCheckpoint
  validations:
    - batch_request:
        datasource_name: my_datasource
        data_connector_name: my_data_connector
        data_asset_name: MyDataAsset
        data_connector_query:
          index: -1
      expectation_suite_name: my_suite
  site_names:
    - my_diagnostic_data_docs_site
  slack_webhook: http://my_slack_webhook.com
  notify_on: failure
  notify_with:
    - my_diagnostic_data_docs_site
  ```

#### Equivalent YAML, using Checkpoint:

  ```yaml
  name: my_checkpoint
  config_version: 1
  class_name: Checkpoint
  validations:
    - batch_request:
        datasource_name: my_datasource
        data_connector_name: my_data_connector
        data_asset_name: MyDataAsset
        data_connector_query:
          index: -1
      expectation_suite_name: my_suite
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
        site_names:
          - my_diagnostic_data_docs_site
    - name: send_slack_notification
      action:
        class_name: SlackNotificationAction
        slack_webhook: http://my_slack_webhook.com
        notify_on: failure
        notify_with:
          - my_diagnostic_data_docs_site
        renderer:
          module_name: great_expectations.render.renderer.slack_renderer
          class_name: SlackRenderer
  ```

#### runtime:

  ```python
  context.run_checkpoint(
    checkpoint_name="my_checkpoint"
)
  ```

  </TabItem>
</Tabs>
