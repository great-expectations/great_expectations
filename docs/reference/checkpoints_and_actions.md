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
           here: [Dividing data assets into Batches](./dividing_data_assets_into_batches.md))
        1. `expectation_suite_name`: the name of the Expectation Suite to validate the batch of data against
        1. `action_list`: a list of actions to perform after each batch is validated

    * #### Optional keys
        1. `name`: providing a name will allow referencing the validation inside the run by name (e.g. "
           user_table_validation")
        1. `evaluation_parameters`: used to define named parameters using Great
           Expectations [Evaluation Parameter syntax](./evaluation_parameters.md)
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

:::caution API note

If the use case calls for instantiating the Checkpoint explicitly, then it is crucial to ensure that only serializable
values are passed as arguments to the constructor.  Specifically, if `batch_request` is specified at any level of the
hierarchy of the Checkpoint configuration (at the top level and/or as part of the validators list structure), then no
runtime `batch_request` can contain `batch_data`, only a database query.  This is because `batch_data` is used to specify
dataframes (Pandas, Spark), which are not serializable (while database queries are plain text, which is serializable).

The proper mechanism for specifying non-serializable parameters is to pass them dynamically to the Checkpoint `run()`
method.  Hence, in a typical scenario, one would instantiate the Checkpoint class with serializable parameters only,
while specifying any non-serializable parameters, commonly dataframes, as arguments to the Checkpoint `run()` method.
:::

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

Below is an example of a `CheckpointResult` object which itself contains `ValidationResult`, `ExpectationSuiteValidationResult`, and `CheckpointConfig` objects.

#### Example CheckpointResult:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L104-L119
```

## Checkpoint configuration default and override behavior

<Tabs
  defaultValue='tab0'
  values={[
  {label: 'No nesting', value: 'tab0'},
  {label: 'Nesting with defaults', value: 'tab1'},
  {label: 'Keys passed at runtime', value: 'tab2'},
  {label: 'Using template', value: 'tab3'},
  {label: 'Using SimpleCheckpoint', value: 'tab4'}
]}>

<TabItem value="tab0">
This configuration specifies full validation dictionaries - no nesting (defaults) are used. When run, this Checkpoint will perform one validation of a single batch of data, against a single Expectation Suite ("my_expectation_suite").

**YAML**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L150-L176
```

**runtime**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L181
```

</TabItem>
<TabItem value="tab1">
This configuration specifies four top-level keys ("expectation_suite_name", "action_list", "evaluation_parameters", and "runtime_configuration") that can serve as defaults for each validation, allowing the keys to be omitted from the validation dictionaries. When run, this Checkpoint will perform two validations of two different batches of data, both against the same Expectation Suite ("my_expectation_suite"). Each validation will trigger the same set of actions and use the same evaluation parameters and runtime configuration.

**YAML**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L199-L229
```

**Runtime**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L234
```

**Results**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L239-L249
```

```console file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L258-L268
```

</TabItem>
<TabItem value="tab2">
This configuration omits the "validations" key from the YAML, which means a "validations " list must be provided when the Checkpoint is run. Because "action_list", "evaluation_parameters", and "runtime_configuration" appear as top-level keys in the YAML configuration, these keys may be omitted from the validation dictionaries, unless a non-default value is desired. When run, this Checkpoint will perform two validations of two different batches of data, with each batch of data validated against a different Expectation Suite ("my_expectation_suite" and "my_other_expectation_suite", respectively). Each validation will trigger the same set of actions and use the same evaluation parameters and runtime configuration.

**YAML**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L275-L295
```

**Runtime**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L301-L321
```

**Results**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L325-L335
```

</TabItem>
<TabItem value="tab3">
This configuration references the Checkpoint detailed in the previous example ("Keys passed at runtime"), allowing the runtime call to be much slimmer.

**YAML**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L347-L362
```

**Runtime**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L367
```

**Results**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L370-L380
```

</TabItem>
<TabItem value="tab4">
This configuration specifies the SimpleCheckpoint class under the "class_name" key, allowing for a much slimmer configuration.

**YAML, using SimpleCheckpoint**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L389-L401
```

**Runtime**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L409
```


**Equivalent YAML, using Checkpoint**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L415-L442
```

**Runtime**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L450
```

</TabItem>
</Tabs>

### Additional Notes
To view the full script used in this page, see it on GitHub:
- [checkpoints_and_actions.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py)
