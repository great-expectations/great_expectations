---
id: checkpoint
title: Checkpoint
hoverText: The primary means for validating data in a production deployment of Great Expectations.
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import Tabs from '@theme/Tabs'; import TabItem from '@theme/TabItem';

<UniversalMap setup='inactive' connect='inactive' create='inactive' validate='active'/>

## Overview

### Definition

A Checkpoint is the primary means for validating data in a production deployment of Great Expectations.

### Features and promises

Checkpoints provide a convenient abstraction for bundling the <TechnicalTag relative="../" tag="validation" text="Validation" /> of a <TechnicalTag relative="../" tag="batch" text="Batch (or Batches)" /> of data against an <TechnicalTag relative="../" tag="expectation_suite" text="Expectation Suite" /> (or several), as well as the <TechnicalTag relative="../" tag="action" text="Actions" /> that should be taken after the validation. 

Like Expectation Suites and <TechnicalTag relative="../" tag="validation_result" text="Validation Results" />, Checkpoints are managed using a <TechnicalTag relative="../" tag="data_context" text="Data Context" />, and have their own Store which is used to persist their configurations to YAML files. These configurations can be committed to version control and shared with your team.

### Relationships to other objects

![How a Checkpoint works](../images/universal_map/overviews/how_a_checkpoint_works.png)

A Checkpoint uses a <TechnicalTag relative="../" tag="validator" text="Validator" /> to run one or more Expectation Suites against one or more Batches provided by one or more <TechnicalTag relative="../" tag="batch_request" text="Batch Requests" />. Running a Checkpoint produces Validation Results and will result in optional Actions being performed if they are configured to do so.

## Use cases

<ValidateHeader/>

In the Validate Data step of working with Great Expectations, there are two points in which you will interact with Checkpoints in different ways: First, when you create them.  And secondly, when you use them to actually Validate your data.

### Creating Checkpoints

As part of the initial setup for the Validate Data step, you will create your Checkpoints.  Checkpoints are reusable and thus only need to be created once, although you can create multiple Checkpoints to cover multiple Validation use cases.

For an in-depth guide on Checkpoint creation, see our [guide on how to create a new Checkpoint](../guides/validation/checkpoints/how_to_create_a_new_checkpoint.md).

### Running Checkpoints

Once you have created a Checkpoint, you will be able to use it to Validate data by running it against a Batch or Batches of data.  The Batch Requests used by a Checkpoint during this process may be pre-defined and saved as part of the Checkpoint's configuration, or the Checkpoint can be configured to accept one or more Batch Request at run time.

For an in-depth guide on this process, please see our guide on [how to validate data by running a Checkpoint](../guides/validation/how_to_validate_data_by_running_a_checkpoint.md).

## Features

### Reusable

You do not need to re-create a Checkpoint every time you Validate data.  If you have created a Checkpoint that covers your data Validation needs, you can save and re-use it for your future Validation needs.  Since you can set Checkpoints up to receive some of their required information (like Batch Requests) at run time, it is easy to create Checkpoints that can be readily applied to multiple disparate sources of data.

### Actions

One of the most powerful features of Checkpoints is that they can be configured to run Actions, which will do some process based on the Validation Results generated when a Checkpoint is run.  Typical uses include sending email, slack, or custom notifications.  Another common use case is updating Data Docs sites.  However, Actions can be created to do anything you are capable of programing in Python.  This gives you an incredibly versatile tool for integrating Checkpoints in your pipeline's workflow!

For in-depth examples of how to set up common Action use cases, please see our [how-to guides for Actions](../guides/validation/index.md#validation-actions).

## API basics

The classes that implement Checkpoints are in the `great_expectations.checkpoint` module.

### Creating a Checkpoint

The simplest way to create a Checkpoint is from the CLI.  The following command will, when **run in the terminal from the root folder of your Data Context**, present you with a Jupyter Notebook which will guide you through the steps of creating a Checkpoint:

```markdown Title: "Terminal command"
great_expectations checkpoint new my_checkpoint
```

For an in-depth guide on Checkpoint creation, see our [guide on how to create a new Checkpoint](../guides/validation/checkpoints/how_to_create_a_new_checkpoint.md).

### Checkpoint configuration

A Checkpoint uses its configuration to determine what data to Validate against which Expectation Suite(s), and what actions to perform on the Validation Results - these validations and Actions are executed by calling a Checkpoint's `run` method (analogous to calling `validate` with a single Batch). Checkpoint configurations are very flexible. At one end of the spectrum, you can specify a complete configuration in a Checkpoint's YAML file, and simply call `my_checkpoint.run()`. At the other end, you can specify a minimal configuration in the YAML file and provide missing keys as kwargs when calling `run`.

At runtime, a Checkpoint configuration has three required and three optional keys, and is built using a combination of the YAML configuration and any kwargs passed in at runtime:

#### Required keys

1. `name`: user-selected Checkpoint name (e.g. "staging_tables")
1. `config_version`: version number of the Checkpoint configuration
1. `validations`: a list of dictionaries that describe each validation that is to be executed, including any actions.
   Each validation dictionary has three required and three optional keys:
    * #### Required keys
        1. `batch_request`: a dictionary describing the batch of data to validate (learn more about specifying Batches
           here: [Dividing data assets into Batches](../reference/dividing_data_assets_into_batches.md))
        1. `expectation_suite_name`: the name of the Expectation Suite to validate the batch of data against
        1. `action_list`: a list of actions to perform after each batch is validated

    * #### Optional keys
        1. `name`: providing a name will allow referencing the validation inside the run by name (e.g. "
           user_table_validation")
        1. `evaluation_parameters`: used to define named parameters using Great
           Expectations [Evaluation Parameter syntax](../reference/evaluation_parameters.md)
        1. `runtime_configuration`: provided to the Validator's `runtime_configuration` (e.g. `result_format`)

#### Optional keys

1. `class_name`: the class of the Checkpoint to be instantiated, defaults to `Checkpoint`
1. `template_name`: the name of another Checkpoint to use as a base template
1. `run_name_template`: a template to create run names, using environment variables and datetime-template syntax (e.g. "%Y-%M-staging-$MY_ENV_VAR")

#### Configuration defaults and parameter override behavior

Checkpoint configurations follow a nested pattern, where more general keys provide defaults for more specific ones. For instance, any required validation dictionary keys (e.g. `expectation_suite_name`) can be specified at the top-level (i.e. at the same level as the validations list), serving as runtime defaults. Starting at the earliest reference template, if a configuration key is re-specified, its value can be appended, updated, replaced, or cause an error when redefined.

##### Replaced

* `name`
* `module_name`
* `class_name`
* `run_name_template`
* `expectation_suite_name`

##### Updated

* `batch_request`: at runtime, if a key is re-defined, an error will be thrown
* `action_list`: actions that share the same user-defined name will be updated, otherwise a new action will be appended
* `evaluation_parameters`
* `runtime_configuration`

##### Appended

* `action_list`: actions that share the same user-defined name will be updated, otherwise a new action will be appended
* `validations`


#### Checkpoint configuration default and override behavior

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
This configuration specifies four top-level keys ("expectation_suite_name", "action_list", "evaluation_parameters", and "runtime_configuration") that can serve as defaults for each validation, allowing the keys to be omitted from the validation dictionaries. When run, this Checkpoint will perform two Validations of two different Batches of data, both against the same Expectation Suite ("my_expectation_suite"). Each Validation will trigger the same set of Actions and use the same <TechnicalTag relative="../" tag="evaluation_parameter" text="Evaluation Parameters" /> and runtime configuration.

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
This configuration omits the "validations" key from the YAML, which means a "validations " list must be provided when the Checkpoint is run. Because "action_list", "evaluation_parameters", and "runtime_configuration" appear as top-level keys in the YAML configuration, these keys may be omitted from the validation dictionaries, unless a non-default value is desired. When run, this Checkpoint will perform two validations of two different batches of data, with each batch of data validated against a different Expectation Suite ("my_expectation_suite" and "my_other_expectation_suite", respectively). Each Validation will trigger the same set of actions and use the same <TechnicalTag relative="../" tag="evaluation_parameter" text="Evaluation Parameters" /> and runtime configuration.

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

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L334-L348
```

**Runtime**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L351
```

**Results**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L353-L363
```

```console file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L369-L379
```

</TabItem>
<TabItem value="tab4">
This configuration specifies the SimpleCheckpoint class under the "class_name" key, allowing for a much slimmer configuration.

**YAML, using SimpleCheckpoint**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L383-L395
```

**Equivalent YAML, using Checkpoint**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L405-L432
```

**Runtime**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L438
```

**Results**:

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L440-L443
```

```console file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L447-L451
```

</TabItem>
</Tabs>

## More details

### SimpleCheckpoint class

For many use cases, the SimpleCheckpoint class can be used to simplify the process of specifying a Checkpoint configuration. SimpleCheckpoint provides a basic set of actions - store Validation Result, store <TechnicalTag relative="../" tag="evaluation_parameter" text="Evaluation Parameters" />, update <TechnicalTag relative="../" tag="data_docs" text="Data Docs" />, and optionally, send a Slack notification - allowing you to omit an `action_list` from your configuration and at runtime.

Configurations using the SimpleCheckpoint class can optionally specify four additional top-level keys that customize and extend the basic set of default actions:

* `site_names`: a list of Data Docs site names to update as part of the update Data Docs action - defaults to "all"
* `slack_webhook`: if provided, an action will be added that sends a Slack notification to the provided webhook
* `notify_on`: used to define when a notification is fired, according to Validation Result outcome - `all`, `failure`, or `success`. Defaults to `all`.
* `notify_with`: a list of Data Docs site names for which to include a URL in any notifications - defaults to `all`

### CheckpointResult

The return object of a Checkpoint run is a CheckpointResult object. The `run_results` attribute forms the backbone of this type and defines the basic contract for what a Checkpoint's `run` method returns. It is a dictionary where the top-level keys are the ValidationResultIdentifiers of the Validation Results generated in the run. Each value is a dictionary having at minimum, a `validation_result` key containing an ExpectationSuiteValidationResult and an `actions_results` key containing a dictionary where the top-level keys are names of Actions performed after that particular Validation, with values containing any relevant outputs of that action (at minimum and in many cases, this would just be a dictionary with the Action's `class_name`).

The `run_results` dictionary can contain other keys that are relevant for a specific Checkpoint implementation. For example, the `run_results` dictionary from a WarningAndFailureExpectationSuiteCheckpoint might have an extra key named "expectation_suite_severity_level" to indicate if the suite is at either a "warning" or "failure" level.

CheckpointResult objects include many convenience methods (e.g. `list_data_asset_names`) that make working with Checkpoint results easier. You can learn more about these methods in the documentation for class: `great_expectations.checkpoint.types.checkpoint_result.CheckpointResult`.

Below is an example of a `CheckpointResult` object which itself contains `ValidationResult`, `ExpectationSuiteValidationResult`, and `CheckpointConfig` objects.

#### Example CheckpointResult

```python file=../../tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py#L101-L115
```

### Example script

To view the full script used in this page, see it on GitHub:
- [checkpoints_and_actions.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/reference/core_concepts/checkpoints_and_actions.py)
