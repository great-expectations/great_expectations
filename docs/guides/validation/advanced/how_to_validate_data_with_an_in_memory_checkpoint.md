---
title: How to Validate data with an in-memory Checkpoint
---
import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'

This guide will demonstrate how to Validate data using a Checkpoint that is configured and run entirely in-memory.  This workflow is appropriate for environments or workflows where a user does not want to or cannot use a Checkpoint Store, e.g. in a [hosted environment](../../../deployment_patterns/how_to_instantiate_a_data_context_hosted_environments.md).


<Prerequisites>

- Have a Data Context
- Have an Expectation Suite
- Have a Datasource
- Have a basic understanding of Checkpoints

</Prerequisites>

:::note
Reading our guide on [Deploying Great Expectations in a hosted environment without file system or CLI](../../../deployment_patterns/how_to_instantiate_a_data_context_hosted_environments.md) is recommended for guidance on the setup, connecting to data, and creating expectations steps that take place prior to this process.
:::

## Steps

### 1. Import the necessary modules

The recommended method for creating a Checkpoint is to use the CLI to open a Jupyter Notebook which contains code scaffolding to assist you with the process.  Since that option is not available (this guide is assuming that your need for an in-memory Checkpoint is due to being unable to use the CLI or access a filesystem) you will have to provide that scaffolding yourself.

In the script that you are defining and executing your Checkpoint in, enter the following code:

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_with_a_python_configured_in_memory_checkpoint.py#L6-L8
```

Importing `great_expectations` will give you access to your Data Context, while we will configure an instance of the `Checkpoint` class as our in-memory Checkpoint.

If you are planning to use a YAML string to configure your in-memory Checkpoint you will also need to import `yaml` from `ruamel`:

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_with_a_yaml_configured_in_memory_checkpoint.py#L4-L5
```

You will also need to initialize `yaml.YAML(...)`:

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_with_a_yaml_configured_in_memory_checkpoint.py#L21
```

### 2. Initialize your Data Context

In the previous section you imported `great_expectations` in order to get access to your Data Context.  The line of code that does this is:

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_with_a_python_configured_in_memory_checkpoint.py#L26
```

Checkpoints require a Data Context in order to access necessary Stores from which to retrieve Expectation Suites and store Validation Results and Metrics, so you will pass `context` in as a parameter when you initialize your `Checkpoint` class later.

### 3. Define your Checkpoint configuration

In addition to a Data Context, you will need a configuration with which to initialize your Checkpoint.  This configuration can be in the form of a YAML string or a Python dictionary,  The following examples show configurations that are equivalent to the one used by the Getting Started Tutorial.

Normally, a Checkpoint configuration will include the keys `class_name` and `module_name`.  These are used by Great Expectations to identify the class of Checkpoint that should be initialized with a given configuration.  Since we are initializing an instance of the `Checkpoint` class directly we don't need the configuration to indicate the class of Checkpoint to be initialized.  Therefore, these two keys will be left out of our configuration.

<Tabs
  defaultValue="python_dict"
  values={[
    {label: 'Python Dictionary', value: 'python_dict'},
    {label: 'YAML String', value: 'yaml_str'},
  ]}>

  <TabItem value="python_dict">

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_with_a_python_configured_in_memory_checkpoint.py#L60-L90
```

  </TabItem>

  <TabItem value="yaml_str">

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_with_a_yaml_configured_in_memory_checkpoint.py#L61-L83
```

  </TabItem>

</Tabs>

When you are tailoring the configuration for your own purposes, you will want to replace the Batch Request and Expectation Suite under the `validations` key with your own values.  You can further edit the configuration to add additional Batch Request and Expectation Suite entries under the `validations` key.  Alternatively, you can even replace this configuration entirely and build one from scratch.  If you choose to build a configuration from scratch, or to further modify the examples provided above, you may wish to reference [our documentation on Checkpoint configurations](../../../terms/checkpoint.md#checkpoint-configuration) as you do. 

### 4. Initialize your Checkpoint 

Once you have your Data Context and Checkpoint configuration you will be able to initialize a `Checkpoint` instance in memory.  There is a minor variation in how you do so, depending on whether you are using a Python dictionary or a YAML string for your configuration.

<Tabs
  defaultValue="python_dict"
  values={[
    {label: 'Python Dictionary', value: 'python_dict'},
    {label: 'YAML String', value: 'yaml_str'},
  ]}>

  <TabItem value="python_dict">

If you are using a Python dictionary as your configuration, you will need to unpack it as parameters for the `Checkpoint` object's initialization.  This can be done with the code:

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_with_a_python_configured_in_memory_checkpoint.py#L96
```

  </TabItem>

  <TabItem value="yaml_str">

If you are using a YAML string as your configuration, you will need to convert it into a dictionary and unpack it as parameters for the `Checkpoint` object's initialization.  This can be done with the code: 

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_with_a_yaml_configured_in_memory_checkpoint.py#L89
```

  </TabItem>

</Tabs>

### 5. Run your Checkpoint

Congratulations!  You now have an initialized `Checkpoint` object in memory.  You can now use it's `run(...)` method to Validate your data as specified in the configuration.

This will be done with the line:

```python file=../../../../tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_with_a_yaml_configured_in_memory_checkpoint.py#L94
```

Congratulations!  Your script is now ready to be run.  Each time you run it, it will initialize and run a Checkpoint in memory, rather than retrieving a Checkpoint configuration from a Checkpoint Store.

### 6. Check your Data Docs

Once you have run your script you can verify that it has worked by checking your Data Docs for new results.

## Notes

To view the full example scripts used in this documentation, see:
- [how_to_validate_data_with_a_yaml_configured_in_memory_checkpoint.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_with_a_python_configured_in_memory_checkpoint.py)
- [how_to_validate_data_with_a_python_configured_in_memory_checkpoint.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/validation/checkpoints/how_to_validate_data_with_a_python_configured_in_memory_checkpoint.py)