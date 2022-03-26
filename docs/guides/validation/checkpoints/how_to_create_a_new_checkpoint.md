---
title: How to create a new Checkpoint
---
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you create a new <TechnicalTag tag="checkpoint" text="Checkpoint" />, which allows you to couple an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> with a data set to <TechnicalTag tag="validation" text="Validate" />.

Note: As of Great Expectations version 0.13.7, we have updated and improved the Checkpoints feature. You can continue to use your existing legacy Checkpoint workflows if you’re working with concepts from the Batch Kwargs (v2) API. If you’re using concepts from the BatchRequest (v3) API, please refer to the new Checkpoints guides.

## Steps for Checkpoints (>=0.13.12)

:::note Prerequisites: 

This how-to guide assumes you have already:

* [Set up a working deployment of Great Expectations](../../../tutorials/getting_started/intro.md)
* [Configured a Datasource using the BatchRequest (v3) API](../../../tutorials/getting_started/connect_to_data.md)
* [Created an Expectation Suite](../../../tutorials/getting_started/create_your_first_expectations.md)

:::

### 1. Use the CLI to open a Jupyter Notebook for creating a new Checkpoint

To assist you with creating Checkpoints, our <TechnicalTag tag="cli" text="CLI" /> has a convenience method that will open a Jupyter Notebook with all the scaffolding you need to easily configure and save your Checkpoint.  Simply run the following CLI command from your <TechnicalTag tag="data_context" text="Data Context" />:

````console
great_expectations checkpoint new my_checkpoint
````

### 2. Configure and save your Checkpoint in the provided Jupyter Notebook

The Jupyter Notebook which was opened in the previous step will guide you through the steps of creating a Checkpoint.  It will also include a default configuration that you can edit to suite your use case. The following sections of this document walk you through an example of how to do this.

### 3. Configuring a SimpleCheckpoint (Example)

#### 3a. Edit the configuration 

For this example, we’ll demonstrate using a basic Checkpoint configuration with the `SimpleCheckpoint` class, which takes care of some defaults. Replace all names such as `my_datasource` with the respective <TechnicalTag tag="datasource" text="Datasource" />, <TechnicalTag tag="data_connector" text="Data Connector" />, <TechnicalTag tag="data_asset" text="Data Asset" />, and Expectation Suite names you have configured in your `great_expectations.yml`.

````yaml
config = """
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
"""
````


This is the minimum required to configure a Checkpoint that will run the Expectation Suite `my_suite` against the Data Asset `MyDataAsset`. See [How to configure a new Checkpoint using test_yaml_config](./how_to_configure_a_new_checkpoint_using_test_yaml_config.md) for advanced configuration options.

#### 3b. Test your config using `context.test_yaml_config`

````python
context.test_yaml_config(yaml_config=config)
````

When executed, test_yaml_config will instantiate the component and run through a self_check procedure to verify that the component works as expected.

In the case of a Checkpoint, this means

1. validating the yaml configuration,
2. verifying that the Checkpoint class with the given configuration, if valid, can be instantiated, and
3. printing warnings in case certain parts of the configuration, while valid, may be incomplete and need to be better specified for a successful Checkpoint operation.

The output will look something like this:

````console
Attempting to instantiate class from config...
Instantiating as a SimpleCheckpoint, since class_name is SimpleCheckpoint
Successfully instantiated SimpleCheckpoint


Checkpoint class name: SimpleCheckpoint
````

If something about your configuration wasn’t set up correctly, `test_yaml_config` will raise an error.

#### 2c. Store your Checkpoint config

After you are satisfied with your configuration, save it by running the appropriate cells in the Jupyter Notebook.

#### 2d. (Optional) Check your stored Checkpoint config

If the <TechnicalTag tag="store" text="Store" /> backend of your <TechnicalTag tag="checkpoint_store" text="Checkpoint Store" /> is on the local filesystem, you can navigate to the `checkpoints` store directory that is configured in `great_expectations.yml` and find the configuration files corresponding to the Checkpoints you created.

#### 2e. (Optional) Test run the new Checkpoint and open Data Docs

Now that you have stored your Checkpoint configuration to the Store backend configured for the Checkpoint Configuration store of your Data Context, you can also test `context.run_checkpoint`, right within your Jupyter Notebook by running the appropriate cells.

Before running a Checkpoint, make sure that all classes and Expectation Suites referred to in the configuration exist.

When `run_checkpoint` returns, the `checkpoint_run_result` can then be checked for the value of the `success` field (all validations passed) and other information associated with running the specified <TechnicalTag tag="action" text="Actions" />.

For more advanced configurations of Checkpoints, please see [How to configure a new Checkpoint using test_yaml_config](../../../guides/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.md).

## Additional Resources

* [How to configure a new Checkpoint using test_yaml_config](../../../guides/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.md)
* [How to add validations data or suites to a Checkpoint](../../../guides/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint.md)
