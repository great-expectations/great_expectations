---
title: Configure a new Checkpoint using test_yaml_config
---
import Prerequsities from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

Use the information provided here to learn how to configure a <TechnicalTag tag="checkpoint" text="Checkpoint" /> using `test_yaml_config`. To create a new Checkpoint,  see [How to create a new Checkpoint](../../../guides/validation/checkpoints/how_to_create_a_new_checkpoint.md).

`test_yaml_config` lets you configure and test the components of a Great Expectations deployment, including <TechnicalTag tag="datasource" text="Datasources" />, <TechnicalTag tag="store" text="Stores" />, and Checkpoints. `test_yaml_config` is intended for use within a Jupyter notebook, where you can use an edit-run-check loop to quickly test your changes before implementation.

## Prerequisites

<Prerequisites>

- [Set up a working deployment of Great Expectations](/docs/guides/setup/setup_overview)
- [Connected to Data](/docs/guides/connecting_to_your_data/connect_to_data_overview)
- [Created an Expectation Suite](/docs/guides/expectations/create_expectations_overview)

</Prerequisites>

## Overview

`test_yaml_config` supports iteratively testing configuration to help zeroing in on the Checkpoint configuration you want. As an iterative workflow, it is particularly well suited to notebooks.

### 1. Setup
Run the following command in the first cell to load the necessary modules and initialize your <TechnicalTag tag="data_context" text="Data Context"/>:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.py setup"
```

### 2. List Assets
Your Checkpoint configuration includes Data Assets and Expectation Suites. Run the following command to list the available asset names:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.py asset_names"
```

Run the following command to list the Expectation Suites:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.py suite_names"
```

### 3. Creating your Checkpoint
Run the following YAML (inline as a Python string) to define `SimpleCheckpoint` as the starting point:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.py create_checkpoint"
```

### 4. Test your Checkpoint Configuration
When you have your YAML configuration, run the following command to test it and ensure it's correct:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.py test_checkpoint"
```

Modifying the previous configuration and testing with the previous cell helps ensure your configuration changes are correct.

### 5. Save your Checkpoint
Run the following command to save your Checkpoint and add it to the Data Context:
```python name="tests/integration/docusaurus/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.py save_checkpoint"
```

### Example Checkpoint configurations

 If you require more fine-grained configuration options, you can use the ``Checkpoint`` base class instead of ``SimpleCheckpoint``.

 In this example, the Checkpoint configuration uses the nesting of `batch_request` sections inside the `validations` block so as to use the defaults defined at the top level.

 ```python
 config = """
 name: my_fancy_checkpoint
 config_version: 1
 class_name: Checkpoint
 run_name_template: "%Y-%M-foo-bar-template-$VAR"
 validations:
   - batch_request:
       datasource_name: my_datasource
       data_asset_name: users
   - batch_request:
       datasource_name: my_datasource
       data_asset_name: users
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
   param1: "$MY_PARAM"
   param2: 1 + "$OLD_PARAM"
 runtime_configuration:
   result_format:
     result_format: BASIC
     partial_unexpected_count: 20
 """
 ```

 The following Checkpoint configuration runs the top-level `action_list` against the top-level `batch_request` as well as the locally-specified `action_list` against the top-level `batch_request`.

 ```python
 config = """
 name: airflow_users_node_3
 config_version: 1
 class_name: Checkpoint
 batch_request:
     datasource_name: my_datasource
     data_asset_name: users
 validations:
   - expectation_suite_name: users.warning  # runs the top-level action list against the top-level batch_request
   - expectation_suite_name: users.error  # runs the locally-specified action_list union with the top-level action-list against the top-level batch_request
     action_list:
     - name: quarantine_failed_data
       action:
           class_name: CreateQuarantineData
     - name: advance_passed_data
       action:
           class_name: CreatePassedData
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
     environment: $GE_ENVIRONMENT
     tolerance: 0.01
 runtime_configuration:
     result_format:
       result_format: BASIC
       partial_unexpected_count: 20
 """
 ```

 The Checkpoint mechanism also offers the convenience of templates.  The first Checkpoint configuration is that of a valid Checkpoint in the sense that it can be run as long as all the parameters not present in the configuration are specified in the `run_checkpoint` API call.

 ```python
 config = """
 name: my_base_checkpoint
 config_version: 1
 class_name: Checkpoint
 run_name_template: "%Y-%M-foo-bar-template-$VAR"
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
 """
 ```

 The above Checkpoint can be run using the code below, providing missing parameters from the configured Checkpoint at runtime.

 ```python
 checkpoint_run_result: CheckpointResult

 checkpoint_run_result = data_context.run_checkpoint(
     checkpoint_name="my_base_checkpoint",
     validations=[
         {
             "batch_request": {
                 "datasource_name": "my_datasource",
                 "data_asset_name": "users",
             },
             "expectation_suite_name": "users.delivery",
         },
         {
             "batch_request": {
                 "datasource_name": "my_datasource",
                 "data_asset_name": "users",
             },
             "expectation_suite_name": "users.delivery",
         },
     ],
 )
 ```

 However, the `run_checkpoint` method can be simplified by configuring a separate Checkpoint that uses the above Checkpoint as a template and includes the settings previously specified in the `run_checkpoint` method:

 ```python
 config = """
 name: my_fancy_checkpoint
 config_version: 1
 class_name: Checkpoint
 template_name: my_base_checkpoint
 validations:
 - batch_request:
     datasource_name: my_datasource
     data_asset_name: users
 - batch_request:
     datasource_name: my_datasource
     data_asset_name: users
 expectation_suite_name: users.delivery
 """
 ```

 Now the `run_checkpoint` method is as simple as in the previous examples:

 ```python
 checkpoint_run_result = context.run_checkpoint(
     checkpoint_name="my_fancy_checkpoint",
 )
 ```

 The `checkpoint_run_result` in both cases (the parameterized `run_checkpoint` method and the configuration that incorporates another configuration as a template) are the same.

 The final example presents a Checkpoint configuration that is suitable for the use in a pipeline managed by Airflow.

 ```python
 config = """
 name: airflow_checkpoint
 config_version: 1
 class_name: Checkpoint
 validations:
 - batch_request:
     datasource_name: my_datasource
     data_asset_name: IN_MEMORY_DATA_ASSET
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
 """
 ```

To run this Checkpoint, the `batch_request` with the `batch_data` nested under the `runtime_parameters` attribute needs to be specified explicitly as part of the `run_checkpoint()` API call, because the data to be <TechnicalTag tag="validation" text="Validated" /> is accessible only dynamically during the execution of the pipeline.

```python
checkpoint_run_result: CheckpointResult = data_context.run_checkpoint(
    checkpoint_name="airflow_checkpoint",
    batch_request={
        "runtime_parameters": {
            "batch_data": my_data_frame,
        },
        "data_connector_query": {
            "batch_filter_parameters": {
                "airflow_run_id": airflow_run_id,
            }
        },
    },
    run_name=airflow_run_id,
)
 ```
