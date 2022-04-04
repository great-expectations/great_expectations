---
title: How to configure a new Checkpoint using test_yaml_config
---
import Prerequsities from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This how-to guide demonstrates advanced examples for configuring a <TechnicalTag tag="checkpoint" text="Checkpoint" /> using ``test_yaml_config``. **Note:** For a basic guide on creating a new Checkpoint, please see [How to create a new Checkpoint](../../../guides/validation/checkpoints/how_to_create_a_new_checkpoint.md).

``test_yaml_config`` is a convenience method for configuring the moving parts of a Great Expectations deployment. It allows you to quickly test out configs for <TechnicalTag tag="datasource" text="Datasources" />, <TechnicalTag tag="store" text="Stores" />, and Checkpoints. ``test_yaml_config`` is primarily intended for use within a notebook, where you can iterate through an edit-run-check loop in seconds.

<Prerequisites>

- [Set up a working deployment of Great Expectations](../../../tutorials/getting_started/intro.md)
- [Configured a Datasource using the v3 API](../../../tutorials/getting_started/connect_to_data.md)
- [Created an Expectation Suite](../../../tutorials/getting_started/create_your_first_expectations.md)

</Prerequisites>


## Steps

### 1. Create a new Checkpoint

From the <TechnicalTag tag="cli" text="CLI" />, execute:

````console
great_expectations checkpoint new my_checkpoint
````

This will open a Jupyter Notebook with a framework for creating and saving a new Checkpoint.  Run the cells in the Notebook until you reach the one labeled "Test Your Checkpoint Configuration."

### 2. Edit your Checkpoint

The Checkpoint configuration that was created when your Jupyter Notebook loaded uses an arbitrary <TechnicalTag tag="batch" text="Batch" /> of data and <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> to generate a basic Checkpoint configuration in the second code cell.  You can edit this configuration to point to add additional entries under the `validations` key, or to edit the existing one.  You can even replace this configuration entirely.  

In the [Additional Information](#additional-information) section at the end of this guide you will find examples of other Checkpoint configurations you can use as your starting point, as well as explanations of the various ways you can arrange the keys and values in your Checkpoint's `config_yaml`.

:::important
After you make edits to the `yaml_config` variable, don't forget to re-run the cell that contains it!
:::

### 3. Use `test_yamal_config()` to validate your Checkpoint configuration

Once you have made changes to the `yaml_config` in your Jupyter Notebook, you can verify that the updated configuration is valid by running the following code:

````python
my_checkpoint = context.test_yaml_config(yaml_config=yaml_config)
````

This code is found in the code cell under the "Test Your Checkpoint Configuration" in your Jupyter Notebook.

If your Checkpoint configuration is valid, you will see an output stating that your checkpoint was instantiated successfully, followed by a Python dictionary representation of the configuration yaml you edited.

### 4. (Optional) Repeat from step 2

From here you can continue to edit your Checkpoint. After each change you should re-run the cell that contains the edited `config_yaml` and then verify that your configuration remains valid by re-running `test_yaml_config(...)`.

### 5. Save your edited Checkpoint

Once you have made all of the changes you planned to implement and your last `test_yaml_config()` check passed, you are ready to save the Checkpoint you've created.  At this point, run the remaining cells in your Jupyter Notebook.  

Your checkpoint will be saved by the cell that contains the command:

```python
context.add_checkpoint(**yaml.load(yaml_config))
```
## Additional Information

### Example `SimpleCheckpoint` configuration

 The ``SimpleCheckpoint`` class takes care of some defaults which you will need to set manually in the ``Checkpoints`` class. The following example shows all possible configuration options for ``SimpleCheckpoint``:

 ```python
 config = """
 name: my_simple_checkpoint
 config_version: 1.0
 class_name: SimpleCheckpoint
 validations:
   - batch_request:
       datasource_name: data__dir
       data_connector_name: my_data_connector
       data_asset_name: TestAsset
       data_connector_query:
         index: 0
     expectation_suite_name: yellow_tripdata_sample_2019-01.warning
 site_names: my_local_site
 slack_webhook: my_slack_webhook_url
 notify_on: all # possible values: "all", "failure", "success"
 notify_with: # optional list of DataDocs site names to display in Slack message
 """
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
     data_connector_name: my_special_data_connector
     data_asset_name: users
     data_connector_query:
         index: -1
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
                 "data_connector_name": "my_special_data_connector",
                 "data_asset_name": "users",
                 "data_connector_query": {
                     "index": -1,
                 },
             },
             "expectation_suite_name": "users.delivery",
         },
         {
             "batch_request": {
                 "datasource_name": "my_datasource",
                 "data_connector_name": "my_other_data_connector",
                 "data_asset_name": "users",
                 "data_connector_query": {
                     "index": -2,
                 },
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
     data_connector_name: my_runtime_data_connector
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

