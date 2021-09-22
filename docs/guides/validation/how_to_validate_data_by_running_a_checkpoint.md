---
title: How to validate your data using a Checkpoint
---

import Prerequisites from '../../guides/connecting_to_your_data/components/prerequisites.jsx';

This guide will help you validate your data by running a Checkpoint.


<Prerequisites>

- Configured a [Data Context](../../tutorials/getting_started/initialize_a_data_context.md).
- Configured an [Expectations Suite](../../tutorials/getting_started/create_your_first_expectations.md).
- Configured a [Checkpoint](./checkpoints/how_to_create_a_new_checkpoint)

</Prerequisites>

Steps
-----

1. You can update your existing checkpoint either using a text editor or by executing the following command:

   ```console
   great_expectations --v3-api checkpoint edit my_checkpoint
   ```

   This will open a **Jupyter Notebook** that will allow you to modify the configuration of your Checkpoint.

   For example, for the Taxi dataset, the end result will look similar to this:
 
   ```yaml
    name: my_checkpoint
    config_version: 1.0
    class_name: SimpleCheckpoint
    run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
    validations:
      - batch_request:
          datasource_name: taxi_data
          data_connector_name: taxi_data_example_data_connector
          data_asset_name: yellow_tripdata_sample_2019-02.csv
          data_connector_query:
            index: -1
        expectation_suite_name: taxi.demo
   ```

2. You may wish to utilize the powerful fully-featured Checkpoint (instead of SimpleCheckpoint).  To do this, open your existing Checkpoint configuration in a text editor and make the necessary changes (such as in the following):

   ```yaml
   name: my_checkpoint
   config_version: 1
   class_name: Checkpoint
   run_name_template: "%Y-%m-foo-bar-template-$VAR"
   validations:
     - batch_request:
         datasource_name: taxi_data
         data_connector_name: taxi_data_example_data_connector
         data_asset_name: yellow_tripdata_sample_2019-02.csv
         data_connector_query:
           index: -1
       expectation_suite_name: taxi.demo
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

   The details of Checkpoint configuration can be found in [How to add validations data or suites to a Checkpoint](./checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint).
   In addition, please see [How to configure a new Checkpoint using test_yaml_config](./checkpoint/how_to_add_validations_data_or_suites_to_a_checkpoint.md) for more Checkpoint configuration examples (including the convenient templating mechanism).

3. Run Checkpoint validations on your data.
   You can run the Checkpoint from the CLI as explained in [How to run a Checkpoint in terminal](./checkpoints/how_to_run_a_checkpoint_in_terminal) or from Python, as explained in [How to run a Checkpoint in Python](./checkpoints/how_to_run_a_checkpoint_in_python).
