---
title: How to create Expectations that span multiple Batches using Evaluation Parameters
---

import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';

This guide will help you create [Expectations](../../../reference/expectations/expectations.md) that span multiple [Batches](../../../reference/dividing_data_assets_into_batches.md) of data using [Evaluation Parameters](../../../reference/evaluation_parameters.md) (see also [Evaluation Parameter stores](../../../reference/data_context.md#evaluation-parameter-stores)). This pattern is useful for things like verifying that row counts between tables stay consistent.

<Prerequisites>

- Configured a [Data Context](../../../tutorials/getting_started/initialize_a_data_context.md).
- Configured a [Datasource](../../../reference/datasources.md) (or several Datasources) with at least two **Data Assets** and understand the basics of **Batch Requests**.
- Also created [Expectations Suites](../../../tutorials/getting_started/create_your_first_expectations.md) for those Data Assets.
- Have a working [Evaluation Parameter store](../../../reference/data_context.md#evaluation-parameter-stores). (Included by default in the base ``great_expectations.yml`` created by ``great_expectations init``)
- Have a working [Checkpoint](../../../guides/validation/how_to_validate_data_by_running_a_checkpoint.md)

</Prerequisites>

Steps
-----

1. **Import great_expectations and instantiate your Data Context**
   ```python
   import great_expectations as ge
   context = ge.DataContext()
   ```

2. **Instantiate two Validators, one for each Data Asset**

    We'll call one of these Validators the *upstream* Validator and the other the *downstream* Validator. Evaluation Parameters will allow us to use Validation Results from the upstream Validator as parameters passed into Expectations on the downstream.

    ```python
   from great_expectations.core.batch import BatchRequest
    batch_request_1 = BatchRequest(
        datasource_name="my_datasource",
        data_connector_name="my_data_connector",
        data_asset_name="my_data_asset_1"
    )
    upstream_validator = context.get_validator(batch_request=batch_request_1, expectation_suite_name="my_expectation_suite_1")

    batch_request_2 = BatchRequest(
        datasource_name="my_datasource",
        data_connector_name="my_data_connector",
        data_asset_name="my_data_asset_2"
    )
    downstream_validator = context.get_validator(batch_request=batch_request_2, expectation_suite_name="my_expectation_suite_2")
    ```
   
   :::note

   The Batch Requests can use the same [Datasource and Data Connector](../../../reference/datasources.md), or can use reference different Datasources.
 
   :::
   
3. **Add an Expectation to the upstream Validator**
   In order to create a Validation Result on the upstream Validator, we need to create and save an Expectation:
   ```python
   upstream_validator.expect_table_row_count_to_be_between(
      min_value=1,
      max_value=100
   )
   upstream_validator.save_expectation_suite()
   ```

5. **Define a URN (Uniform Resource Identifier)**
   An Evaluation Parameter ``URN`` is a string which Great Expectations resolves at runtime to obtain a Metric.
   A ``URN`` represents a series of namespaces separated by colons (``:``), and always begins with the prefix ``urn:great_expectations``.
   It can access Validations, Metrics, and Stores, and uses the following syntax:
   1. ```python
      # Validations
      validation_urn = "urn:great_expectations:validations:<expectation suite name>:<metric name>:<metric kwargs>"
      ```
   2. ```python
      # Metrics
      metric_urn = "urn:great_expectations:metrics:<run id>:<expectation suite name>:<metric name>:<metric kwargs>"
      ```
   3. ```python
      # Stores
      store_urn = "urn:great_expectations:stores:<store name>:<metric name>:<metric kwargs>"
      ```
      

   For example, if your project contains an Expectation Suite called ``my_expectation_suite_1``, which has a ``expect_table_row_count_to_be_between`` Expectation, you could reference the Expectation's ``observed_value`` metric from its most recent Validation Result with the following URN:
   ``'urn:great_expectations:validations:my_expectation_suite_1:expect_table_row_count_to_be_between.result.observed_value'``

6. **Add an Expectation using an Evaluation Parameter to the downstream Validator.**
   Next, create and save an Expectation on the downstream Validator.
   This Expectation's `value` kwarg will reference a value obtained from the upstream Validation Result. 
   Disable interactive evaluation on the Validator to declare an Expectation even when it cannot be evaluated immediately.
   Then, when saving the Expectation, set the ``discard_failed_expectations`` flag to ``False``, since this Expectation will only be successful when run in the same Checkpoint as the upstream Validator.

   ```python
   downstream_validator.interactive_evaluation = False
   eval_param_urn = 'urn:great_expectations:validations:my_expectation_suite_1:expect_table_row_count_to_be_between.result.observed_value'
   downstream_validator.expect_table_row_count_to_equal(
      value={
         '$PARAMETER': eval_param_urn, # this is the actual parameter we're going to use in the validation
      }
   )
   downstream_validator.save_expectation_suite(discard_failed_expectations=False) 
   ```
   ::: 

   **A Closer Look**
   The core of this is a ``$PARAMETER : URN`` pair. When Great Expectations encounters a ``$PARAMETER`` flag during validation, it will replace the ``URN`` with a value retrieved from an [Evaluation Parameter stores](../../../reference/data_context.md#evaluation-parameter-stores) or [Metrics Store](../../../reference/metrics.md) (see also [How to configure a MetricsStore](../../../guides/setup/configuring_metadata_stores/how_to_configure_a_metricsstore.md)).

   This declaration above includes two ``$PARAMETERS``.
   The first is the real parameter that will be used after the Expectation Suite is Validated as part of a Checkpoint.
   The second parameter supports immediate evaluation in the notebook.

   When executed in the notebook, this Expectation will generate an [Expectation Validation Result](../../../reference/validation.md). 
   Most values will be missing, since interactive evaluation was disabled.

   ```python
   {
      "result": {},
      "success": null,
      "meta": {},
      "exception_info": {
         "raised_exception": false,
         "exception_traceback": null,
         "exception_message": null
      }
   }
    ```
   
   :::

   :::warning

   Your URN must be exactly correct in order to work in production. Unfortunately, successful execution at this stage does not guarantee that the URN is specified correctly and that the intended parameters will be available when executed later.

   :::


8. **Validate using a Checkpoint.**
   First, [Add your new Expectation Suites to your Checkpoint](../../validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint.md) by adding the Expectation Suites created above to the `validations` list of your Checkpoint config.
    
   Then, validate the Checkpoint from the CLI: 
   ```bash
   $ great_expectations checkpoint run <checkpoint name>
   ```
   
   Alternately, validate the Checkpoint directly from your code:

   ```python
   results = context.run_checkpoint(
       checkpoint_name="my_checkpoint"
   )
   ```
   

9. **Rebuild Data Docs and review results in docs.**
   
   Build or update Data Docs from the command line:
   ```bash
   great_expectations docs build
   ```
   
   Alternately, build or update Data Docs directly from your code:

   ```python
   context.build_data_docs()
   ```

       Once your Docs rebuild, open them in a browser and navigate to the page for the new Validation Result.

       If your Evaluation Parameter was executed successfully, you'll see something like this:

       ![image](../../../../docs/images/evaluation_parameter_success.png)

       If it encountered an error, you'll see something like this. The most common problem is a mis-specified URN name.

       ![image](../../../../docs/images/evaluation_parameter_error.png)

Comments
--------
