---
title: Test an Expectation
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPython from '../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstallation from '../_core_components/prerequisites/_gx_installation.md';
import PrereqDatacontext from '../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqDataSourceAndAssetConnectedToData from '../_core_components/prerequisites/_data_source_asset_and_batch_definition.md';
import PrereqExpectation from '../_core_components/prerequisites/_expectation.md';

Data can be validated against individual Expectations.  This workflow is generally used when engaging in exploration of new data, or when building out a set of Expectations to comprehensively describe the state that your data should conform to.

## Prerequisites

- <PrereqPython/>.
- <PrereqGxInstallation/>.
- <PrereqDatacontext/>.
- <PrereqDataSourceAndAssetConnectedToData/>.
- <PrereqExpectation/>.

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Retrieve your Batch Definition.

   In this example your Data Context is stored in the variable `context`.

   To retrieve your Batch Definition from your Data Context, update the `data_source_name`, `data_asset_name`, and `batch_definition_name` in the following code and execute it:

   ```python title="Python"
   data_source_name = "my_taxi_data"
   asset_name = "2018_taxi_data"
   batch_definition_name = "all_records_in_asset"
   batch = context.get_datasource(datasource_name).get_asset(asset_name).get_batch_definition(batch_definition_name=batch_definition_name).get_batch()
   ```

2. Run the Expectation on the Batch of data.

   In this example, the Expectation to test is already stored in the variable `expectation`:

   ```python title="Python"
   validation_results = batch.validate(expectation)
   ```

3. Evaluate the returned Validation Results.

   ```python title="Python"
   print(validation_results)
   ```

   When you print your Validation Results they will be presented in a dictionary format.  There are a few key/value pairs in particular that are important for evaluating your Validation Results.  These are:

   - `expectation_config`: Provides a dictionary that describes the Expectation that was run and what its parameters are.
   - `success`: The value of this key indicates if the data that was validated met the criteria described in the Expectation.
   - `result`: Contains a dictionary with additional information that shows why the Expectation succeded or failed. 

   In the following example you can see the Validation Results for an Expectation that failed because the `observed_value` reported in the `result` dictionary is outside of the `min_value` and `max_value` range described in the `expectation_config`:

   ```python title="Python output"
   {
     # highlight-start
     "success": false,
     # highlight-end
     "expectation_config": {
       "expectation_type": "expect_column_max_to_be_between",
       "kwargs": {
         "batch_id": "2018-06_taxi",
         "column": "passenger_count",
         # highlight-start
         "min_value": 4.0,
         "max_value": 5.0
         # highlight-end
       },
       "meta": {},
       "id": "38368501-4599-433a-8c6a-28f5088a4d4a"
     },
     "result": {
     # highlight-start
       "observed_value": 6
     # highlight-end
     },
     "meta": {},
     "exception_info": {
       "raised_exception": false,
       "exception_traceback": null,
       "exception_message": null
     }
   }
   ```

4. Optional. Adjust the Expectation's parameters and retest.

   If the Expectation did not return the results you anticipated you can update it to reflect the actual state of your data, rather than recreating it from scratch. An Expectation object stores the parameters that were provided to initialize it as attributes.  To modify an Expectation you overwrite those attributes.

   For example, if your Expectation took the parameters `min_value` and `max_value`, you could update them with:

   ```python title="Python input"
   expectation.min_value = 1
   expectation.max_value = 6
   ```

   Once you have set the new values for the Expectation's parameters you can reuse the Batch Definition from earlier and repeat this procedure to test your changes.

   ```python title="Python input"
   new_validation_results = batch.validate(expectation)
   print(new_validation_results)
   ```

   This time, the updated Expectation accurately describes the data and the validation succeds:

   ```python title="Python output"
   {
     # highlight-start
     "success": true,
     # highlight-end
     "expectation_config": {
       "expectation_type": "expect_column_max_to_be_between",
       "kwargs": {
         "batch_id": "2018-06_taxi",
         "column": "passenger_count",
         # highlight-start
         "min_value": 1.0,
         "max_value": 6.0
         # highlight-end
       },
       "meta": {},
       "id": "38368501-4599-433a-8c6a-28f5088a4d4a"
     },
     "result": {
       # highlight-start
       "observed_value": 6
       # highlight-end
     },
     "meta": {},
     "exception_info": {
       "raised_exception": false,
       "exception_traceback": null,
       "exception_message": null
     }
   }
   ```

   For more information about Validation Results, what they contain, and how to adjust their verbosity see [Choose result format](../run_validations/choose_result_format.md).


</TabItem>

<TabItem value="sample_code" label="Sample code">


   ```python title="Python"
   import great_expectations as gx
   import great_expectations.expectations as gxe

   # Your Data Context:
   context = gx.get_context()

   # The Expectation to test:
   expectation = gxe.ExpectColumnMaxToBeBetween(column="passenger_count", min_value=4, max_value=5)

   # Retrieve a Batch of data to test your Expectation on.
   datasource_name = "all_csv_files"
   asset_name = "csv_files"
   batch_definition_name = "2018-06_taxi"
   batch = context.get_datasource(datasource_name).get_asset(asset_name).get_batch_definition(batch_definition_name=batch_definition_name).get_batch()
   
   # Test the Expectation:
   validation_results = batch.validate(expectation)
   
   # Review the results:
   print(validation_results)
   
   # Optional. Modify the Expectation and test again:
   
   expectation.min_value = 1
   expectation.max_value = 6
   
   new_validation_results = batch.validate(expectation)
   print(new_validation_results)
   ```

</TabItem>

</Tabs>

