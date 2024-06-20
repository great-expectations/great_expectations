---
title: Test an Expectation
---

## Prerequisites

- Python
- GX installed
- An Expectation
- A Data Context with a fully configured Data Source, Data Asset, and Batch Definition.

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

   The Validation Results object returned by `batch.validate(...)` 

4. Optional. Adjust the Expectation's parameters and retest.

   If the Expectation did not return the results you anticipated you can update it to reflect the actual state of your data, rather than recreating it from scratch. An Expectation object stores the parameters that were provided to initialize it as attributes.  To modify an Expectation you overwrite those attributes.

   For example, if your Expectation took the parameters `min_value` and `max_value`, you could update them with:

   ```python title="Python"
   expectation.min_value = 1
   expectation.max_value = 6
   ```

   Once you have set the new values for the Expectation's parameters you can reuse the Batch Definition from earlier and repeat this procedure to test your changes.

   ```python title="Python"
   new_validation_results = batch.validate(expectation)
   print(new_validation_results)
   ```


```python title="Python"
   import great_expectations as gx
   import great_expectations.expectations as gxe

   # Your Data Context:
   context = gx.get_context()

   # The Expectation to test:
   expectation = gxe.ExpectColumnMaxToBeBetween(column="passenger_count", min_value=4, max_value=6)

   # Retrieve a Batch of data to test your Expectation on.
   datasource_name = "all_csv_files"
   asset_name = "csv_files"
   batch_definition_name = "2018-06_taxi2"
   batch = context.get_datasource(datasource_name).get_asset(asset_name).get_batch_definition(batch_definition_name=batch_definition_name).get_batch()
   
   # Test the Expectation:
   validation_results = batch.validate(expectation)
   
   # Review the results:
   print(validation_results)
   
   # Optional. Modify the Expectation and test again:
   
   ```python title="Python"
   expectation.min_value = 1
   expectation.max_value = 6
   
   validation_results = batch.validate(expectation)
   print(validation_results)
   ```
   
```