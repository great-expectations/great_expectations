---
title: Test an Expectation
description: Test an individual Expectation by validating a Batch of data.
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPython from '../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstallation from '../_core_components/prerequisites/_gx_installation.md';
import PrereqDatacontext from '../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqDataSourceAndAssetConnectedToData from '../_core_components/prerequisites/_data_source_asset_and_batch_definition.md';
import PrereqExpectation from '../_core_components/prerequisites/_expectation.md';

Data can be validated against individual Expectations.  This workflow is generally used when engaging in exploration of new data, or when building out a set of Expectations to comprehensively describe the state that your data should conform to.

<h2>Prerequisites</h2>

- <PrereqPython/>.
- <PrereqGxInstallation/>.
- <PrereqDatacontext/>.
- [A Batch of sample data](/core/define_expectations/retrieve_a_batch_of_test_data.md).  This guide assumes the variable `batch` contains your sample data.
- <PrereqExpectation/>.  This guide assumes the variable `expectation` contains the Expectation to be tested.

### Procedure

<Tabs 
   queryString="procedure"
   defaultValue="instructions"
   values={[
      {value: 'instructions', label: 'Instructions'},
      {value: 'sample_code', label: 'Sample code'}
   ]}
>

<TabItem value="instructions" label="Instructions">

1. Run the Expectation on the Batch of data.

   In this example, the Expectation to test was defined with preset parameters and is already stored in the variable `expectation`. The variable `batch` contains a Batch that was retrieved from a `.csv` file using the pandas defualt Data Source:

   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_examples/test_an_expectation.py - test expectation with preset parameters"
   ```
   
   In this example, the Expectation to test was defined to take Expectation Parameters at runtime:
 
   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_examples/test_an_expectation.py - test expectation with expectation parameters"
   ```

2. Evaluate the returned Validation Results.

   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_examples/test_an_expectation.py - evaluate Validation Results"
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

3. Optional. Adjust the Expectation's parameters and retest.

   If the Expectation did not return the results you anticipated you can update it to reflect the actual state of your data, rather than recreating it from scratch. An Expectation object with preset parameters stores the parameters that were provided to initialize it as attributes.  To modify the Expectation you overwrite those attributes.

   For example, if your Expectation has took the parameters `min_value` and `max_value`, you could update them with:

   ```python title="Python input" name="docs/docusaurus/docs/core/define_expectations/_examples/test_an_expectation.py - modify preset expectation parameters"
   ```

   Once you have set the new values for the Expectation's parameters you can reuse the Batch Definition from earlier and repeat this procedure to test your changes.

   ```python title="Python input" name="docs/docusaurus/docs/core/define_expectations/_examples/test_an_expectation.py - test and review a modified Expectation"
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

   When an Expectation uses an Expectation Parameter dictionary you don't have to modify anything on the Expectation object.  Instead, update the dictionary with new values and then test it with the updated dictionary:
 
   ```python title="Python input" name="docs/docusaurus/docs/core/define_expectations/_examples/test_an_expectation.py - modify and retest Expectation Parameters dictionary" 
   ```

   For more information about Validation Results, what they contain, and how to adjust their verbosity see [Choose result format](../trigger_actions_based_on_results/choose_a_result_format/choose_a_result_format.md).


</TabItem>

<TabItem value="sample_code" label="Sample code">


   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_examples/test_an_expectation.py - full code example"
   ```

</TabItem>

</Tabs>

