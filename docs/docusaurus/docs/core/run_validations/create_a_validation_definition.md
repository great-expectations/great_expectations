---
title: Create a Validation Definition
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import GxData from '../_core_components/_data.jsx'
import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../_core_components/prerequisites/_gx_installation.md';
import PrereqPreconfiguredDataContext from '../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqPreconfiguredDataSourceAndAsset from '../_core_components/prerequisites/_data_source_and_asset_connected_to_data.md';
import PrereqPreconfiguredExpectationSuiteAndExpectations from '../_core_components/prerequisites/_expectation_suite_with_expectations.md';

import StepRequestADataContext from '../_core_components/common_steps/_request_a_data_context.md';


A Validation Definition is a fixed reference that links a Batch of data to an Expectation Suite. It can be run by itself to validate the referenced data against the associated Expectations for testing or data exploration.  Multiple Validation Definitions can also be provided to a Checkpoint which, when run, executes Actions based on the Validation Results for each provided Validation Definition.

<h2>Prerequisites</h2>

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- <PrereqPreconfiguredDataContext/>.
- <PrereqPreconfiguredDataSourceAndAsset/>.
- <PrereqPreconfiguredExpectationSuiteAndExpectations/>.

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Import the `ValidationDefinition` class from the {GxData.product_name} library:

   ```python title="Python"
   from great_expectations.core import ValidationDefinition
   ```

2. Request a Data Context:

   ```python title="Python"
   import great_expectations as gx
   
   context = gx.get_context()
   ```

3. Retrieve an Expectation Suite with Expectations.

   Update the value of `suite_name` in the following code with the name of your Expectation Suite.  Then execute the code to retrieve that Expectation Suite:

   ```python title="Python"
   suite_name = "<NAME OF AN EXISTING EXPECTATION SUITE>"
   suite = context.get_expectation_suite(suite_name)
   ```

4. Retrieve the Batch Definition that describes the data to associate with the Expectation Suite.

   Update the values of `data_source_name`, `data_asset_name`, and `batch_definition_name` in the following code with the names of your previously defined Data Source, one of its Data Assets, and a Batch Definition for that Data Asset.  Then execute the code to retrieve the Batch Definition:

   ```python title="Python"
   data_source_name = "my_datasource"
   data_asset_name = "my_data_asset"
   batch_definition_name = "my_batch_definition"

   batch_definition = context.get_datasource(data_source_name).get_asset(data_asset_name).get_batch_definition(batch_definition_name)
   ```

5. Create a `ValidationDefinition` instance using the Batch Definition, Expectation Suite, and a unique name.

   Update the value of `definition_name` with a descriptive name that indicates the purpose of the Validation Definition.  Then execute the code to create your Validation Definition:

   ```python title="Python"
   definition_name = "My Validation Definition"
   validation_definition = ValidationDefinition(data=batch_definition, suite=suite, name=definition_name)
   ```

6. Optional. Save the Validation Definition to your Data Context.

   ```python title="Python"
   validation_definition = context.validation_definitions.add(validation_definition)
   ```

   :::tip

   You can add a Validation Definition to your Data Context at the same time as you create it with the following code:

   ```python title="Python"
   definition_name = "My second Validation Definition"
   validation_definition = context.validation_definitions.add(ValidationDefinition(data=batch_definition, suite=suite, name=definition_name)) 
   ```

   :::

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python showLineNumbers title="Python"
from great_expectations.core import ValidationDefinition
import great_expectations as gx

context = gx.get_context()

suite_name = "my_expectation_suite"
suite = context.suites.get(name=suite_name)

data_source_name = "my_datasource"
data_asset_name = "my_data_asset"
batch_definition_name = "my_batch_definition"
batch_definition = context.get_datasource(data_source_name).get_asset(data_asset_name).get_batch_definition(batch_definition_name)

# highlight-start
definition_name = "My Validation Definition"
validation_definition = ValidationDefinition(data=batch_definition, suite=suite, name=definition_name)
# highlight-end

# highlight-start
validation_definition = context.validation_definitions.add(validation_definition)
# highlight-end

# highlight-start
new_definition_name = "My second Validation Definition"
validation_definition = context.validation_definitions.add(ValidationDefinition(data=batch_definition, suite=suite, name=new_definition_name)) 
# highlight-end
```

</TabItem>

</Tabs>