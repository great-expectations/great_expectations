---
title: Manage Validation Definitions
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../../_core_components/prerequisites/_gx_installation.md';
import PrereqPreconfiguredDataContext from '../../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqPreconfiguredDataSourceAndAsset from '../../_core_components/prerequisites/_data_source_and_asset_connected_to_data.md';
import PrereqPreconfiguredExpectationSuiteAndExpectations from '../../_core_components/prerequisites/_expectation_suite_with_expectations.md';

import StepRequestADataContext from '../../_core_components/common_steps/_request_a_data_context.md';

A Validation Definition is a fixed reference that links a Batch of data to an Expectation Suite. It can be run by itself to validate the referenced data against the associated Expectations for testing or data exploration.  Multiple Validation Definitions can also be provided to a Checkpoint which, when run, executes Actions based on the Validation Results for each provided Validation Definition.

## Prerequisites

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- <PrereqPreconfiguredDataContext/>.
- <PrereqPreconfiguredDataSourceAndAsset/>.
- <PrereqPreconfiguredExpectationSuiteAndExpectations/>.


## Create a Validation Definition

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Import the `ValidationDefinition` class from the GX library.

  ```python title="Python"
  from great_expectations.core import ValidationDefinition
  ```

2. <StepRequestADataContext/>.

  In this example the variable `context` is your Data Context.

3. Get an Expectation Suite with Expectations.  This can be [an existing Expectation Suite retrieved from your Data Context](/core/create_expectations/expectation_suites/manage_expectation_suites.md#get-an-existing-expectation-suite) or a new Expectation Suite in your current code.

  In this example the variable `suite` is your Expectation Suite.

4. Get an existing or create a new Batch Definition describing the data that will be associated with the Expectation Suite.

  In this example the variable `batch_definition` is your Batch Definition.

5. Create a `ValidationDefinition` instance using the Batch Definition, Expectation Suite, and a unique name.

  ```python title="Python"
  definition_name = "My Validation Definition"
  validation_definition = ValidationDefintion(data=batch_definition, suite=suite, name=definition_name)
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
import great_expectations as gx
from great_expectations.core import ValidationDefinition

context = gx.get_context()

existing_suite_name = "my_expectation_suite"
suite = context.suites.get(name=existing_suite_name)

existing_data_source_name = "my_datasource"
existing_data_asset_name = "my_data_asset"
existing_batch_definition_name = "my_batch_definition"
batch_definition = context.get_datasource(existing_data_source_name).get(existing_data_asset_name).get(existing_batch_definition_name)

# highlight-start
definition_name = "My Validation Definition"
  validation_definition = ValidationDefintion(data=batch_definition, suite=suite, name=definition_name)
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

## List available Validation Definitions

<Tabs>

<TabItem value="procedure" label="Procedure">

1. <StepRequestADataContext/>.

  In this example the variable `context` is your Data Context.

2. Use the Data Context to retrieve and print the names of the available Validation Definitions:

  ```python
  validation_definition_names = [definition.name for definition in context.validation_definitions]
  print(validation_definition_names)
  ```


</TabItem>

<TabItem value="sample_code" label="Sample code">

```python showLineNumbers title="Python"
import great_expectations as gx

context = gx.get_context()

# highlight-start
for definition in context.validation_definitions:
      print(definition.name)
# highlight-end
```

</TabItem>

</Tabs>

## Get a Validation Definition by name

<Tabs>

<TabItem value="procedure" label="Procedure">

1. <StepRequestADataContext/>.

  In this example the variable `context` is your Data Context.

2. Use the Data Context to request the Validation Definition.

  ```python title="Python"
  definition_name = "My Validation Definition"
  validation_definition = context.validation_definitions.get(name=definition_name)
  ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python showLineNumbers title="Python"
import great_expectations as gx

context = gx.get_context()

# highlight-start
definition_name = "My Validation Definition"
validation_definition = context.validation_definitions.get(name=definition_name)
# highlight-end
```

</TabItem>

</Tabs>

## Get Validation Definitions by attributes

<Tabs>

<TabItem value="procedure" label="Procedure">

1. <StepRequestADataContext/>.

  In this example the variable `context` is your Data Context.

2. Determine the attributes to filter on.

  Validation Definitions associate an Expectation Suite with a Batch Definition.  This means that valid attributes to filter on include the attributes for the Expectation Suite, as well as the attributes for the Batch Definition, the Batch Definition's Data Asset, and the Data Asset's Data Source.

3. Use a list comprehension to return all Validation Definitions that match the filtered attributes.

  For example, you can retrieve all Validation Definitions that include a specific Expectation Suite by filtering on the Expectation Suite name:

  ```python title="Python"
  existing_expectation_suite_name = "my_expectation_suite"
  validation_definitions_for_suite = [
    definition for definition in context.validation_definitions
    if definition.suite.name == existing_expectation_suite_name
  ]
  ```

  Or you could return all Validation Definitions involving a specific Data Asset by filtering on the Data Source and Data Asset names:

  ```python title="Python"
  existing_data_source_name = "my_data_source"
  existing_data_asset_name = "my_data_asset"
  validation_definitions_for_asset = [
    definition for definition in context.validation_definitions
    if definition.data_source.name == existing_data_source_name
    and definition.asset.name == existing_data_asset_name
  ]
  ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python showLineNumbers title="Python"
import great_expectations as gx

context = gx.get_context()

# highlight-start
existing_expectation_suite_name = "my_expectation_suite"
validation_definitions_for_suite = [
    definition for definition in context.validation_definitions
    if definition.expectation_suite.name == existing_expectation_suite_name
  ]
# highlight-end

# highlight-start
existing_data_source_name = "my_data_source"
existing_data_asset_name = "my_data_asset"
validation_definitions_for_asset = [
  definition for definition in context.validation_definitions
  if definition.data_source.name == existing_data_source_name
  and definition.asset.name == existing_data_asset_name
]
# highlight-end
```

</TabItem>

</Tabs>

## Delete a Validation Definition

<Tabs>

<TabItem value="procedure" label="Procedure">

1. <StepRequestADataContext/>.

  In this example the variable `context` is your Data Context.

2. [Get the Validation Definition](#get-a-validation-definition-by-name) to delete.

  In this example the variable `validation_definition` is the Validation Definition to delete.

3.  Use the Data Context to delete the Validation Definition:

  ```python title="Python"
  context.validations.delete(name=validation_definition.name)
  ```

  You can directly provide the Validation Definition's name as a string.  However, retrieving the Validation Definition from your Data Context and using its name attribute to specify the Validation Definition to delete will ensure that you do not introduce typos, differences in capitalization, or otherwise attempt to delete a Validation Definition that does not exist.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python showLineNumbers title="Python"
import great_expectations as gx

context = gx.get_context()

definition_name = "My Validation Definition"
validation_definition = context.validation_definitions.get(name=definition_name)

# highlight-start
context.validation_definitions.delete(validation_definition.name)
# highlight-end
```

</TabItem>

</Tabs>


## Duplicate a Validation Definition

<Tabs>

<TabItem value="procedure" label="Procedure">

Validation definitions are intended to be fixed references that link a set of data to an Expectation Suite.  As such, they do not include an update method.  However, multiple Validation Definitions with the same Batch Definition and Expectation Suite can exist as long as each has a unique name.

Although an existing Validation Definition cannot be renamed, a duplicate can be created that has a name different or updated from the original.

1. Import the GX library and `ValidationDefintion` class:

  ```python title="Python"
  import great_expectations as gx
  from great_expectations.core import ValidationDefinition
  ```

2. <StepRequestADataContext/>.

  In this example the variable `context` is your Data Context.

3. Get the original Validation Definition.

  In this example the variable `original_validation_definition` is the original Validation Definition.

2. Get the Batch Definition and Expectation Suite from the original Validation Definition:
  
  ```python title="Python"
  original_suite = original_validation_definition.suite
  original_batch = original_validation_definition.batch_definition
  ```

3. Add a new Validation Definition to the Data Context using the same Batch Definition and Expectation Suite as the original:

  ```python title="Python"
  new_definition_name = "my_validation_definition"
  new_validation_definition = ValidationDefintion(
    data=original_batch,
    suite=original_suite,
    name=definition_name
  )
  context.validation_definitions.add(new_validation_definition)
  ```

4. Optional. [Delete the original Validation Definition](#delete-a-validation-definition).


</TabItem>

<TabItem value="sample_code" label="Sample code">

```python showLineNumbers title="Python"
import great_expectations as gx
from great_expectations.core import ValidationDefinition

context = gx.get_context()

original_definition_name = "my_vldtn_dfntn"
original_validation_definition = context.validation_definitions.get(original_definition_name)

# highlight-start
original_suite = original_validation_definition.suite
original_batch = original_validation_definition.batch_definition
# highlight-end

# highlight-start
new_definition_name = "my_validation_definition"
new_validation_definition = ValidationDefintion(
  data=original_batch,
  suite=original_suite,
  name=definition_name
)
context.validation_definitions.add(new_validation_definition)
# highlight-end

context.validation_definitions.delete(original_validation_definition)
```

</TabItem>

</Tabs>

## Run a Validation Definition

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Create a new or retrieve an existing Validation Definition.

2. Execute the Validation Definition's `run()` method:

  ```python title="Python"
  validation_result = validation_definition.run()
  ```

  Validation Results are automatically saved in your Data Context when a Validation Definition's `run()` method is called.  For convenience, the `run()` method also returns the Validation Results as an object you can review.

3. Review the Validation Results:
 
  ```python title="Python"
  print(validation_result)
  ```

  :::tip

  GX Cloud users can view the Validation Results in the GX Cloud UI by following the url provided with:

  ```python title="Python"
  print(validation_result.result_url)
  ```

  :::

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python showLineNumbers title="Python"
import great_expectations as gx

context = gx.get_context()

existing_validation_definition_name = "my_validation_definition"
validation_definition = context.validation_definitions.get(existing_validation_definition_name)

# highlight-next-line
validation_result = validation_definition.run()

# highlight-next-line
print(validation_result)

# highlight-next-line
print(validation_result.results_url)
```

</TabItem>

</Tabs>