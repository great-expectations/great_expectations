---
sidebar_label: 'Manage Expectations'
title: 'Manage Expectations'
description: Create and manage individual Expectations in Python with GX Core.
---
import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../../_core_components/prerequisites/_gx_installation.md';
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';


An Expectation is a verifiable assertion about your data. Expectations make implicit assumptions about your data explicit, and they provide a flexible, declarative language for describing expected behavior. They can help you better understand your data and help you improve data quality. 

## Prerequisites

- <PrereqPythonInstalled/>
- <PrereqGxInstalled/>.
- Recommended. [A preconfigured Data Context](/core/installation_and_setup/manage_data_contexts.md).
- Recommended. [A preconfigured Data Source and Data Asset connected to your data](/core/manage_and_access_data/connect_to_data/connect_to_data.md).

## Create an Expectation

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Import the `expectations` module from the GX Core library.

2. Initialize an Expectation class with the required parameters for that Expectation.

  From the `expectations` module you will have access to the core Expectations in GX.  The specific parameters you provide when initializing an Expectation are determined by the Expectation class.
  
  You can view available Expectations and the parameters they take in the [Expectation Gallery](https://greatexpectations.io/expectations).

</TabItem>

<TabItem value="example" label="Example code">

  ```python showLineNumbers title="Python" name="tests/integration/docusaurus/core/expectations/create_an_expectation.py full example code"
  ```

</TabItem>

</Tabs>

## Test an Expectation

<!-- TODO: Replace the sample code with snippets from example scripts under test -->

<Tabs>

<TabItem value="procedure" label="Procedure">

1. [Retrieve a Batch of data](/core/manage_and_access_data/request_data.md) to test the Expectation against.

2. Get the Expectation to test.  This could be a [newly created](#create-an-expectation) Expectation, an Expectation [retrieved from an Expectation Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md#get-an-expectation-from-an-expectation-suite), or a pre-existing Expectation from your code.

3. Validate the Expectation against the Batch.

4. Optional. [Modify the Expectation](#modify-an-expectation) and test it again.
 
5. Optional. [Add the Expectation to an Expectation Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md#add-expectations-to-an-expectation-suite).
   
  :::caution 

  Expectations do not persist between Python sessions unless they are saved as part of an Expectation Suite.

  :::

</TabItem>

<TabItem value="example" label="Example code">

```python showLineNumbers title="Python"
  import great_expectations as gx
  import great_expectations.expectations as gxe
  
  context = gx.get_context()
  data_asset = context.get_datasource("my_datasource").get_asset("my_asset")
  batch =

  expectation = gxe.ExpectColumnValuesToBeInSet(
    column="passenger_count", value_set=[1, 2, 3, 4, 5]
  )
  
  # highlight-next-line
  validation_result = batch.validate(expectation)
  ```

</TabItem>

</Tabs>

## Modify an Expectation

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Get the Expectation to modify.  This could be a [newly created](#create-an-expectation) Expectation that you wish to adjust, an Expectation [retrieved from an Expectation Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md#get-an-expectation-from-an-expectation-suite), or a pre-existing Expectation from your code.  

  This example uses an Expectation that was newly created in an Expectation Suite.

2. Modify the Expectation's attributes.

  The specific attributes that can be modified correspond to the parameters used to initialize the Expectation.  You can view available Expectations and the parameters they take in the [Expectation Gallery](https://greatexpectations.io/expectations).

3. Optional. If the Expectation belongs to an Expectation Suite, save the changes to the Expectation Suite.

  `expectation.save()` is explicitly used to update the configuration of an Expectation in an Expectation Suite.
  
  Although you can [test your modified Expectation](#test-an-expectation) without saving any changes to its Expectation Suite, the Expectation Suite will continue to use the Expectation's original values unless you use `expectation.save()` to persist your changes.
  
  If the Expectation is not part of an Expectation Suite, `expectation.save()` will fail.

</TabItem>

<TabItem value="example" label="Example code">

```python showLineNumbers title="Python" name="core/expectations/_examples/edit_an_expectation.py full example code"
```

</TabItem>

</Tabs>

## Customize an Expectation Class

<!-- TODO: Replace code examples with snippets from scripts under test -->

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Choose and import a base Expectation class.

  Any of the core Expectation classes in GX can be customized. You can view the available Expectations and their functionality in the [Expectation Gallery](https://greatexpectations.io/expectations).

2. Create a new Expectation class that inherits the base Expectation class.
  
  The core Expectations in GX have names descriptive of their functionality.  When creating your customized class you can provide a class name that is more indicative of your specific use case.

3. Override the Expectation's attributes with new default values.

  The attributes that can be overriden correspond to the parameters required by the base Expectation.  These can be referenced from the [Expectation Gallery](https://greatexpectations.io/expectations).

4. Customize the rendering of the new Expectation when displayed in Data Docs.

  The `render_text` attribute contains the text describing the customized Expectation when your results are rendered into Data Docs.  This text can be formatted with Markdown syntax.

</TabItem>

<TabItem value="example" label="Example code">

```python showLineNumbers title="Python"
from great_expectations.expectations import ExpectColumnValueToBeBetween

# highlight-start
class ExpectValidPassengerCount(ExpectColumnValueToBeBetween):
    column: str = "passenger_count"
    min_value: int = 0
    max_value: int = 6
    render_text: str = "There should be between **0** and **6** passengers."
# highlight-end
```

</TabItem>

</Tabs>

## Next steps

- Create Custom SQL Expectations
- Manage Expectation Suites