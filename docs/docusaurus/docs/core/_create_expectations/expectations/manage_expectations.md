---
sidebar_label: 'Manage Expectations'
title: 'Manage Expectations'
description: Create and manage individual Expectations in Python with GX Core.
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../../_core_components/prerequisites/_gx_installation.md';
import PrereqPreconfiguredDataContext from '../../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqPreconfiguredDataSourceAndAsset from '../../_core_components/prerequisites/_data_source_and_asset_connected_to_data.md';

An Expectation is a verifiable assertion about your data. Expectations make implicit assumptions about your data explicit, and they provide a flexible, declarative language for describing expected behavior. They can help you better understand your data and help you improve data quality. 

## Prerequisites

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- Recommended. <PrereqPreconfiguredDataSourceAndAsset/>.
- Recommended. <PrereqPreconfiguredDataSourceAndAsset/>.

## Create an Expectation

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Import the `expectations` module from the GX Core library:

  ```python title="Python" name="core/create_expectations/expectations/_examples/create_an_expectation.py import the expectations module"
  ```

2. Initialize an Expectation class with the required parameters for that Expectation:

  ```python title="Python" name="core/create_expectations/expectations/_examples/create_an_expectation.py create the expectation"
  ```
  
  The `expectations` module contains the core Expectations classes available in GX.  The specific parameters you provide when initializing an Expectation are determined by the Expectation class.
  
  You can view available Expectations and the parameters they take in the [Expectation Gallery](https://greatexpectations.io/expectations).

</TabItem>

<TabItem value="example" label="Sample code">

  ```python showLineNumbers title="Python" name="tests/integration/docusaurus/core/expectations/create_an_expectation.py full example code"
  ```

</TabItem>

</Tabs>

## Test an Expectation

<!-- TODO: Replace the sample code with snippets from example scripts under test -->

<Tabs>

<TabItem value="procedure" label="Procedure">

1. [Retrieve a Batch of data](/core/manage_and_access_data/request_data.md) to test the Expectation against.  

  In this procedure the variable `batch` is your Batch of data. 

2. Get the Expectation to test.  This could be a [newly created](#create-an-expectation) Expectation, an Expectation [retrieved from an Expectation Suite](/core/_create_expectations/expectation_suites/manage_expectation_suites.md#get-an-expectation-from-an-expectation-suite), or a pre-existing Expectation from your code.

  In this procedure the variable `expectation` is your Expectation to test.

3. Validate the Expectation against the Batch:
  
  ```python title="Python"
  validation_result = batch.validate(expectation)
  ```

4. Optional. [Modify the Expectation](#modify-an-expectation) and test it again.
 
5. Optional. [Add the Expectation to an Expectation Suite](/core/_create_expectations/expectation_suites/manage_expectation_suites.md#add-expectations-to-an-expectation-suite).
   
  :::caution 

  Expectations do not persist between Python sessions unless they are saved as part of an Expectation Suite.

  :::

</TabItem>

<TabItem value="example" label="Sample code">

```python showLineNumbers title="Python"
  import great_expectations as gx
  import great_expectations.expectations as gxe
  
  context = gx.get_context()
  data_asset = context.data_sources.get("my_datasource").get_asset("my_asset")
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

1. Get the Expectation to modify.  This could be a [newly created](#create-an-expectation) Expectation that you wish to adjust, an Expectation [retrieved from an Expectation Suite](/core/_create_expectations/expectation_suites/manage_expectation_suites.md#get-an-expectation-from-an-expectation-suite), or a pre-existing Expectation from your code.  

  In this procedure the variable `expectation` is the Expectation you're modifying.

2. Modify the Expectation's attributes:

  ```python title="Python" name="core/create_expectations/expectations/_examples/edit_an_expectation.py modify the expectation"
  ```

  The specific attributes that can be modified correspond to the parameters used to initialize the Expectation.  You can view available Expectations and the parameters they take in the [Expectation Gallery](https://greatexpectations.io/expectations).

3. Optional. [Test the modified Expectation](#test-an-expectation) against a Batch of data.

  Repeat this step until the results from testing the Expectation correspond to the desired results for your specific use case and data.

4. Optional. If the Expectation belongs to an Expectation Suite, save the changes to the Expectation Suite:

  ```python title="Python" name="core/create_expectations/expectations/_examples/edit_an_expectation.py save the expectation"
  ```

  `expectation.save()` is explicitly used to update the configuration of an Expectation in an Expectation Suite.
  
  An Expectation Suite continues to use the Expectation's original values unless you save your modifications.  However, you can [test your modified Expectation](#test-an-expectation) without saving any changes to its Expectation Suite.  This allows you to explicitly decide if you want to keep or discard your changes after testing.
  
  The command `expectation.save()` fails if the Expectation is not part of an Expectation Suite.

</TabItem>

<TabItem value="example" label="Sample code">

```python showLineNumbers title="Python" name="core/expectations/_examples/edit_an_expectation.py full example code"
```

</TabItem>

</Tabs>

## Customize an Expectation Class

<!-- TODO: Replace code examples with snippets from scripts under test -->

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Choose and import a base Expectation class:

  ```python title="Python"
  from great_expectations.expectations import ExpectColumnValueToBeBetween
  ```

  You can customize any of the core Expectation classes in GX. You can view the available Expectations and their functionality in the [Expectation Gallery](https://greatexpectations.io/expectations).

2. Create a new Expectation class that inherits the base Expectation class.
  
  The core Expectations in GX have names descriptive of their functionality.  When you create a customized Expectation class you can provide a class name that is more indicative of your specific use case:

  ```python title="Python"
  class ExpectValidPassengerCount(ExpectColumnValueToBeBetween):
  ```

3. Override the Expectation's attributes with new default values:

  ```python title="Python"
  class ExpectValidPassengerCount(ExpectColumnValueToBeBetween):
      column: str = "passenger_count"
      min_value: int = 0
      max_value: int = 6
  ```

  The attributes that can be overriden correspond to the parameters required by the base Expectation.  These can be referenced from the [Expectation Gallery](https://greatexpectations.io/expectations).

4. Customize the rendering of the new Expectation when displayed in Data Docs:

  ```python title="Python"
  class ExpectValidPassengerCount(ExpectColumnValueToBeBetween):
      column: str = "passenger_count"
      min_value: int = 0
      max_value: int = 6
      render_text: str = "There should be between **0** and **6** passengers."
  ```

  The `render_text` attribute contains the text describing the customized Expectation when your results are rendered into Data Docs.  You can format the text with Markdown syntax.

</TabItem>

<TabItem value="example" label="Sample code">

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
- [Manage Expectation Suites](/core/_create_expectations/expectation_suites/manage_expectation_suites.md)