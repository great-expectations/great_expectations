---
sidebar_label: 'Manage Expectations'
title: 'Manage Expectations'
description: Create and manage individual Expectations in Python with GX Core.
---

An Expectation is a verifiable assertion about your data. Expectations make implicit assumptions about your data explicit, and they provide a flexible, declarative language for describing expected behavior. They can help you better understand your data and help you improve data quality. 

## Prerequisites


- Installed Python.
- Installed the GX Core library.

## Create an Expectation

1. Import the `expectations` module from the GX Core library:
   
  ```python title="Python code" name="tests/integration/docusaurus/core/expectations/create_an_expectation.py imports"
  ```

2. Initialize an Expectation class with the required parameters for that Expectation:

  ```python title="Python code" name="tests/integration/docusaurus/core/expectations/create_an_expectation.py initialize Expectations"
)
  ```

  The specific parameters you provide when initializing an Expectation are determined by the Expectation class.  You can view available Expectations and the parameters they take in the [Expectation Gallery](https://greatexpectations.io/expectations).

<details>
<summary>Full example code</summary>
<p>

```python title="Python code" name="tests/integration/docusaurus/core/expectations/create_an_expectation.py full example code"
```

</p>
</details>

## Test an Expectation

<!-- TODO: Replace the sample code with snippets from example scripts under test -->

1. Retrieve a Batch of data to test the Expectation against.

  :::note
  
  The rest of this section assumes the variable `batch` is your Batch of data.

  :::

2. Get the Expectation to test.  This could be a [newly created](#create-an-expectation) Expectation, an Expectation [retrieved from an Expectation Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md#get-an-expectation-from-an-expectation-suite), or a pre-existing Expectation from your code.
  
  :::note

  The rest of this section assumes the variable `expectation` is the Expectation you wish to test.

  :::

3. Validate the Expectation against the Batch:

  ```python title="Python code"
validation_result = batch.validate(expectation)
  ```

4. (Optional) [Modify the Expectation](#modify-an-expectation) and test it again.
 
5. (Optional) [Add the Expectation to an Expectation Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md#add-expectations-to-an-expectation-suite).
   
  :::caution 

  Expectations do not persist between Python sessions unless they are saved as part of an Expectation Suite.

  :::

## Modify an Expectation

1. Get the Expectation to modify.  This could be a [newly created](#create-an-expectation) Expectation that you wish to adjust, an Expectation [retrieved from an Expectation Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md#get-an-expectation-from-an-expectation-suite), or a pre-existing Expectation from your code.  

  This example uses an Expectation that was newly created in an Expectation Suite:

  ```python title="Python code" name="core/expectations/_examples/edit_an_expectation.py get expectation"
  ```

2. Modify the Expectation's attributes:

  ```python title="Python code" name="core/expectations/_examples/edit_an_expectation.py modify attributes"
  ```

  The specific attributes that can be modified correspond to the parameters used to initialize the Expectation.  You can view available Expectations and the parameters they take in the [Expectation Gallery](https://greatexpectations.io/expectations).

3. (Optional) If the Expectation belongs to an Expectation Suite, save the changes to the Expectation Suite:

  ```python title="Python code" name="core/expectations/_examples/edit_an_expectation.py save the Expectation"
  ```

  :::info

  `expectation.save()` is explicitly used to update the configuration of an Expectation in an Expectation Suite.  If the Expectation is not part of an Expectation Suite, `expectation.save()` will fail.

  :::

<details>
<summary>Full example code</summary>
<p>

```python title="Python code" name="core/expectations/_examples/edit_an_expectation.py full example code
```

</p>
</details>

## Customize an Expectation Class

<!-- TODO: Replace code examples with snippets from scripts under test -->

1. Choose and import a base Expectation class.

  You can view available Expectations in the [Expectation Gallery](https://greatexpectations.io/expectations).  This example will create a customized Expectation for a valid passenger count in taxi data out of the base Expectation `ExpectColumnValueToBeBetween`:

  ```python title="Python code"
from great_expectations.expectations import ExpectColumnValueToBeBetween
  ```

2. Create a new Expectation that inheirits the base Expectation class:

 ```python title="Python code"
class ExpectValidPassengerCount(ExpectColumnValueToBeBetween):
 ```

3. Override the Expectation's attributes with new default values:

  ```python title="Python code"
class ExpectValidPassengerCount(ExpectColumnValueToBeBetween):
    column: str = "passenger_count"
    min_value: int = 0
    max_value: int = 6
  ```

  The attributes that can be overriden correspond to the parameters required by the base Expectation.  These can be referenced from the [Expectation Gallery](https://greatexpectations.io/expectations).

4. Customize the rendering of the new Expectation when displayed in Data Docs.

  The `render_text` attribute contains the text describing the customized Expectation when your results are rendered into Data Docs.  This text can be formatted with Markdown syntax:

  ```python title="Python code"
class ExpectValidPassengerCount(ExpectColumnValueToBeBetween):
    column: str = "passenger_count"
    min_value: int = 0
    max_value: int = 6
    render_text: str = "There should be between **0** and **6** passengers."
  ```

## Next steps

- Create Custom SQL Expectations
- Manage Expectation Suites