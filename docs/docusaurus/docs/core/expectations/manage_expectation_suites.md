---
sidebar_label: 'Manage Expectation Suites'
title: 'Manage Expectation Suites'
description: Create and manage sets of Expectations in Python with GX Core.
---

An Expectation is a verifiable assertion about your data. Expectations make implicit assumptions about your data explicit, and they provide a flexible, declarative language for describing expected behavior. They can help you better understand your data and help you improve data quality. 

An Expectation Suite contains a group of Expectations that describe the same set of data.  All of the Expectations that you apply to your data will grouped into Expectation Suites.

## Prerequisites

This guide assumes you have:

- Installed Python.
- Installed the GX Core library.

## Create a new Expectation Suite

1. Import the GX Core library and the `ExpectationSuite` class.
  ```python name="tests/integration/docusaurus/core/expectations/create_an_expectation_suite.py imports"
  ```

2. Get a Data Context.
  ```python name="tests/integration/docusaurus/core/expectations/create_an_expectation_suite.py get_context"
  ```

3. Create an Expectation Suite.
  ```python name="tests/integration/docusaurus/core/expectations/create_an_expectation_suite.py create Expectation Suite"
  ```

4. Add the Expectation Suite to your Data Context.
  ```python name="tests/integration/docusaurus/core/expectations/create_an_expectation_suite.py add snippet to Data Context"
  ```

:::tip
You can add an Expectation Suite to your Data Context at the same time as you create the Expectation Suite with the following code:
```python name="tests/integration/docusaurus/core/expectations/create_an_expectation_suite.py create and add Expectation Suite to Data Context"
```
:::

<details><summary>Full example code</summary>
<p>

```python name="tests/integration/docusaurus/core/expectations/create_an_expectation_suite.py full example code"
```

</p>
</details>

## Get an existing Expectation Suite

1. Import the GX Core library.
  ```python name="tests/integration/docusaurus/core/expectations/get_an_expectation_suite.py imports"
  ```

2. Get a Data Context.
  ```python name="tests/integration/docusaurus/core/expectations/get_an_expectation_suite.py get_context"
  ```

3. Use the Data Context to retrieve the existing Expectation Suite.
  ```python name="tests/integration/docusaurus/core/expectations/get_an_expectation_suite.py create Expectation Suite"
  ```

<details><summary>Full example code</summary>
<p>

```python name="tests/integration/docusaurus/core/expectations/get_an_expectation_suite.py full example code"
```

</p>
</details>

## Modify an Expectation Suite
1. [Create a new](#create-a-new-expectation-suite) or [get an existing](#get-an-existing-expectation-suite) Expectation Suite.
   :::note
   The rest of this section assumes your Expectation Suite instance is stored in the variable `suite`.
   :::

2. Modify the Expectation Suite's attributes.
   ```python name="tests/integration/docusaurus/core/expectations/modify_an_expectation_suite.py edit attribute"
   ```

3. Save the Expectation Suite.
   ```python name="tests/integration/docusaurus/core/expectations/modify_an_expectation_suite.py save the Expectation"
   ```

<details><summary>Full example code</summary>
<p>

```python name="tests/integration/docusaurus/core/expectations/modify_an_expectation_suite.py full example code"
```

</p>
</details>

## Delete an Expectation Suite

1. Import the GX Core library.
  ```python name="tests/integration/docusaurus/core/expectations/delete_an_expectation_suite.py imports"
  ```

2. Get a Data Context.
  ```python name="tests/integration/docusaurus/core/expectations/delete_an_expectation_suite.py get_context"
  ```

3. Use the Data Context to delete an existing Expectation Suite.
  ```python name="tests/integration/docusaurus/core/expectations/delete_an_expectation_suite.py delete Expectation Suite"
  ```

<details><summary>Full example code</summary>
<p>

```python name="tests/integration/docusaurus/core/expectations/delete_an_expectation_suite.py full example code"
```

</p>
</details>

## Add Expectations to an Expectation Suite

1. Import the GX Core library and `expectations` module.
  ```python name="tests/integration/docusaurus/core/expectations/add_expectations_to_an_expectation_suite.py imports"
  ```

2. Get a Data Context.
  ```python name="tests/integration/docusaurus/core/expectations/add_expectations_to_an_expectation_suite.py get_context"
  ```

3. [Create a new](#create-a-new-expectation-suite) or [get an existing](#get-an-existing-expectation-suite) Expectation Suite.
  :::note
  The rest of this section assumes your Expectation Suite instance is stored in the variable `suite`.
  :::

4. Add Expectations to the Expectation Suite.
  ```python name="tests/integration/docusaurus/core/expectations/add_expectations_to_an_expectation_suite.py add Expectations"
  ```
  The specific parameters provided when adding an Expectation depend on the Expectation class.  You can view an Expectation and the parameters it takes in the Expectation Gallery.

<details><summary>Full example code</summary>
<p>

```python name="tests/integration/docusaurus/core/expectations/add_expectations_to_an_expectation_suite.py full example code"
```

</p>
</details>

## Get a specific Expectation from an Expectation Suite

1. Import the GX Core library and `expectations` module.
  ```python name="tests/integration/docusaurus/core/expectations/get_a_specific_expectation_from_an_expectation_suite.py imports"
  ```

2. Get a Data Context.
  ```python name="tests/integration/docusaurus/core/expectations/get_a_specific_expectation_from_an_expectation_suite.py get_context"
  ```

3. [Get an existing Expectation Suite](get-an-existing-expectation-suite) that contains Expectations, or [create a new Expectation Suite](#create-a-new-expectation-suite) and [add some Expectations to it](#add-expectations-to-an-expectation-suite).
  :::note
  The rest of this section assumes your Expectation Suite instance is stored in the variable `suite`.
  :::

4. Iterate through the Expectation Suite's Expectations and check Expectation class and attributes until the desired Expectation is found.
  ```python name="tests/integration/docusaurus/core/expectations/get_a_specific_expectation_from_an_expectation_suite.py retrieve expectation"
  ```

<details><summary>Full example code</summary>
<p>

```python name="tests/integration/docusaurus/core/expectations/get_a_specific_expectation_from_an_expectation_suite.py full example code"
```

</p>
</details>

## Edit a single Expectation in an Expectation Suite

1. [Get the Expectation to edit](#get-a-specific-expectation-from-an-expectation-suite) from its Expectation Suite.
  :::note
  The rest of this section assumes your retrieved Expectation is stored in the variable `expectation`.
  :::

2. Modify the Expectation's attributes.
  ```python name="tests/integration/docusaurus/core/expectations/edit_a_single_expectation.py edit attribute"
  ```

3. Save the modified Expectation.
  ```python name="tests/integration/docusaurus/core/expectations/edit_a_single_expectation.py save the Expectation"
  ```

<details><summary>Full example code</summary>
<p>

```python name="tests/integration/docusaurus/core/expectations/edit_a_single_expectation.py full example code"
```

</p>
</details>

## Edit all Expectations in an Expectation Suite

1. [Get an existing Expectation Suite](get-an-existing-expectation-suite) that contains Expectations, or [create a new Expectation Suite](#create-a-new-expectation-suite) and [add some Expectations to it](#add-expectations-to-an-expectation-suite).
  :::note
  The rest of this section assumes your Expectations Suite instance is stored in the variable `suite`.
  :::

2. Iterate through the Expectations in the Expectations Suite and apply the desired edits.
  ```python name="tests/integration/docusaurus/core/expectations/edit_all_expectations_in_an_expectation_suite.py modify Expectations"
  ```

3. Save the Expectation Suite and all modifications to the Expectations within it.
  ```python name="tests/integration/docusaurus/core/expectations/edit_all_expectations_in_an_expectation_suite.py save Expectation Suite"
  ```

<details><summary>Full example code</summary>
<p>

```python name="tests/integration/docusaurus/core/expectations/edit_all_expectations_in_an_expectation_suite.py full example code"
```

</p>
</details>

## Delete an Expectation from an Expectation Suite

1. [Get the Expectation to delete from its Expectation Suite](#get-a-specific-expectation-from-an-expectation-suite).
  :::note
  The rest of this section assumes your retrieved Expectation is stored in the variable `expectation` and your Expectation Suite is stored in the variable `suite`.
  :::

2. Use the Expectation Suite to delete the Expectation.
  ```python name="tests/integration/docusaurus/core/expectations/delete_an_expectation_in_an_expectation_suite.py delete the Expectation"
  ```

<details><summary>Full example code</summary>
<p>

```python name="tests/integration/docusaurus/core/expectations/delete_an_expectation_in_an_expectation_suite.py full example code"
```

</p>
</details>