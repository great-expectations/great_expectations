---
sidebar_label: 'Manage Expectation Suites'
title: 'Manage Expectation Suites'
description: Create and manage GX Core Expectation Suites with Python.
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../../_core_components/prerequisites/_gx_installation.md';
import PrereqPreconfiguredDataContext from '../../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqPreconfiguredDataSourceAndAsset from '../../_core_components/prerequisites/_data_source_and_asset_connected_to_data.md';

An Expectation Suite contains a group of Expectations that describe the same set of data.  All the Expectations that you apply to your data are grouped into an Expectation Suite.

## Prerequisites

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- Recommended. <PrereqPreconfiguredDataContext/>.
- Recommended. <PrereqPreconfiguredDataSourceAndAsset/>.

## Create an Expectation Suite

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Import the GX Core library and the `ExpectationSuite` class:

  ```python title="Python code" name="core/expectation_suites/_examples/create_an_expectation_suite.py imports"
  ```

2. [Get a Data Context](/core/installation_and_setup/manage_data_contexts.md).

  In this example the variable `context` is your Data Context.

3. Create an Expectation Suite:

  ```python title="Python code" name="core/expectation_suites/_examples/create_an_expectation_suite.py create Expectation Suite"
  ```

4. Add the Expectation Suite to your Data Context:

  ```python title="Python code" name="core/expectation_suites/_examples/create_an_expectation_suite.py add snippet to Data Context"
  ```

:::tip

You can add an Expectation Suite to your Data Context at the same time as you create the Expectation Suite with the following code:

```python title="Python code" name="core/expectation_suites/_examples/create_an_expectation_suite.py create and add Expectation Suite to Data Context"
```

:::

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python" name="core/expectation_suites/_examples/create_an_expectation_suite.py full example code"
```

</TabItem>

</Tabs>

## Get an existing Expectation Suite

<Tabs>

<TabItem value="procedure" label="Procedure">

1. [Get a Data Context](/core/installation_and_setup/manage_data_contexts.md).

  In this example the variable `context` is your Data Context.

2. Use the Data Context to retrieve the existing Expectation Suite:

  ```python title="Python code" name="core/expectation_suites/_examples/get_an_expectation_suite.py create Expectation Suite"
  ``` 

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python code" name="core/expectation_suites/_examples/get_an_expectation_suite.py full example code"
```

</TabItem>

</Tabs>


## Delete an Expectation Suite

<Tabs>

<TabItem value="procedure" label="Procedure">

1. [Get a Data Context](/core/installation_and_setup/manage_data_contexts.md).

  In this example the variable `context` is your Data Context.

2. Get the Expectation Suite to delete from the Data Context:

  ```python title="Python code" name="core/expectation_suites/_examples/delete_an_expectation_suite.py get Expectation Suite"
  ```

3. Use the Data Context to delete the retrieved Expectation Suite:

  ```python title="Python code" name="core/expectation_suites/_examples/delete_an_expectation_suite.py delete Expectation Suite"
  ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python code" name="core/expectation_suites/_examples/delete_an_expectation_suite.py full example code"
```

</TabItem>

</Tabs>


## Add Expectations to an Expectation Suite

<Tabs>

<TabItem value="procedure" label="Procedure">

1. [Create a new](#create-a-new-expectation-suite) or [get an existing](#get-an-existing-expectation-suite) Expectation Suite.

  In this example the variable `suite` is your Expectation Suite.

2. [Create an Expectation](/core/create_expectations/expectations/manage_expectations.md#create-an-expectation).
  
  In this example the variable `expectation` is the Expectation to add to the Expectation Suite.

3. Add the Expectation to the Expectation Suite:

  ```python title="Python code" name="core/expectation_suites/_examples/add_expectations_to_an_expectation_suite.py add an Expectation to an Expectation Suite"
  ```

  :::tip 
  
  You can create an Expectation at the same time as you add it to the Expectation Suite:

  ```python title="Python code" name="core/expectation_suites/_examples/add_expectations_to_an_expectation_suite.py create and add an Expectation"
  ```
  
  :::

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python code" name="core/expectation_suites/_examples/add_expectations_to_an_expectation_suite.py full example code"
```

</TabItem>

</Tabs>

## Get an Expectation from an Expectation Suite

<Tabs>

<TabItem value="procedure" label="Procedure">

1. [Get an existing Expectation Suite](#get-an-existing-expectation-suite) that contains Expectations or [add some Expectations to a new Expectation Suite](#add-expectations-to-an-expectation-suite).

  In this example the variable `suite` is your Expectation Suite.

2. Find the desired Expectation by iterating through the Expectations in the Expectation Suite and comparing classes and attributes to those of the desired Expectation:

  ```python title="Python code" name="core/expectation_suites/_examples/get_a_specific_expectation_from_an_expectation_suite.py retrieve expectation"
  ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python code" name="core/expectation_suites/_examples/get_a_specific_expectation_from_an_expectation_suite.py full example code"
```

</TabItem>

</Tabs>

## Edit a single Expectation in an Expectation Suite

<Tabs>

<TabItem value="procedure" label="Procedure">

1. [Get the Expectation to edit](#get-a-specific-expectation-from-an-expectation-suite) from its Expectation Suite.

  In this example the variable `expectation` is the Expectation you want to edit.

2. [Modify the Expectation](/core/create_expectations/expectations/manage_expectations.md#modify-an-expectation-in-an-expecatation-suite):

  ```python title="Python code" name="core/expectation_suites/_examples/edit_a_single_expectation.py edit attribute"
  ```

4. Save the modified Expectation in the Expectation Suite:

  ```python title="Python code" name="core/expectation_suites/_examples/edit_a_single_expectation.py save the Expectation"
  ```

  `expectation.save()` is explicitly used to update the configuration of an Expectation in an Expectation Suite.
  
  An Expectation Suite continues to use the Expectation's original values unless you save your modifications. You can [test changes to an Expectation](/core/create_expectations/expectations/manage_expectations.md#test-an-expectation) without running `expectation.save()`, but those changes will not persist in the Expectation Suite until `expectation.save()` is run.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python code" name="core/expectation_suites/_examples/edit_a_single_expectation.py full example code"
```

</TabItem>

</Tabs>

## Edit multiple Expectations in an Expectation Suite

<Tabs>

<TabItem value="procedure" label="Procedure">

1. [Get an existing Expectation Suite](#get-an-existing-expectation-suite) that contains Expectations, or [add some Expectations to a new Expectation Suite](#add-expectations-to-an-expectation-suite). 

  In this example the variable `suite` is your Expectation Suite.

2. Modify multiple Expectations in the Expectation Suite:

  ```python title="Python code" name="core/expectation_suites/_examples/edit_all_expectations_in_an_expectation_suite.py modify Expectations"
  ```

3. Save the Expectation Suite and all modifications to the Expectations within it:

  ```python title="Python code" name="core/expectation_suites/_examples/edit_all_expectations_in_an_expectation_suite.py save Expectation Suite"
  ```  

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python code" name="core/expectation_suites/_examples/edit_all_expectations_in_an_expectation_suite.py full example code"
```

</TabItem>

</Tabs>

## Delete an Expectation from an Expectation Suite

<Tabs>

<TabItem value="procedure" label="Procedure">

1. [Get the Expectation Suite containing the Expectation to delete](#get-an-existing-expectation-suite).

  In this example the variable `suite` is the Expectation Suite containing the Expectation to delete.

3. [Get the Expectation to delete from its Expectation Suite](#get-a-specific-expectation-from-an-expectation-suite).

  In this example the variable `expectation` is the Expectation to delete.

5. Use the Expectation Suite to delete the Expectation:

  ```python title="Python code" name="core/expectation_suites/_examples/delete_an_expectation_in_an_expectation_suite.py delete the Expectation"
  ```  

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python code" name="core/expectation_suites/_examples/delete_an_expectation_in_an_expectation_suite.py full example code"
```

</TabItem>

</Tabs>