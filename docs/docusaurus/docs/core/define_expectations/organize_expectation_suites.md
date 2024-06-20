---
title: Organize Expectations into an Expectation Suite
description: Create and populate a GX Core Expectation Suite with Python.
hide_table_of_contents: true
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../_core_components/prerequisites/_gx_installation.md';
import PrereqPreconfiguredDataContext from '../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqPreconfiguredDataSourceAndAsset from '../_core_components/prerequisites/_data_source_and_asset_connected_to_data.md';

An Expectation Suite contains a group of Expectations that describe the same set of data.  Combining all the Expectations that you apply to a given set of data into an Expectation Suite allows you to evaluate them as a group, rather than individually.  All of the Expectations that you use to validate your data in production workflows should be grouped into Expectation Suites.

## Prerequisites

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- Recommended. <PrereqPreconfiguredDataContext/>.
- Recommended. <PrereqPreconfiguredDataSourceAndAsset/>.


<Tabs>

<TabItem value="procedure" label="Procedure">

1. Retrieve or create a Data Context.

   In this procedure, your Data Context is stored in the variable `context`.  For information on creating or connecting to a Data Context see [Create a Data Context](/core/set_up_a_gx_environment/create_a_data_context.md).

2. Create an Expectation Suite.

   To create a new Expectation Suite you first need to import the `ExpectationSuite` class:

   ```python title="Python input"
   from great_expectations.core.expectation_suite import ExpectationSuite
   ```
   
   Next, you will provide a descriptive name and instantiate the `ExpectationSuite` class.  In the following code update the variable `suite_name` with a a name relevant to your data.  Then execute the code:

   ```python title="Python input"
   suite_name = "my_expectation_suite"
   suite = ExpectationSuite(name=suite_name)
   ```

4. Create an Expectation.

   In this procedure, your Expectation is stored in the variable `expectation`.  For information on creating an Expectation see [Create an Expectation](./create_an_expectation.md).

5. Add the Expectation to the Expectation Suite.

   An Expectation Suite's `add_expectation(...)` method takes in an instance of an Expectation and adds it to the Expectation Suite's configuraton: 

   ```python title="Python input"
   suite.add_expectation(expectation)
   ```
   
   You can also instantiate an Expectation at the same time as you add it to an Expectation Suite:

   ```python title="Python input"
   import great_expectations.expectations as gxe
   
   suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))
   ```

   If you have a configured Data Source, Data Asset, and Batch Definition you can test your Expectation before adding it to your Expectation Suite.  To do this see [Test an Expectation](./test_an_expectation.md).

   However, if you test and modify an Expectation _after_ you have added it to your Expectation Suite you must explicitly save those modifications before they will be pushed to the Expectation Suite itself:

   ```python title="Python input"
   expectation.column = "pickup_location_id"
   expectation.save()
   ```

6. Continue to create and add additional Expectations
   
   Repeat the process of creating, testing, and adding Expectations to your Expectation Suite until the Expectation Suite adequately describes your data's ideal state.

7. Add the Expectation Suite to your Data Context

   Once you have finalized the contents of your Expectation Suite you should save it to your Data Context:  

   ```python title="Python input"
   suite = context.suites.add(suite)
   ```

   With a File or GX Cloud Data Context your saved Expectation Suite will be available between Python sessions.  You can retrieve your Expectation Suite from your Data Context with the following code:

   ```python title="Python input"
   existing_suite_name = "my_expectation_suite"  # replace this with the name of your Expectation Suite
   suite = context.suites.get(name=existing_suite_name)
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python input"
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
import great_expectations.expectations as gxe

context = gx.get_context()
expectation = gxe.ExpectColumnValuesToNotBeNull(column="passenger_count")

# Create an Expectation Suite
suite_name = "my_expectation_suite"
suite = ExpectationSuite(name=suite_name)

# Add a previously created Expectation to the Expectation Suite
suite.add_expectation(expectation)

# Add an Expectation to the Expectation Suite when it is created
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pickup_datetime"))

# Update the configuration of an Expectation, then push the changes to the Expectation Suite
expectation.column = "pickup_location_id"
expectation.save()

# Retrieve an Expectation Suite from the Data Context
existing_suite_name = "my_expectation_suite"  # replace this with the name of your Expectation Suite
suite = context.suites.get(name=existing_suite_name)
```


</TabItem>

</Tabs>