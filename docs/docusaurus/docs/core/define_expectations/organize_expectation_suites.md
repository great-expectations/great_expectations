---
title: Organize Expectations into an Expectation Suite
description: Create and populate a GX Core Expectation Suite with Python.
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../_core_components/prerequisites/_gx_installation.md';
import PrereqPreconfiguredDataContext from '../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqPreconfiguredDataSourceAndAsset from '../_core_components/prerequisites/_data_source_and_asset_connected_to_data.md';

An Expectation Suite contains a group of Expectations that describe the same set of data.  Combining all the Expectations that you apply to a given set of data into an Expectation Suite allows you to evaluate them as a group, rather than individually.  All of the Expectations that you use to validate your data in production workflows should be grouped into Expectation Suites.

<h2>Prerequisites</h2>

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- Recommended. <PrereqPreconfiguredDataContext/>.
- Recommended. <PrereqPreconfiguredDataSourceAndAsset/>.

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

1. Retrieve or create a Data Context.

   In this procedure, your Data Context is stored in the variable `context`.  For information on creating or connecting to a Data Context see [Create a Data Context](/core/set_up_a_gx_environment/create_a_data_context.md).

2. Create an Expectation Suite.

   To create a new Expectation Suite you will instantiate the `ExpectationSuite` class, which is available from the `great_expectations` module.

   Each Expectation Suite will require a unique name.  In the following code update the variable `suite_name` with a a name relevant to your data.  Then execute the code to create your Expectation Suite:

   ```python title="Python input" name="docs/docusaurus/docs/core/define_expectations/_examples/organize_expectations_into_suites.py - create an Expectation Suite"
   ```

3. Add the Expectation Suite to your Data Context

   Once you have finalized the contents of your Expectation Suite you should save it to your Data Context:  

   ```python title="Python input" name="docs/docusaurus/docs/core/define_expectations/_examples/organize_expectations_into_suites.py - add Expectation Suite to the Data Context"
   ```

   With a File or GX Cloud Data Context your saved Expectation Suite will be available between Python sessions.  You can retrieve your Expectation Suite from your Data Context with the following code:

   ```python title="Python input" name="docs/docusaurus/docs/core/define_expectations/_examples/organize_expectations_into_suites.py - retrieve an Expectation Suite"
   ```

4. Create an Expectation.

   In this procedure, your Expectation is stored in the variable `expectation`.  For information on creating an Expectation see [Create an Expectation](./create_an_expectation.md).

5. Add the Expectation to the Expectation Suite.

   An Expectation Suite's `add_expectation(...)` method takes in an instance of an Expectation and adds it to the Expectation Suite's configuraton: 

   ```python title="Python input" name="docs/docusaurus/docs/core/define_expectations/_examples/organize_expectations_into_suites.py - add an Expectation in a variable to an Expectation Suite"
   ```

   If you have a configured Data Source, Data Asset, and Batch Definition you can test your Expectation before adding it to your Expectation Suite.  To do this see [Test an Expectation](./test_an_expectation.md).

   However, if you test and modify an Expectation _after_ you have added it to your Expectation Suite you must explicitly save those modifications before they will be pushed to the Expectation Suite's configuration:

   ```python title="Python input" name="docs/docusaurus/docs/core/define_expectations/_examples/organize_expectations_into_suites.py - update an Expectation and push changes to the Suite config"
   ```
   
   Because the `save()` method of a modified Expectation updates its Expectation Suite's configuration, the `save()` method will only function if the Expectation Suite has been added to your Data Context.

6. Continue to create and add additional Expectations
   
   Repeat the process of creating, testing, and adding Expectations to your Expectation Suite until the Expectation Suite adequately describes your data's ideal state.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python input" name="docs/docusaurus/docs/core/define_expectations/_examples/organize_expectations_into_suites.py - full code example"
```


</TabItem>

</Tabs>