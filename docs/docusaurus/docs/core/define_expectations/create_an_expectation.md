---
title: Create an Expectation
description: Create and modify an Expectation in Python.
---
import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../_core_components/prerequisites/_gx_installation.md';

import StandardArgumentsTable from './_expectations/_standard_arguments_table.md';

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

An Expectation is a verifiable assertion about your data. Expectations make implicit assumptions about your data explicit, and they provide a flexible, declarative language for describing expected behavior. They can help you better understand your data and help you improve data quality.

<h2>Prerequisites</h2>

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Choose an Expectation to create.

   GX comes with many built in Expectations to cover your data quality needs.  You can find a catalog of these Expectations in the [Expectation Gallery](https://greatexpectations.io/expectations/).  When browsing the Expectation Gallery you can filter the available Expectations by the data quality issue they address and by the Data Sources they support.  There is also a search bar that will let you filter Expectations by matching text in their name or docstring.

   In your code, you will find the classes for Expectations in the `expectations` module:

   ```python title="Python"
   from great_expectations import expectations as gxe
   ```

2. Determine the Expectation's parameters

   To determine the parameters your Expectation uses to evaluate data, reference the Expectation's entry in the [Expectation Gallery](https://greatexpectations.io/expectations/).  Under the **Args** section you will find a list of parameters that are necessary for the Expectation to be evaluated, along with the a description of the value that should be provided.

   Parameters that indicate a column, list of columns, or a table must be provided when the Expectation is created.  The value in these parameters is used to differentiate instances of the same Expectation class.  All other parameters can be set when the Expectation is created or be assigned a dictionary lookup that will allow them to be set at runtime.
   
3. Optional. Determine the Expectation's other parameters

   In addition to the parameters that are required for an Expectation to evaluate data all Expectations also support some standard parameters that determine how strictly Expectations are evaluated and permit the addition of metadata.  In the Expectations Gallery these are found under each Expectation's **Other Parameters** section.

   These parameters are:

   <StandardArgumentsTable/>

4. Create the Expectation.
  
   Using the Expectation class you picked and the parameters you determined when referencing the Expectation Gallery, you can create your Expectation.

   <Tabs queryString="expectation_parameters" groupId="expectation_parameters" defaultValue='preset' values={[{label: 'Preset parameters', value:'preset'}, {label: 'Runtime parameters', value:'runtime'}]}>

   <TabItem value="preset" label="Preset parameters">
   
      In this example the `ExpectColumnMaxToBeBetween` Expectation is created and all of its parameters are defined in advance while leaving `strict_min` and `strict_max` as their default values:

      ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_examples/create_an_expectation.py - preset expectation"
      ```

   </TabItem>
   
   <TabItem value="runtime" label="Runtime parameters">

      Runtime parameters are provided by passing a dictionary to the `expectation_parameters` argument of a Checkpoint's `run()` method.
      
      To indicate which key in the `expectation_parameters` dictionary corresponds to a given parameter in an Expectation you define a lookup as the value of the parameter when the Expectation is created.  This is done by passing in a dictionary with the key `$PARAMETER` when the Expectation is created.  The value associated with the `$PARAMETER` key is the lookup used to find the parameter in the runtime dictionary.

      In this example, `ExpectColumnMaxToBeBetween` is created for both the `passenger_count` and the `fare` fields, and the values for `min_value` and `max_value` in each Expectation will be passed in at runtime.  To differentiate between the parameters for each Expectation a more specific key is set for finding each parameter in the runtime `expectation_parameters` dictionary:
   
      ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_examples/create_an_expectation.py - dynamic expectations"
      ```

      The runtime `expectation_parameters` dictionary for the above example would look like:
   
      ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_examples/create_an_expectation.py - example expectation_parameters"
      ``` 

   </TabItem>

   </Tabs>

</TabItem>

<TabItem value="sample_code" label="Sample code">

   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_examples/create_an_expectation.py - full code example"
   ```

</TabItem>

</Tabs>

   

