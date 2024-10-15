---
title: Connect to dataframe data
description: Connect to data in pandas or Spark Dataframes organize that data into Batches for retrieval and validation.
hide_feedback_survey: false
hide_title: false
toc_max_heading_level: 2
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../../_core_components/prerequisites/_gx_installation.md'
import PrereqSparkIfNecessary from '../../_core_components/prerequisites/_optional_spark_installation.md'
import PrereqDataContext from '../../_core_components/prerequisites/_preconfigured_data_context.md'

A dataframe is a set of data that resides in-memory and is represented in your code by a variable to which it is assigned.  To connect to this in-memory data you will define a Data Source based on the type of dataframe you are connecting to, a Data Asset that connects to the dataframe in question, and a Batch Definition that will return all of the records in the dataframe as a single Batch of data.

## Create a Data Source

Because the dataframes reside in memory you do not need to specify the location of the data when you create your Data Source.  Instead, the type of Data Source you create depends on the type of dataframe containing your data. Great Expectations has methods for connecting to both pandas and Spark dataframes.  

### Prerequisites

- <PrereqPythonInstalled/>
- <PrereqGxInstalled/>
  - Optional. <PrereqSparkIfNecessary/>.
- <PrereqDataContext/>.  These examples assume the variable `context` contains your Data Context.

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

1. Define the Data Source parameters.

   A dataframe Data Source requires the following information:

   - `name`: A name by which to reference the Data Source.  This should be unique among all Data Sources on the Data Context.

   Update `data_source_name` in the following code with a descriptive name for your Data Source:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_source.py - define Data Source name"
   ```

2. Create the Data Source.

   To read a pandas dataframe you will need to create a pandas Data Source.  Likewise, to read a Spark dataframe you will need to create a Spark Data Source.

   <Tabs queryString="execution_engine" groupId="execution_engine" defaultValue='pandas'>

      <TabItem value="pandas" label="pandas">

      Execute the following code to create a pandas Data Source:

      ```Python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_source.py Add Data Source"
      ```

      </TabItem>

      <TabItem value="spark" label="Spark">

      Execute the following code to create a Spark Data Source:

      ```Python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_source.py Add Data Source"
      ```

      </TabItem>

   </Tabs>

</TabItem>

<TabItem value="sample_code" label="Sample code">

   <Tabs queryString="execution_engine" groupId="execution_engine" defaultValue='pandas'>

   <TabItem value="pandas" label="pandas">

   ```Python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_source.py - full example"
   ```

   </TabItem>

   <TabItem value="spark" label="Spark">

   ```Python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_source.py - full example"
   ```

   </TabItem>

   </Tabs>

</TabItem>

</Tabs>

## Create a Data Asset

A dataframe Data Asset is used to group your Validation Results.  For instance, if you have a data pipeline with three stages and you wanted the Validation Results for each stage to be grouped together, you would create a Data Asset with a unique name representing each stage.

### Prerequisites

- <PrereqPythonInstalled/>
- <PrereqGxInstalled/>
  - Optional. <PrereqSparkIfNecessary/>.
- <PrereqDataContext/>.  These examples assume the variable `context` contains your Data Context.
- A [pandas or Spark dataframe Data Source](#create-a-data-source).

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

1. Optional. Retrieve your Data Source.

   If you do not already have a variable referencing your pandas or Spark Data Source, you can retrieve a previously created one with:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_asset.py - retrieve Data Source"
   ```

2. Define the Data Asset's parameters.

   A dataframe Data Asset requires the following information:

   - `name`: A name by which the Data Asset can be referenced.  This should be unique among Data Assets on the Data Source.

   Update the `data_asset_name` parameter in the following code with a descriptive name for your Data Asset:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_asset.py - define Data Asset name"
   ```

3. Add a Data Asset to the Data Source.

   Execute the following code to add a Data Asset to your Data Source:

   ```title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_asset.py - add Data Asset"
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_asset.py - full code example"
   ```

</TabItem>

</Tabs>

## Create a Batch Definition

Typically, a Batch Definition is used to describe how the data within a Data Asset should be retrieved.  With dataframes, all of the data in a given dataframe will always be retrieved as a Batch.

This means that Batch Definitions for dataframe Data Assets don't work to subdivide the data returned for validation.  Instead, they serve as an additional layer of organization and allow you to further group your Validation Results.  For example, if you have already used your dataframe Data Assets to group your Validation Results by pipeline stage, you could use two Batch Definitions to further group those results by having all automated validations use one Batch Definition and all manually executed validations use the other.


### Prerequisites

- <PrereqPythonInstalled/>
- <PrereqGxInstalled/>
  - Optional. <PrereqSparkIfNecessary/>.
- <PrereqDataContext/>.  These examples assume the variable `context` contains your Data Context.
- A [pandas or Spark dataframe Data Asset](#create-a-data-asset).

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

1. Optional. Retrieve your Data Asset.

   If you do not already have a variable referencing your pandas or Spark Data Asset, you can retrieve a previously created Data Asset with:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_batch_definition.py - retrieve Data Asset"
   ```

2. Define the Batch Definition's parameters.

   A dataframe Batch Definition requires the following information:

   - `name`: A name by which the Batch Definition can be referenced.  This should be unique among Batch Definitions on the Data Asset.

   Because dataframes are always provided in their entirety, dataframe Batch Definitions always use the `add_batch_definition_whole_dataframe()` method.

   Update the value of `batch_definition_name` in the following code with something that describes your dataframe:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_batch_definition.py - define Batch Definition name"
   ```

3. Add the Batch Definition to the Data Asset.

   Execute the following code to add a Batch Definition to your Data Asset:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_batch_definition.py - add Batch Definition"
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_batch_definition.py - full code example"
   ```

</TabItem>

</Tabs>

## Provide a dataframe through Batch Parameters

Because dataframes exist in memory and cease to exist when a Python session ends the dataframe itself is not saved as part of a Data Assset or Batch Definition.  Instead, a dataframe created in the current Python session is passed in at runtime as a Batch Parameter dictionary.

### Prerequisites

- <PrereqPythonInstalled/>
- <PrereqGxInstalled/>
  - Optional. <PrereqSparkIfNecessary/>.
- <PrereqDataContext/>.  These examples assume the variable `context` contains your Data Context.
- A [Batch Definition on a pandas or Spark dataframe Data Asset](#create-a-batch-definition).
- Data in a pandas or Spark dataframe.  These examples assume the variable `dataframe` contains your pandas or Spark dataframe.
- Optional. A Validation Definition.

### Procedure

1. Define the Batch Parameter dictionary.

   A dataframe can be added to a Batch Parameter dictionary by defining it as the value of the dictionary key `dataframe`:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_batch_parameters_batch_definition.py - batch parameters example"
   ```

   The following examples create a dataframe by reading a `.csv` file and storing it in a Batch Parameter dictionary:

   <Tabs queryString="execution_engine" groupId="execution_engine" defaultValue='pandas'>

      <TabItem value="pandas" label="pandas">

      ```Python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_batch_parameters_batch_definition.py - pandas dataframe"
      ```

      </TabItem>

      <TabItem value="spark" label="Spark">

      ```Python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_batch_parameters_batch_definition.py - spark dataframe"
      ```

      </TabItem>

   </Tabs>

3. Pass the Batch Parameter dictionary to a `get_batch()` or `validate()` method call.

   Runtime Batch Parameters can be provided to the `get_batch()` method of a Batch Definition or to the `validate()` method of a Validation Definition.

   <Tabs>

   <TabItem value="batch" label="Batch Definition">

   The `get_batch()` method of a Batch Definition retrieves a single Batch of data.  Runtime Batch Parameters can be provided to the `get_batch()` method to specify the data returned as a Batch.  The `validate()` method of this Batch can then be used to test individual Expectations. 

   ```Python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_batch_parameters_batch_definition.py - batch.validate() example"
   ```

   The results generated by `batch.validate()` are not persisted in storage.  This workflow is solely intended for interactively creating Expectations and engaging in data Exploration.

   For further information on using an individual Batch to test Expectations see [Test an Expectation](/core/define_expectations/test_an_expectation.md).

   </TabItem>

   <TabItem value="validate" label="Validation Definition">

   A Validation Definition's `run()` method validates an Expectation Suite against a Batch returned by a Batch Definition.  Runtime Batch Parameters can be provided to a Validation Definition's `run()` method to specify the data returned in the Batch.  This allows you to validate your dataframe by executing the Expectation Suite included in the Validation Definition.

   ```Python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_batch_parameters_validation_definition.py - validation_definition.run() example"
   ```

   For more information on Validation Definitions see [Run Validations](/core/run_validations/run_validations.md).

   </TabItem>

   </Tabs>