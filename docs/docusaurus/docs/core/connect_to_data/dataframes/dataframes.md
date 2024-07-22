---
title: Connect to data in dataframes
description: Follow this guide to connect to a pandas or Spark dataframe in GX.
hide_feedback_survey: false
hide_title: false
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../../_core_components/prerequisites/_gx_installation.md'
import PrereqSparkIfNecessary from '../../_core_components/prerequisites/_optional_spark_installation.md'
import PrereqDataContext from '../../_core_components/prerequisites/_preconfigured_data_context.md'

A dataframe is a set of data that resides in-memory and is represented in your code by a variable to which it is assigned.  Great Expectations has methods for connecting to both pandas and Spark dataframes.

## Create a Data Source



### Prerequisites

- <PrereqPythonInstalled/>
- <PrereqGxInstalled/>
  - Optional. <PrereqSparkIfNecessary/>.
- Data in a pandas or Spark dataframe.  These examples assume the variable `dataframe` contains your pandas or Spark dataframe.
- <PrereqDataContext/>.  These examples assume the variable `context` contains your Data Context.

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Define the Data Source parameters.

   A dataframe Data Source requires the following information:

   - `name`: A name by which to reference the Data Source.  This should be unique among all Data Sources on the Data Context.

   Because the dataframe itself resides in memory and is referenced in your code by the variable it is assigned to you do not need to specify the location of the data when you create your Data Source or provide any other parameters.

   Update `data_source_name` in the following code with a descriptive name for your Data Source:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_source.py - define Data Source parameters"
   ```

2. Create the Data Source.

   To read a pandas dataframe you will need to create a pandas Data Source.  Likewise, to read a Spark dataframe you will need to create a Spark Data Source.

   <Tabs queryString="execution_engine" groupId="execution_engine" defaultValue='pandas'>

      <TabItem value="pandas" label="pandas">

      Execute the following code to create a pandas Data Source:

      ```Python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_source.py - create Data Source"
      ```

      </TabItem>

      <TabItem value="spark" label="Spark">

      Execute the following code to create a Spark Data Source:

      ```Python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_source.py - create Data Source"
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

## Create a Data Asset and Batch Definition

To access data from your dataframe in GX you will connect to the dataframe with a Data Asset.  Then you will define a Batch Definition with which the data can be retrieved.

Because dataframes exist in memory and cease to exist when the Python session ends a dataframe Data Asset will need to be created anew in every Python session that utilizes it.

### Prerequisites

- <PrereqPythonInstalled/>
- <PrereqGxInstalled/>
  - Optional. <PrereqSparkIfNecessary/>.
- Data in a pandas or Spark dataframe.  These examples assume the variable `dataframe` contains your pandas or Spark dataframe.
- <PrereqDataContext/>.  These examples assume the variable `context` contains your Data Context.
- - A [pandas or Spark dataframe Data Source](#create-a-data-source).

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Optional. Retrieve your Data Source.

   If you do not already have a variable referencing your pandas or Spark Data Source, you can retrieve a previously created one with:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_asset.py - retrieve Data Source"
   ```

2. Define the Data Asset's parameters.

   A dataframe Data Asset requires the following information:

   - `name`: A name by which the Data Asset can be referenced.  This should be unique among Data Assets on the Data Source.
   - `dataframe`: The pandas or Spark dataframe that the Data Asset should retrieve data from.

   The following examples create a dataframe by reading a `.csv` file and defines a name for the Data Asset:

   <Tabs queryString="execution_engine" groupId="execution_engine" defaultValue='pandas'>

      <TabItem value="pandas" label="pandas">

      ```Python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_asset.py - define Data Asset parameters"
      ```

      </TabItem>

      <TabItem value="spark" label="Spark">

      ```Python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_asset.py - define Data Asset parameters"
      ```

      </TabItem>

   </Tabs>

3. Add the Data Asset to the Data Source.

   Execute the following code to create a dataframe Data Asset and add it to your Data Source:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_asset.py - create Data Asset"
   ```

4. Add a Batch Definition to the Data Asset.

   Dataframe Data Assets do not support further partitioning into Batches.  A Batch Definition for a dataframe Data Asset will always have a single Batch available which contains all of the records in the Data Asset.  Because of this you only need to provide a name when defining a dataframe Batch Definition:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_asset.py - add Batch Definition"
   ```

5. Optional. Verify the Batch Definition.

   You can verify that your Batch Definition can retrieve data from your dataframe by requesting the available Batch and printing the first few records:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_asset.py - verify Batch Definition"
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

   <Tabs queryString="execution_engine" groupId="execution_engine" defaultValue='pandas'>

      <TabItem value="pandas" label="pandas">

      ```Python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_asset.py - full example"
      ```

      </TabItem>

      <TabItem value="spark" label="Spark">

      ```Python title="Python" name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_asset.py - full example"
      ```

      </TabItem>

   </Tabs>

</TabItem>

</Tabs>

