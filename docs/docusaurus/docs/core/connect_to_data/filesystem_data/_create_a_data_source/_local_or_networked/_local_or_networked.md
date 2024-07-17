import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstall from '../../../../_core_components/prerequisites/_python_installation.md'
import PrereqGxInstall from '../../../../_core_components/prerequisites/_gx_installation.md'
import PrereqDataContext from '../../../../_core_components/prerequisites/_preconfigured_data_context.md'

import PandasDefault from './_pandas_default.md'

### Prerequisites
- <PrereqPythonInstall/>
- <PrereqGxInstall/>
  - Optional. To create a Spark Filesystem Data Source you will also need to [install the Spark Python dependencies](/core/set_up_a_gx_environment/install_additional_dependencies.md?dependencies=spark).
- <PrereqDataContext/>
- Access to data files in a local or networked directory.

:::info Quick access  to sample data
<PandasDefault/>
:::

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Define the Data Source's parameters.

   The following information is required when you create a Filesystem Data Source for a local or networked directory:

   - `name`: A descriptive name used to reference the Data Source.  This should be unique within the Data Context.
   - `base_directory`: The path to the folder that contains the data files, or the root folder of the directory hierarchy that contains the data files.
   
   If you are using a File Data Context, you can provide a path that is relative to the Data Context's `base_directory`.  Otherwise, you should provide the absolute path to the folder that contains your data.

   In this example, a relative folder path is defined for a folder that happens to contain taxi trip data for New York City:

   ```python title="Python"
   # This path is relative to the `base_directory` of the Data Context.
   source_folder = "./data/taxi_yellow_tripdata_samples"

   data_source_name = "my_filesystem_data_source"
   ```

2. Add a Filesystem Data Source to your Data Context.

   GX can leverage either pandas or Spark as the backend for your Filesystem Data Source.  To create your Data Source, execute one of the following sets of code:
 
   <Tabs queryString="data_source_type" groupId="data_source_type" defaultValue='pandas_filesystem'>

   <TabItem value="pandas_filesystem" label="pandas">

   ```python title="Python"
   data_source = gx.data_sources.add_pandas_filesystem(
      name=data_source_name,
      base_directory=source_folder
   )
   ```

   </TabItem>

   <TabItem value="spark" label="Spark">

   ```python title="Python"
   data_source = gx.data_sources.add_spark_filesystem(
      name=data_source_name,
      base_directory=source_folder
   )
   ```

   </TabItem>

   </Tabs>

5. Optional. Retrieve your Data Source from your Data Context.

   You can retrieve your Data Source elsewhere in your code by updating the value of `data_source_name` and executing:

   ```python title="Python"
   data_source_name="nyc_taxi_data"
   data_source = context.data_sources.get(data_source_name)
   ```

   If you are using a File Data Context your Data Source can also be retrieved from the Data Context in future python sessions.

</TabItem>

<TabItem value="sample_code" label="Sample code">

   Choose from the following to see the full example code for a local or networked Data Source, using either pandas or Spark to read the data files:

   <Tabs queryString="data_source_type" groupId="data_source_type" defaultValue='pandas_filesystem'>

   <TabItem value="pandas_filesystem" label="pandas example">

   ```python title="Python"
   import great_epectations as gx

   context = gx.get_context()

   # Define the Data Source's parameters:
   # This path is relative to the `base_directory` of the Data Context.
   source_folder = "./data/taxi_yellow_tripdata_samples"

   data_source_name = "my_filesystem_data_source"
   
   # Create the Data Source:
   data_source = gx.data_sources.add_pandas_filesystem(
      name="nyc_taxi_data",
      base_directory=source_folder
   )
   
   # Retrieve the Data Source:
   data_source = context.data_sources.get("nyc_taxi_data")
   ```

   </TabItem>

   <TabItem value="spark" label="Spark example">

   ```python title="Python"
   import great_epectations as gx

   context = gx.get_context()

   # Define the Data Source's parameters:
   # This path is relative to the `base_directory` of the Data Context.
   source_folder = "./data/taxi_yellow_tripdata_samples"
   
   data_source_name = "my_filesystem_data_source"
   
   # Create the Data Source:
   data_source = gx.data_sources.add_spark_filesystem(
      name=data_source_name,
      base_directory=source_folder
   )

   # Retrieve the Data Source:
   data_source = context.data_sources.get("nyc_taxi_data")
   ```

   </TabItem>

   </Tabs>

</TabItem>

</Tabs>

