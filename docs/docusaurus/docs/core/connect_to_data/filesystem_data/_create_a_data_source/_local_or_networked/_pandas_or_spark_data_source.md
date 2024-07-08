import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';


### Prerequisites

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Define the Data Source's parameters.

   The following information is required when you create a Filesystem Data Source:

   - `name`: A unique name for the Data Source.  This name can be used to retrieve the Data Source from the Data Context if needed.
   - `base_directory`: The path to the folder that contains the data files, or the root folder of the directory hierarchy that contains the data files.
   
   If you are using a File Data Context, you can provide a path that is relative to the Data Context's `base_directory`.  Otherwise, you should provide the absolute path to the folder that contains your data.

   In this example, a relative folder path is defined for a folder that happens to contain taxi trip data for New York City:

   ```python title="Python"
   # This path is relative to the `base_directory` of the Data Context.
   source_folder = "./data/taxi_yellow_tripdata_samples"

   data_source_name = "nyc_taxi_data"
   ```

2. Add a Filesystem Data Source to your Data Context.

   Once you have a folder path and name for your Data Source you can create it with:

   <!-- The queryString is set by the selected tab in the parent document (since the process is identical for pandas and Spark and only the code example needs to be changed. -->

   <Tabs className="hidden" queryString="data_source_type" groupId="data_source_type" defaultValue='pandas_filesystem'>

   <TabItem value="pandas_filesystem" label="pandas">

   ```python title="Python"
   datasource = gx.data_sources.add_pandas_filesystem(name=data_source_name, base_directory=source_folder)
   ```

   </TabItem>

   <TabItem value="spark" label="Spark">

   ```python title="Python"
   datasource = gx.data_sources.add_spark_filesystem(name=data_source_name, base_directory=source_folder)
   ```

   </TabItem>

   </Tabs>

3. Optional. Retrieve your Data Source from your Data Context.

   You can retrieve your Data Source elsewhere in your code with:

   ```python title="Python"
   datasource = context.data_sources.get("nyc_taxi_data")
   ```

   If you are using a File Data Context you can also retrieve your Data Source from the Data Context in future python sessions.

</TabItem>

<TabItem value="sample_code" label="Sample code">

   <!-- The queryString is set by the selected tab in the parent document (since the process is identical for pandas and Spark and only the code example needs to be changed. -->

   <Tabs className="hidden" queryString="data_source_type" groupId="data_source_type" defaultValue='pandas_filesystem'>

   <TabItem value="pandas_filesystem" label="pandas">

   ```python title="Python"
   import great_epectations as gx

   context = gx.get_context()

   # Define the Data Source's parameters:
   # This path is relative to the `base_directory` of the Data Context.
   source_folder = "./data/taxi_yellow_tripdata_samples"
   
   data_source_name = "nyc_taxi_data"
   
   # Create the Data Source:
   datasource = gx.data_sources.add_pandas_filesystem(name=data_source_name, base_directory=source_folder)

   # Retrieve the Data Source:
   datasource = context.data_sources.get("nyc_taxi_data")
   ```

   </TabItem>

   <TabItem value="spark" label="Spark">

   ```python title="Python"
   import great_epectations as gx

   context = gx.get_context()

   # Define the Data Source's parameters:
   # This path is relative to the `base_directory` of the Data Context.
   source_folder = "./data/taxi_yellow_tripdata_samples"
   
   data_source_name = "nyc_taxi_data"
   
   # Create the Data Source:
   datasource = gx.data_sources.add_spark_filesystem(name=data_source_name, base_directory=source_folder)

   # Retrieve the Data Source:
   datasource = context.data_sources.get("nyc_taxi_data")
   ```

   </TabItem>

   </Tabs>

</TabItem>

</Tabs>