import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

### Prerequisites
- Python install
- GX install
- Data Context
- Access to data files in a local or networked folder hierarchy.
- A pandas or Spark Filesystem Data Source configured for the local or networked data files.

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Define your Data Asset's parameters.

   A File Data Asset for files in a local or networked folder hierarchy only needs one piece of information to be created.

   - `name`: A descriptive name with which to reference the Data Asset.  This name should be unique among all Data Assets for the same Data Source.

   This example uses taxi trip data stored in `.csv` files, so the name `"taxi_csv_files"` will be used for the Data Asset: 

   ```python
   asset_name="taxi_csv_files"
   ```

2. Add the Data Asset to your Data Source.

   A new Data Asset is created and added to a Data Source simultaneously.  The file format that the Data Asset can read is determined by the method used when the Data Asset is added to the Data Source.

   - To see the file formats supported by a pandas File Data Source, reference the `.add_*_asset(...)` methods in [the API documentation for a `PandasFilesystemDatasource`](/reference/api/datasource/fluent/PandasFilesystemDatasource_class.mdx).
   - - To see the file formats supported by a Spark File Data Source, reference the `.add_*_asset(...)` methods in [the API documentation for a `SparkFilesystemDatasource`](/reference/api/datasource/fluent/SparkFilesystemDatasource_class.mdx).

   The following example creates a Data Asset that can read `.csv` file data:

   ```python
   file_csv_asset = data_source.add_csv_asset(name=asset_name)
   ```
   
4. Optional. Retrieve the Data Asset from your Data Source.

   You can retrieve your Data Asset from the Data Context by updating `data_source_name` with the name of your Data Source and `asset_name` with the name of your Data Asset before executing the following:

   ```python
   data_source_name = "nyc_taxi_data"
   asset_name = "taxi_csv_files"
   file_csv_asset = context.get_data_source(data_source_name).get_asset(asset_name)
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

   ```python
   import great_expectations as gx
   
   # This example uses a File Data Context which already has
   #  a Data Source defined.
   context = gx.get_context()

   data_source_name = "nyc_taxi_data"
   data_source = context.get_datasource(data_source_name)

   # Define the Data Asset's parameters:
   asset_name = "taxi_csv_files"

   # Add the Data Asset to the Data Source:
   file_csv_asset = data_source.add_csv_asset(name=asset_name)

   # Use the Data Context to retrieve the Data Asset when needed:
   data_source_name = "nyc_taxi_data"
   asset_name = "taxi_csv_files"
   file_csv_asset = context.get_data_source(data_source_name).get_asset(asset_name)
   ```

</TabItem>

</Tabs>