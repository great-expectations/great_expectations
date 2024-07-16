import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstall from '../../../../_core_components/prerequisites/_python_installation.md'
import PrereqGxInstall from '../../../../_core_components/prerequisites/_gx_installation_with_s3_dependencies.md'
import PrereqDataContext from '../../../../_core_components/prerequisites/_preconfigured_data_context.md'
import PrereqSparkFilesystemDataSource from '../../../../_core_components/prerequisites/_data_source_spark_filesystem.md'

### Prerequisites
- <PrereqPythonInstall/>.
- <PrereqGxInstall/> and [Spark dependencies](/core/set_up_a_gx_environment/install_additional_dependencies.md?dependencies=spark).
- <PrereqDataContext/>.
- [A Filesystem Data Source configured to access data files in S3](/core/connect_to_data/filesystem_data/filesystem_data.md?data_source_type=spark&environment=s3#create-a-data-source).

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Define your Data Asset's parameters.

   To define a Directory Data Asset for data in an S3 bucket you provide the following elements:

   - `asset_name`: A name by which you can reference the Data Asset in the future.  This should be unique within the Data Source.
   - `s3_prefix`: The path to the data files for the Data Asset, relative to the root of the S3 bucket.

   This example uses taxi trip data stored in `.csv` files in the `data/taxi_yellow_tripdata_samples/` folder within the Data Sources S3 bucket:

   ```python title="Python"
    asset_name = "s3_taxi_csv_directory_asset"
    s3_prefix = "data/taxi_yellow_tripdata_samples/"
    ```

2. Add the Data Asset to your Data Source.

   A new Data Asset is created and added to a Data Source simultaneously.  The file format that the Data Asset can read is determined by the method used when the Data Asset is added to the Data Source.

   To see the file formats supported by a Spark File Data Source, reference the `.add_*_asset(...)` methods in [the API documentation for a `SparkFilesystemDatasource`](/reference/api/datasource/fluent/SparkFilesystemDatasource_class.mdx).

   The following example creates a Data Asset that can read `.csv` file data:

   ```python
   s3_file_data_asset = datasource.add_directory_asset(
      name=asset_name,
      s3_prefix=s3_prefix
   )
   ```
   
3. Optional. Retrieve the Data Asset from your Data Source.

   You can retrieve your Data Asset from the Data Context by updating `data_source_name` with the name of your Data Source and `asset_name` with the name of your Data Asset before executing the following:

   ```python
   data_source_name = "nyc_taxi_data"
   asset_name = "s3_taxi_csv_directory_asset"
   s3_file_data_asset = context.get_data_source(data_source_name).get_asset(asset_name)
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
   asset_name = "s3_taxi_csv_directory_asset"
   s3_prefix = "data/taxi_yellow_tripdata_samples/"

   # Add the Data Asset to the Data Source:
   s3_file_data_asset = datasource.add_directory_asset(
      name=asset_name,
      s3_prefix=s3_prefix
   )

   # Use the Data Context to retrieve the Data Asset when needed:
   data_source_name = "nyc_taxi_data"
   asset_name = "s3_taxi_csv_directory_asset"
   s3_file_data_asset = context.get_data_source(data_source_name).get_asset(asset_name)
   ```

</TabItem>

</Tabs>