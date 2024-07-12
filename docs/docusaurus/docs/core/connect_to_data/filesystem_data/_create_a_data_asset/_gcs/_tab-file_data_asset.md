import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstall from '../../../../_core_components/prerequisites/_python_installation.md'
import PrereqGxInstall from '../../../../_core_components/prerequisites/_gx_installation_with_gcs_dependencies.md'
import PrereqDataContext from '../../../../_core_components/prerequisites/_preconfigured_data_context.md'

### Prerequisites
- <PrereqPythonInstall/>.
- <PrereqGxInstall/>.
- <PrereqDataContext/>.
- Access to data files in Google Cloud Storage.
- [A pandas](/core/connect_to_data/filesystem_data/filesystem_data.md?data_source_type=pandas&environment=gcs#create-a-data-source) or [Spark Filesystem Data Source configured for Google Cloud Storage data files](/core/connect_to_data/filesystem_data/filesystem_data.md?data_source_type=spark&environment=gcs#create-a-data-source).

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Define your Data Asset's parameters.

   To define a File Data Asset for Google Cloud Storage you provide the following elements:

   - `name`: A name by which you can reference the Data Asset in the future.  This should be unique among Data Assets on the same Data Source.
   - `gcs_prefix`: The beginning of the object key name.
   - `gcs_delimiter`: Optional. A character used to define the hierarchical structure of object keys within a bucket (default is "/").
   - `gcs_recursive_file_discovery`: Optional. A boolean indicating if files should be searched recursively from subfolders (default is False).
   - `gcs_max_results`: Optional. The maximum number of keys in a single response (default is 1000).

   This example uses taxi trip data stored in `.csv` files in the `data/taxi_yellow_tripdata_samples/` folder within the Google Cloud Storage Data Source:

   ```python title="Python"
   asset_name = "gcs_taxi_csv_file_asset"
   gcs_prefix = "data/taxi_yellow_tripdata_samples/"
   ```

2. Add the Data Asset to your Data Source.

   A new Data Asset is created and added to a Data Source simultaneously.  The file format that the Data Asset can read is determined by the method used when the Data Asset is added to the Data Source.

   - To see the file formats supported by a pandas File Data Source, reference the `.add_*_asset(...)` methods in [the API documentation for a `PandasFilesystemDatasource`](/reference/api/datasource/fluent/PandasFilesystemDatasource_class.mdx).
   - To see the file formats supported by a Spark File Data Source, reference the `.add_*_asset(...)` methods in [the API documentation for a `SparkFilesystemDatasource`](/reference/api/datasource/fluent/SparkFilesystemDatasource_class.mdx).

   The following example creates a Data Asset that can read `.csv` file data:

   ```python
   file_csv_asset = data_source.add_csv_asset(
      name=asset_name,
      gcs_prefix=gcs_prefix,
   )
   ```
   
3. Optional. Retrieve the Data Asset from your Data Source.

   You can retrieve your Data Asset from the Data Context by updating `data_source_name` with the name of your Data Source and `asset_name` with the name of your Data Asset before executing the following:

   ```python
   data_source_name = "nyc_taxi_data"
   asset_name = "gcs_taxi_csv_file_asset"
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
   asset_name = "gcs_taxi_csv_file_asset"
   gcs_prefix = "data/taxi_yellow_tripdata_samples/"

   # Add the Data Asset to the Data Source:
   file_csv_asset = data_source.add_csv_asset(name=asset_name)

   # Use the Data Context to retrieve the Data Asset when needed:
   data_source_name = "nyc_taxi_data"
   asset_name = "gcs_taxi_csv_file_asset"
   file_csv_asset = context.get_data_source(data_source_name).get_asset(asset_name)
   ```

</TabItem>

</Tabs>