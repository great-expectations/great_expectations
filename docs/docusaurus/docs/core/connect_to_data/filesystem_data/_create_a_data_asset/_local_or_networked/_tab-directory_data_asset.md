import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstall from '../../../../_core_components/prerequisites/_python_installation.md'
import PrereqGxInstall from '../../../../_core_components/prerequisites/_gx_installation_with_spark_dependencies.md'
import PrereqDataContext from '../../../../_core_components/prerequisites/_preconfigured_data_context.md'
import PrereqSparkFilesystemDataSource from '../../../../_core_components/prerequisites/_data_source_spark_filesystem.md'

### Prerequisites
- <PrereqPythonInstall/>.
- <PrereqGxInstall/>.
- <PrereqDataContext/>.
- [A Spark Filesystem Data Source configured to access data files in a local or networked folder hierarchy](/core/connect_to_data/filesystem_data/filesystem_data.md?data_source_type=spark&environment=filesystem#create-a-data-source).

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Define your Data Asset's parameters.

   A Directory Data Asset for files in a local or networked folder hierarchy only needs two pieces of information to be created.

   - `name`: A descriptive name with which to reference the Data Asset.  This name should be unique among all Data Assets for the same Data Source.
   - `data_directory`: The path of the containing the data files for the Data Asset.  This path can be relative to the Data Source's `base_directory`.

   This example uses taxi trip data stored in `.csv` files in the `data/` folder within the Data Source's directory tree:

   ```python
   asset_name="taxi_csv_directory"
   data_directory="./data"
   ```

2. Add the Data Asset to your Data Source.

   A new Data Asset is created and added to a Data Source simultaneously.  The file format that the Data Asset can read is determined by the method used when the Data Asset is added to the Data Source.

   To see the file formats supported by a Spark Directory Data Source, reference the `.add_directory_*_asset(...)` methods in [the API documentation for a `SparkFilesystemDatasource`](/reference/api/datasource/fluent/SparkFilesystemDatasource_class.mdx).
   
   The following example creates a Data Asset that can read `.csv` file data:

   ```python
   directory_csv_asset = data_source.add_directory_csv_asset(
      name=asset_name,
      data_directory = data_directory
   )
   ```
   
4. Optional. Retrieve the Data Asset from your Data Source.

   You can retrieve your Data Asset from the Data Context by updating `data_source_name` with the name of your Data Source and `asset_name` with the name of your Data Asset before executing the following:

   ```python
   data_source_name = "my_filesystem_data_source"
   asset_name = "taxi_csv_directory"
   file_csv_asset = context.get_data_source(data_source_name).get_asset(asset_name)
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

   ```python
   import great_expectations as gx
   
   # This example uses a File Data Context which already has
   #  a Data Source defined.
   context = gx.get_context()

   data_source_name = "my_filesystem_data_source"
   data_source = context.get_datasource(data_source_name)

   # Define the Data Asset's parameters:
   asset_name = "taxi_csv_directory"
   data_directory = "./data"
   
   # Add the Data Asset to the Data Source:
   directory_csv_asset = data_source.add_directory_csv_asset(
      name=asset_name,
      data_directory = data_directory
   )

   # Use the Data Context to retrieve the Data Asset when needed:
   data_source_name = "my_filesystem_data_source"
   asset_name = "taxi_csv_directory"
   file_csv_asset = context.get_data_source(data_source_name).get_asset(asset_name)
   ```

</TabItem>

</Tabs>