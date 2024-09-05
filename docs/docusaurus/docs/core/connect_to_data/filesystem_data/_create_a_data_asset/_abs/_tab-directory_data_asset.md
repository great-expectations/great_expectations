import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstall from '../../../../_core_components/prerequisites/_python_installation.md'
import PrereqGxInstall from '../../../../_core_components/prerequisites/_gx_installation_with_abs_dependencies.md'
import PrereqDataContext from '../../../../_core_components/prerequisites/_preconfigured_data_context.md'
import PrereqSparkFilesystemDataSource from '../../../../_core_components/prerequisites/_data_source_spark_filesystem.md'

### Prerequisites
- <PrereqPythonInstall/>.
- <PrereqGxInstall/> and [Spark dependencies](/core/set_up_a_gx_environment/install_additional_dependencies.md?dependencies=spark).
- <PrereqDataContext/>.
- [A Filesystem Data Source configured to access data files in Azure Blob Storage](/core/connect_to_data/filesystem_data/filesystem_data.md?data_source_type=spark&environment=abs#create-a-data-source).

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

1. Retrieve your Data Source.
   Replace the value of `data_source_name` in the following code with the name of your Data Source and execute it to retrieve your Data Source from the Data Context:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_abs/_directory_asset.py - retrieve Data Source"
   ```

1. Define your Data Asset's parameters.

   To define a Directory Data Asset for Azure Blob Storage you provide the following elements:

   - `name`: A name by which you can reference the Data Asset in the future.
   - `abs_container`: The name of your Azure Blob Storage container.
   - `abs_name_starts_with`: The path to the data files for the Data Asset in the Azure Blob Storage container.  This should be relative to the root of the `abs_container`.
   - `data_directory`: The path of the folder containing data files for the Data asset, relative to the root of the `abs_container`.
   - `abs_recursive_file_discovery`: (Optional) A boolean (True/False) indicating if files should be searched recursively from subfolders.  The default is False.

   This example uses taxi trip data stored in `.csv` files in the `data/taxi_yellow_tripdata_samples/` folder within the Azure Blob Storage container:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_abs/_directory_asset.py - define Data Asset parameters"
    ```

3. Add the Data Asset to your Data Source.

   A new Data Asset is created and added to a Data Source simultaneously.  The file format that the Data Asset can read is determined by the method used when the Data Asset is added to the Data Source.

   To see the file formats supported by a Spark Directory Data Source, reference the `.add_directory_*_asset(...)` methods in [the API documentation for a `SparkFilesystemDatasource`](/reference/api/datasource/fluent/SparkFilesystemDatasource_class.mdx).
   
   The following example creates a Data Asset that can read `.csv` file data:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_abs/_directory_asset.py - add Data Asset"

</TabItem>

<TabItem value="sample_code" label="Sample code">

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_abs/_directory_asset.py - full example"
   ```

</TabItem>

</Tabs>