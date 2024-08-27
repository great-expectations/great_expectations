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

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_local_or_networked/_directory_asset.py - retrieve Data Source"
   ```

2. Define your Data Asset's parameters.

   A Directory Data Asset for files in a local or networked folder hierarchy only needs two pieces of information to be created.

   - `name`: A descriptive name with which to reference the Data Asset.  This name should be unique among all Data Assets for the same Data Source.
   - `data_directory`: The path of the containing the data files for the Data Asset.  This path can be relative to the Data Source's `base_directory`.

   This example uses taxi trip data stored in `.csv` files in the `data/` folder within the Data Source's directory tree:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_local_or_networked/_directory_asset.py - define Data Asset parameters"
   ```

3. Add the Data Asset to your Data Source.

   A new Data Asset is created and added to a Data Source simultaneously.  The file format that the Data Asset can read is determined by the method used when the Data Asset is added to the Data Source.

   To see the file formats supported by a Spark Directory Data Source, reference the `.add_directory_*_asset(...)` methods in [the API documentation for a `SparkFilesystemDatasource`](/reference/api/datasource/fluent/SparkFilesystemDatasource_class.mdx).
   
   The following example creates a Data Asset that can read `.csv` file data:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_local_or_networked/_directory_asset.py - add Data Asset"
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_local_or_networked/_directory_asset.py - full example"
   ```

</TabItem>

</Tabs>