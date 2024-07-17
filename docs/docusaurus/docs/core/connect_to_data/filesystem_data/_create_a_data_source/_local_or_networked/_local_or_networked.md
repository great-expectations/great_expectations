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

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_local_or_networked/_spark.py - define Data Source parameters"
   ```

2. Add a Filesystem Data Source to your Data Context.

   GX can leverage either pandas or Spark as the backend for your Filesystem Data Source.  To create your Data Source, execute one of the following sets of code:
 
   <Tabs queryString="data_source_type" groupId="data_source_type" defaultValue='pandas_filesystem'>

   <TabItem value="pandas_filesystem" label="pandas">

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_local_or_networked/_pandas.py - add Data Source"
   ```

   </TabItem>

   <TabItem value="spark" label="Spark">

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_local_or_networked/_spark.py - add Data Source"
   ```

   </TabItem>

   </Tabs>

</TabItem>

<TabItem value="sample_code" label="Sample code">

   Choose from the following to see the full example code for a local or networked Data Source, using either pandas or Spark to read the data files:

   <Tabs queryString="data_source_type" groupId="data_source_type" defaultValue='pandas_filesystem'>

   <TabItem value="pandas_filesystem" label="pandas example">

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_local_or_networked/_pandas.py - full example"
   ```

   </TabItem>

   <TabItem value="spark" label="Spark example">

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_local_or_networked/_spark.py - full example"
   ```

   </TabItem>

   </Tabs>

</TabItem>

</Tabs>

