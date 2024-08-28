import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstall from '../../../../_core_components/prerequisites/_python_installation.md'
import PrereqGxInstall from '../../../../_core_components/prerequisites/_gx_installation_with_gcs_dependencies.md'
import PrereqDataContext from '../../../../_core_components/prerequisites/_preconfigured_data_context.md'

### Prerequisites
- <PrereqPythonInstall/>
- <PrereqGxInstall/>
  - Optional. To create a Spark Filesystem Data Source you will also need to [install the Spark Python dependencies](/core/set_up_a_gx_environment/install_additional_dependencies.md?dependencies=spark).
- <PrereqDataContext/>
- Access to data files in Google Cloud Storage.

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

1. Set up the Data Source's credentials.

   By default, GCS credentials are handled through the gcloud command line tool and the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.  The gcloud command line tool is used to set up authentication credentials, and the `GOOGLE_APPLICATION_CREDENTIALS` environment variable provides the path to the `json` file with those credentials.

   For more information on using the gcloud command line tool, see Google Cloud's [Cloud Storage client libraries documentation](https://cloud.google.com/storage/docs/reference/libraries).

2. Define the Data Source's parameters.

   The following information is required when you create a GCS Data Source:

   - `name`: A descriptive name used to reference the Data Source.  This should be unique within the Data Context.
   - `bucket_or_name`: The GCS bucket or instance name.
   - `gcs_options`: Optional. A dictionary that can be used to specify an alternative method for providing GCS credentials.
   
   The `gcs_options` dictionary can be left empty if the default `GOOGLE_APPLICATION_CREDENTIALS` environment variable is populated.  Otherwise, the `gcs_options` dictionary should have either the key `filename` or the key `info`.

   - `filename`: The value of this key should be the specific path to your credentials json.  If you provide this then the `info` key should be left out of the dictionary.
   - `info`: The actual JSON data from your credentials file in the form of a string.  If you provide this then the `filename` key should not be included in the dictionary.

   Update the variables in the following code and execute it to define `name`, `bucket_or_name`, and `gcs_options`. In this example the default `GOOGLE_APPLICATION_CREDENTIALS` environment variable points to the location of the credentials json and therefore the `gcs_options` dictionary is left empty:  

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_gcs/_spark.py - define Data Source parameters"
   ```

3. Add a Google Cloud Storage Data Source to your Data Context.

   GX can leverage either pandas or Spark as the backend for your Google Cloud Storage Data Source.  To create your Data Source, execute one of the following sets of code:

   <Tabs queryString="data_source_type" groupId="data_source_type" defaultValue='pandas_filesystem'>

   <TabItem value="pandas_filesystem" label="pandas">

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_gcs/_pandas.py - add Data Source"
   ```

   </TabItem>

   <TabItem value="spark" label="Spark">

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_gcs/_spark.py - add Data Source"
   ```

   </TabItem>

   </Tabs>

</TabItem>

<TabItem value="sample_code" label="Sample code">

   Choose from the following to see the full example code for a S3 Filesystem Data Source, using either pandas or Spark to read the data files:

   <Tabs queryString="data_source_type" groupId="data_source_type" defaultValue='pandas_filesystem'>

   <TabItem value="pandas_filesystem" label="pandas example">

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_gcs/_pandas.py - add Data Source"
   
   ```

   </TabItem>

   <TabItem value="spark" label="Spark example">

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_gcs/_spark.py - add Data Source"
   ```

   </TabItem>

   </Tabs>

</TabItem>

</Tabs>