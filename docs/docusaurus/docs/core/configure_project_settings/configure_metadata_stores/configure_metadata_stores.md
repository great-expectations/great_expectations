---
title: Configure project Stores
description: Configure the location of stored metadata and Validation Results for a File Data Context.
hide_feedback_survey: false
hide_title: false
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../../_core_components/prerequisites/_gx_installation.md'
import PrereqFileDataContext from '../../_core_components/prerequisites/_file_data_context.md'


Stores are used by GX to store and retreive information ranging from project metadata such as Expectation Suite configurations to the Validation Results generated when Checkpoints are run.

Ephemeral Data Contexts store this information in memory, while GX Cloud Data Contexts store this information online.  File Data Contexts, however, store this information in files thata can be copied or shared between other File Data Contexts.

By default, Store files are created in folders within the `base_folder` of the File Data Context.  However, you can update your Data Context to specify where these Stores should reside, or to indicate existing Stores to load when the Data Context is initialized.

### Prerequisites:

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- <PrereqFileDataContext/>.

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

1. Load a File Data Context.

   Because Store configurations are loaded when a Data Context is initialized, an Ephemeral Data Context will always initialize with default in-memory Stores and any changes to them will not persist when a new Ephemeral Data Context is initialized. 

   GX Cloud accounts manage Stores for you online, and do not support custom Store configurations.

   Therefore, only File Data Contexts can have customized Store configurations.  This procedure assumes you have a File Data Context loaded as the variable `context`:

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/_examples/configure_metadata_stores.py - retrieve a File Data Context"
   ```

2. Determine the Store to update.

   GX utilizes 5 Stores for different types of data and metadata.  These Stores are the Expectations Store, Validation Definitions Store, Checkpoint Store, Validation Results Store, and the Suite Parameter Store.  All Stores can be accessed by passing a corresponding key to a Data Context's `variables.config.stores` attribute.  

   The `variables.config.stores` attribute gives access to the configuration values prior to any string substitution that may take palce, which also allows you to include string substitution references in your configuration.  To view the resolved path after string substitution takes place, use `variables.stores`.  For more information on how to configure string substitution references and values see [Configure credentials](/core/configure_project_settings/configure_credentials/configure_credentials.md).

   The following code shows how to print the configuration for each of these stores:

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/_examples/configure_metadata_stores.py - access Metadata Store configurations"
   ```
   
   When updating a Store configuration you will use the same key to access it from your Data Context's `variables.config.stores` attribute as was used to print it in the above example.
   
3. Update the `base_path` of the Store to change.

   Each Store has a `store_backend` configuration that determines how and where the Store accesses and saves information.  To change the location of a Store, you will update the `store_backend` configuration's `base_directory` value.  For instance, to change the location of an Expectation Store, you would update the `expectation_store_directory` variable in the following code and execute it:

   ```pyhton title="Python" name="docs/docusaurus/docs/core/configure_project_settings/_examples/configure_metadata_stores.py - example update Expectations Store base directory"
   ```

   The path provided for the `base_directory` should be either be an absolute path, or a path relative to the File Data Context's `project_root_dir`.

4. Save the File Data Context variables.

   Once a Data Context's `variables` have been updated, the changes need to be saved to the Data Context's configuration file so that they will persist when the Data Context is loaded in the future.  This is done with:

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/_examples/configure_metadata_stores.py - save changes to the Data Context"
   ```

5. Re-initialize the File Data Context.

   Because Store configurations are loaded when the Data Context is initialized, you will need to re-initialize your Data Context before your changes will take effect.  This is done by loading the Data Context again, exactly as when it was loaded the first time:

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/_examples/configure_metadata_stores.py - re-initialize the Data Context"
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/_examples/configure_metadata_stores.py - full code example"
```

</TabItem>

</Tabs>

