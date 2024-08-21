---
title: Configure Data Docs
description: Configure the location where static Data Docs webpages are created.
hide_feedback_survey: false
hide_title: false
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../../_core_components/prerequisites/_gx_installation.md'
import PrereqFileDataContext from '../../_core_components/prerequisites/_file_data_context.md'

import PrereqAbsInstalled from '../../_core_components/prerequisites/_gx_installation_with_abs_dependencies.md'
import PrereqS3Installed from '../../_core_components/prerequisites/_gx_installation_with_s3_dependencies.md'
import PrereqGcsInstalled from '../../_core_components/prerequisites/_gx_installation_with_gcs_dependencies.md'

import EnvironmentAbs from './_backends/_abs.md'
import EnvironmentGcs from './_backends/_gcs.md'
import EnvironmentS3 from './_backends/_s3.md'
import EnvironmentFilesystem from './_backends/_local_or_networked.md'

Data Docs translate Expectations, Validation Results, and other metadata into human-readable documentation that is saved as static web pages. Automatically compiling your data documentation from your data tests in the form of Data Docs keeps your documentation current.  This guide covers how to configure additional locations where Data Docs should be created.

### Prerequisites:

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- <PrereqFileDataContext/>.  This guide assumes the variable `context` contains your Data Context.

To host Data Docs in an environment other than a local or networked filesystem, you will also need to install the appropriate dependencies and configure access credentials accordingly:

- Optional. <PrereqS3Installed/> and [credentials configured](/core/configure_project_settings/configure_credentials/configure_credentials.md).
- Optional. <PrereqGcsInstalled/> and [credentials configured](/core/configure_project_settings/configure_credentials/configure_credentials.md).
- Optional. <PrereqAbsInstalled/> and [credentials configured](/core/configure_project_settings/configure_credentials/configure_credentials.md).

### Procedure
<Tabs 
  queryString="procedure"
  defaultValue="instructions" 
  values={[
    {value: 'instructions', label:'Instructions'},
    {value:'sample_code', label:'Sample code'}
  ]}
>

<TabItem value="instructions" label="Instructions">

1. Define a configuration dictionary for your new Data Docs site.

   The main component that requires customization in a Data Docs site configuration is its `store_backend`.  The `store_backend` is a dictionary that tells GX where the Data Docs site will be hosted and how to access that location when the site is updated.

   The specifics of the `store_backend` will depend on the environment in which the Data Docs will be created.  GX 1.0 supports generation of Data Docs in local or networked filesystems, Amazon S3, Google Cloud Service, and Azure Blob Storage.

   To create a Data Docs site configuration, select one of the following environments and follow the corresponding instructions.

   <Tabs 
      queryString="environment"
      groupId="environment"
      defaultValue='filesystem'
      values={[
         {label: 'Filesystem', value:'filesystem'},
         {label: 'Amazon S3', value:'s3'},
         {label: 'Azure Blob Storage', value:'abs'},
         {label: 'Google Cloud Service', value:'gcs'},
      ]}
   >
   
   <TabItem value="filesystem">
   
   <EnvironmentFilesystem/>
   
   </TabItem>

   <TabItem value="s3">
   
   <EnvironmentS3/>
   
   </TabItem>

   <TabItem value="abs">
   
   <EnvironmentAbs/>
   
   </TabItem>

   <TabItem value="gcs">
   
   <EnvironmentGcs/>
   
   </TabItem>

   </Tabs>

4. Add your configuration to your Data Context.

   All Data Docs sites have a unique name within a Data Context. Once your Data Docs site configuration has been defined, add it to the Data Context by updating the value of `site_name` in the following to something more descriptive and then execute the code::

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - add data docs config to Data Context"
   ```

5. Optional. Build your Data Docs sites manually.

   You can manually build a Data Docs site by executing the following code:

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - manually build Data Docs"
   ```

6. Optional. Automate Data Docs site updates with Checkpoint Actions.

   You can automate the creation and update of Data Docs sites by including the `UpdateDataDocsAction` in your Checkpoints.  This Action will automatically trigger a Data Docs site build whenever the Checkpoint it is included in completes its `run()` method.

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - automate data docs with a Checkpoint Action"
   ```

7. Optional. View your Data Docs.

   Once your Data Docs have been created, you can view them with:

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - view data docs"
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

   GX 1.0 supports the Data Docs configurations for the following environments:

   <Tabs 
      queryString="environment"
      groupId="environment"
      defaultValue='filesystem'
      values={[
         {label: 'Filesystem', value:'filesystem'},
         {label: 'Amazon S3', value:'s3'},
         {label: 'Azure Blob Storage', value:'abs'},
         {label: 'Google Cloud Service', value:'gcs'},
      ]}
   >
   
   <TabItem value="filesystem">
   
   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - full code example"
   ```
   
   </TabItem>

   <TabItem value="s3">
   
   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_s3.py - full code example"
   ```
   
   </TabItem>

   <TabItem value="abs">
   
   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_abs.py - full code example"
   ```
   
   </TabItem>

   <TabItem value="gcs">
   
   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_gcs.py - full code example"
   ```
   
   </TabItem>

   </Tabs>

</TabItem>

</Tabs>

