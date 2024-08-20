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

Data Docs translate Expectations, Validation Results, and other metadata into human-readable documentation that is saved as static web pages. Automatically compiling your data documentation from your data tests in the form of Data Docs keeps your documentation current.  This guide covers how to configure additional locations where Data Docs should be created.

### Prerequisites:

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- <PrereqFileDataContext/>.  This guide assumes the variable `context` contains your Data Context.

To host Data Docs in an environment other than a local or networked filesystem, you will also need to install the appropriate dependencies and configure access credentials accordingly:

- Optional. <PrereqS3Installed> and [credentials configured](core/configure_project_settings/configure_credentials.configure_credentials.md).
- Optional. <PrereqGcsInstalled/> and [credentials configured](core/configure_project_settings/configure_credentials.configure_credentials.md).
- Optional. <PrereqAbsInstalled/> and [credentials configured](core/configure_project_settings/configure_credentials.configure_credentials.md).

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

   All Data Docs sites have a unique name within a Data Context.  Start building a Data Docs configuration dictionary by updating the value of `site_name` in the following to something more descriptive and then execute the code:

   ```python title="Python"
   site_name="my_data_docs_site"

   site_config={
       site_name=site_name,
       "class_name": "SiteBuilder",
       "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
   }
   ```

2. Add a `store_backend` to your Data Docs configuration.

   The `store_backend` tells GX 1.0 where to generate Data Docs pages and how to connect to that location.  The specifics of the `store_backend` will depend on the environment in which the Data Docs will be created.  GX 1.0 supports generation of Data Docs in local or networked filesystems, Amazon S3, Google Cloud Service, and Azure Blob Storage.



3. Add your configuration to your Data Context.

   Once your Data Docs site configuration has been defined, add it to the Data Context by executing the following code:

   ```python title="Python"
   context.add_data_docs_site(site_config)
   ```

4. Optional. Build your Data Docs sites manually.

   You can manually build a Data Docs site by executing the following code:

   ```python title="Python"
   context.build_data_docs(site_names=site_name)
   ```

5. Optional. Automate Data Docs site updates with Checkpoint Actions.

   You can automate the creation and update of Data Docs sites by including the `UpdateDataDocsAction` in your Checkpoints.  This Action will automatically trigger a Data Docs site build whenever the Checkpoint it is included in completes its `run()` method.

   ```python title="Python"
   checkpoint = gx.Checkpoint(
        name=checkpoint_name,
        validation_definitions=[validation_definition],
        actions=[gx.checkpoint.actions.UpdateDataDocsAction(name="foo", site_names=[site_name])],
    )

   result = checkpoint.run(batch_parameters={"year": 2024})
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">



</TabItem>

</Tabs>

```python title="Python"
site_name = "site_name"
context.add_data_docs_site(
site_config={
    "class_name": "SiteBuilder",
    "store_backend": {
        "class_name": "TupleAzureBlobStoreBackend",
        "container": "another",
        "connection_string": connection_string,
    },
    "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
},
site_name=site_name,
)
...
checkpoint = gx.Checkpoint(
        name=checkpoint_name,
        validation_definitions=[validation_definition],
        actions=[gx.checkpoint.actions.UpdateDataDocsAction(name="foo", site_names=[site_name])],
    )
...
# we blow up here!
result = checkpoint.run(batch_parameters={"year": 2024})

context.build_data_docs(site_names=site_name)
```