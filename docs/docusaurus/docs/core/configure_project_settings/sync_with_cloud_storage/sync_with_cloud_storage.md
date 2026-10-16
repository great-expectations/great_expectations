---
title: Sync a project with cloud storage
description: Keep a File Data Context in Amazon S3, Azure Blob Storage, or Google Cloud Storage, and run validations from workers that start without a copy of it.
hide_feedback_survey: false
hide_title: false
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import PrereqFileDataContext from '../../_core_components/prerequisites/_file_data_context.md'
import PrereqCheckpoint from '../../_core_components/prerequisites/_checkpoint.md'

import ConnectS3 from './_backends/_s3.md';
import ConnectAbs from './_backends/_abs.md';
import ConnectGcs from './_backends/_gcs.md';

Serverless functions and short-lived containers start from an image and can lose their local disk whenever they shut down, so a `gx/` folder that one run creates might not be there for the next. To validate data from workers like these, keep the project in a cloud storage bucket. Each worker downloads the project's configuration, runs a Checkpoint, and uploads the Validation Results from that run. A separate step builds Data Docs from every stored result.

A File Data Context keeps its configuration and its Validation Results in a local `gx/` folder. The code on this page copies files between that folder and the bucket with your storage service's Python client. It gives every location in the bucket a single kind of writer, so workers that run at the same time don't overwrite each other's results or the configuration.

## How the work is divided

Three kinds of process use the bucket:

| Location in the bucket | Written by | Read by |
| --- | --- | --- |
| `gx/`, except `gx/uncommitted/` | The configuration publisher | Workers and the Data Docs publisher |
| `gx/uncommitted/validations/` | Workers, one new file per Validation Result | The Data Docs publisher |
| `data_docs/` | The Data Docs publisher | People who view Data Docs |

The examples use the prefixes `gx/` and `data_docs/`, and you can change either one. On this page, "bucket" also means an Azure Blob Storage container.

### Configuration has one writer

Any number of workers can download the configuration at the same time, but only one process should upload it. An upload replaces any object with the same name, and the last upload wins, so two processes that publish configuration can leave the bucket with a mix of their files. Publish configuration from one place, such as a CI job that runs when changes merge to your main branch, or a single person. Workers treat the configuration as read-only and never upload it.

Each file uploads separately, so a worker that downloads the configuration while a publish is in progress can get some old files and some new ones. Publish configuration changes while no workers are starting.

The configuration publisher skips `uncommitted/`, which holds `config_variables.yml`. Give workers their credentials through environment variables or a secrets manager instead. See [Configure credentials](/core/configure_project_settings/configure_credentials/configure_credentials.md) and [Access secrets managers](/core/configure_project_settings/access_secrets_managers/access_secrets_managers.md).

### Each run adds its own Validation Results

GX saves a Validation Result at `uncommitted/validations/<suite name>/<run name>/<run time>/<batch identifier>.json`. Workers upload each result to the same path below the bucket's `gx/` prefix, so the object's name identifies the Expectation Suite, the run, and the Batch it validated. The batch identifier combines the Data Source name, the Data Asset name, and any batch parameters other than a dataframe, such as `year` and `month`. The run time is the current UTC time, to the microsecond, when the run starts.

If you don't pass a run name, the path has `__none__` in its place. Pass a run name that identifies what started the run, such as your scheduler's run ID, so that you can trace a stored result back to the job that produced it. Two runs with the same run name still get separate results, because their run times differ.

There is one exception. If GX can read the run name as a date, for example `2026-10-16`, and you don't pass a run time, GX uses that date as the run time. Every run with that name then writes to the same path, and each one replaces the last.

A worker uploads only the files its own run added to the Validation Results folder. It never uploads configuration.

### Data Docs have one writer

Workers don't build or upload Data Docs. A worker's copy of the project holds only the Validation Results from its own run, so a site built there covers that one run. Instead, one process downloads the configuration and every stored Validation Result, builds the site, and uploads it. Run that process on a schedule, after a batch of validation runs finishes, or by hand. Its download grows with the number of stored results.

Leave `UpdateDataDocsAction` out of the Checkpoints that workers run. In a worker, the Action builds pages in a temporary folder that the worker deletes without uploading.

## Avoid sync commands that delete

Command-line sync tools can delete objects in the destination that aren't in the source. `aws s3 sync --delete`, `gcloud storage rsync --delete-unmatched-destination-objects`, and `azcopy sync --delete-destination=true` all do this. Don't use them to copy files into the bucket. No process in this pattern has a full copy of the bucket: the configuration publisher has no Validation Results, and a worker has only the results from its own run. A deleting sync from either one removes the results that other runs uploaded.

The functions on this page only upload, so they never remove configuration that you delete from your project. When you delete an Expectation Suite, Validation Definition, or Checkpoint, delete its file from the bucket too.

## Prerequisites

- <PrereqPythonInstalled/>.
- GX Core with the Python client for your storage service: [Amazon S3](/core/set_up_a_gx_environment/install_additional_dependencies.md?dependencies=amazon), [Azure Blob Storage](/core/set_up_a_gx_environment/install_additional_dependencies.md?dependencies=azure), or [Google Cloud Storage](/core/set_up_a_gx_environment/install_additional_dependencies.md?dependencies=gcs).
- <PrereqFileDataContext/>.
- <PrereqCheckpoint/> in that File Data Context.
- A bucket or container, and credentials that can list, read, and write objects in it.

## Procedure

<Tabs
   queryString="procedure"
   defaultValue="instructions"
   values={[
      {value: 'instructions', label: 'Instructions'},
      {value: 'sample_code', label: 'Sample code'}
   ]}
>

<TabItem value="instructions" label="Instructions">

1. Connect to your storage.

   <Tabs queryString="environment" groupId="environment" defaultValue="s3" values={[{label: 'Amazon S3', value: 's3'}, {label: 'Azure Blob Storage', value: 'abs'}, {label: 'Google Cloud Storage', value: 'gcs'}]}>

   <TabItem value="s3">
   <ConnectS3/>
   </TabItem>

   <TabItem value="abs">
   <ConnectAbs/>
   </TabItem>

   <TabItem value="gcs">
   <ConnectGcs/>
   </TabItem>

   </Tabs>

2. Publish the configuration.

   `publish_configuration()` uploads every file in the project's `gx/` folder except those in `uncommitted/`. GX adds `uncommitted/` to the `.gitignore` file it creates, so the upload holds the same files you keep in version control. Call it from the one place that publishes configuration, with the path to the `gx/` folder:

   <Tabs queryString="environment" groupId="environment" defaultValue="s3" values={[{label: 'Amazon S3', value: 's3'}, {label: 'Azure Blob Storage', value: 'abs'}, {label: 'Google Cloud Storage', value: 'gcs'}]}>

   <TabItem value="s3">

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_s3.py - define publish_configuration"
   ```

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_s3.py - call publish_configuration"
   ```

   </TabItem>

   <TabItem value="abs">

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_abs.py - define publish_configuration"
   ```

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_abs.py - call publish_configuration"
   ```

   </TabItem>

   <TabItem value="gcs">

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - define publish_configuration"
   ```

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - call publish_configuration"
   ```

   </TabItem>

   </Tabs>

3. Run Checkpoints in workers.

   `run_checkpoint()` downloads the configuration, without `uncommitted/`, into a new temporary folder and loads a File Data Context from it. It runs the Checkpoint with the run name you pass, then uploads the files that appeared in the Validation Results folder during the run. GX writes a store identifier file into that folder when it loads the project, which is why the function compares the folder before and after the run instead of uploading all of it. The temporary folder is deleted when the function returns. Call it in each worker with the Checkpoint's name and a run name:

   <Tabs queryString="environment" groupId="environment" defaultValue="s3" values={[{label: 'Amazon S3', value: 's3'}, {label: 'Azure Blob Storage', value: 'abs'}, {label: 'Google Cloud Storage', value: 'gcs'}]}>

   <TabItem value="s3">

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_s3.py - define run_checkpoint"
   ```

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_s3.py - call run_checkpoint"
   ```

   </TabItem>

   <TabItem value="abs">

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_abs.py - define run_checkpoint"
   ```

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_abs.py - call run_checkpoint"
   ```

   </TabItem>

   <TabItem value="gcs">

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - define run_checkpoint"
   ```

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - call run_checkpoint"
   ```

   </TabItem>

   </Tabs>

   The function returns the Checkpoint's result, so the worker can act on it, for example by failing when `result.success` is `False`. If you changed the `base_directory` of the Validation Results Store, set `results_dir` to that folder. See [Configure project Stores](/core/configure_project_settings/configure_metadata_stores/configure_metadata_stores.md).

4. Publish Data Docs.

   `publish_data_docs()` downloads the configuration and every stored Validation Result into a new temporary folder. It builds Data Docs there and uploads the site below `data_docs_prefix`. Call it from the one place that publishes Data Docs:

   <Tabs queryString="environment" groupId="environment" defaultValue="s3" values={[{label: 'Amazon S3', value: 's3'}, {label: 'Azure Blob Storage', value: 'abs'}, {label: 'Google Cloud Storage', value: 'gcs'}]}>

   <TabItem value="s3">

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_s3.py - define publish_data_docs"
   ```

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_s3.py - call publish_data_docs"
   ```

   </TabItem>

   <TabItem value="abs">

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_abs.py - define publish_data_docs"
   ```

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_abs.py - call publish_data_docs"
   ```

   </TabItem>

   <TabItem value="gcs">

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - define publish_data_docs"
   ```

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - call publish_data_docs"
   ```

   </TabItem>

   </Tabs>

   The function uploads the project's default Data Docs site, `local_site`, from `uncommitted/data_docs/local_site/`. If your site uses a different `base_directory`, set `site_dir` to that folder. See [Configure Data Docs](/core/configure_project_settings/configure_data_docs/configure_data_docs.md).

</TabItem>

<TabItem value="sample_code" label="Sample code">

   The sample defines the functions from the procedure. Call `publish_configuration()` from your configuration publisher, `run_checkpoint()` from each worker, and `publish_data_docs()` from your Data Docs publisher.

   <Tabs queryString="environment" groupId="environment" defaultValue="s3" values={[{label: 'Amazon S3', value: 's3'}, {label: 'Azure Blob Storage', value: 'abs'}, {label: 'Google Cloud Storage', value: 'gcs'}]}>

   <TabItem value="s3">

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_s3.py - full code example"
   ```

   </TabItem>

   <TabItem value="abs">

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_abs.py - full code example"
   ```

   </TabItem>

   <TabItem value="gcs">

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_gcs.py - full code example"
   ```

   </TabItem>

   </Tabs>

</TabItem>

</Tabs>
