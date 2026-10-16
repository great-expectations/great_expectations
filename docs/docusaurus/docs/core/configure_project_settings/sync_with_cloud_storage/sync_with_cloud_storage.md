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

Serverless functions and short-lived containers start from an image and can lose their local disk whenever they shut down. A `gx/` folder that one run creates might therefore not be there for the next. To validate data from workers like these, keep the project in a cloud storage bucket. Each worker downloads the project's configuration, runs a Checkpoint, and uploads the Validation Results from that run. A separate step builds Data Docs from every stored result. The examples cover Amazon S3, Azure Blob Storage, and Google Cloud Storage. On this page, "bucket" also means an Azure Blob Storage container.

A File Data Context keeps its configuration and its Validation Results in a local `gx/` folder. The code on this page copies files between that folder and the bucket with your storage service's Python client. It divides the work between three kinds of process: one configuration publisher, any number of workers, and one Data Docs publisher. Every location in the bucket has a single kind of writer, so workers that run at the same time don't overwrite each other's results or the configuration.

## Prerequisites

- <PrereqPythonInstalled/>.
- GX Core with the Python client for your storage service: [Amazon S3](/core/set_up_a_gx_environment/install_additional_dependencies.md?dependencies=amazon), [Azure Blob Storage](/core/set_up_a_gx_environment/install_additional_dependencies.md?dependencies=azure), or [Google Cloud Storage](/core/set_up_a_gx_environment/install_additional_dependencies.md?dependencies=gcs).
- <PrereqFileDataContext/>.
- <PrereqCheckpoint/> in that File Data Context.
- A bucket or container, and credentials for each process. [How the work is divided](#how-the-work-is-divided) lists the access each process needs.

## How the work is divided

Each location in the bucket has one kind of writer:

| Location in the bucket | Written by | Read by |
| --- | --- | --- |
| `gx/`, except `gx/uncommitted/` | The configuration publisher | Workers and the Data Docs publisher |
| `gx/uncommitted/validations/` | Workers, one new file per Validation Result | The Data Docs publisher |
| `data_docs/` | The Data Docs publisher | People who view Data Docs |

The examples use the prefixes `gx/` and `data_docs/`, and you can change either one. Keep the trailing `/` on each prefix, because the functions add file paths directly after it.

Give each process only the access its role needs, so that a worker can't change the configuration or the site, even by mistake:

- The configuration publisher writes below `gx/`.
- Workers list and read below `gx/`, and write below `gx/uncommitted/validations/`.
- The Data Docs publisher lists and reads below `gx/`, and writes below `data_docs/`.

In Google Cloud Storage, replacing an object also needs the `storage.objects.delete` permission. The configuration publisher and the Data Docs publisher replace objects every time they run after the first.

### Configuration has one writer

Any number of workers can download the configuration at the same time, but only one process should upload it. An upload replaces any object with the same name, and the last upload wins. Two processes that publish configuration can therefore leave the bucket with a mix of their files. Choose one configuration publisher, such as a CI job that runs when changes merge to your main branch, or a single person. Workers treat the configuration as read-only and never upload it.

Each file uploads separately, so a worker that downloads the configuration while a publish is in progress can get some old files and some new ones. Publish configuration changes while no workers are starting.

The configuration publisher skips `uncommitted/`, which holds `config_variables.yml`. Give workers their credentials through environment variables or a secrets manager instead. See [Configure credentials](/core/configure_project_settings/configure_credentials/configure_credentials.md) and [Access secrets managers](/core/configure_project_settings/access_secrets_managers/access_secrets_managers.md).

Workers use the Data Sources in the published configuration, so each worker must be able to reach the data they point to. A filesystem Data Source whose `base_directory` is a folder on the publisher's machine fails in a worker that doesn't have that folder.

### Each run adds its own Validation Results

A worker uploads only the files its own run added to the Validation Results folder. GX saves a Validation Result at `uncommitted/validations/<suite name>/<run name>/<run time>/<batch identifier>.json`. Each dot in the suite name starts a new folder, so results for a suite named `orders.daily` are saved below `orders/daily/`. Workers upload each result to the same path below the bucket's `gx/` prefix. The object's name therefore identifies the Expectation Suite, the run, and the Data Asset it validated.

The batch identifier joins the Data Source name, the Data Asset name, and the options of the Batch that GX read, such as `year` and `month`. It leaves out a file path and a dataframe. Batches read by path through one Data Asset, or from different dataframes, therefore all get the same batch identifier. A monthly Batch Definition run without Batch parameters still records the year and month of the Batch it picked.

The run time is the current UTC time, to the microsecond, when the run starts. Two runs with the same run name therefore still get separate results. If you don't pass a run name, the path has `__none__` in its place. `run_checkpoint()` requires one: pass a name that identifies what started the run, so that you can trace a stored result back to the job that produced it.

GX tries to read every run name as a date, and it uses the run name as a folder name. Choose run names that follow these rules:

- **Don't use a run name that GX can read as a date.** That includes a full date such as `2026-10-16`, a short number such as `12`, and a month or weekday name such as `march`. GX completes a partial date from the current date and uses the result as the run time, because `run_checkpoint()` passes no run time of its own. Runs with that name then share a run time and write to the same path, each one replacing the last. For a full date that is every run, and for a partial date at least every run on the same day.
- Don't use a run name that is only digits. A long number, such as a GitHub Actions run ID, makes GX raise `OverflowError` before the Checkpoint runs. Passing a run time to `RunIdentifier` doesn't avoid this: the Checkpoint then runs, but Data Docs leave its result out. Put a word in front of the ID instead, for example `gh-38891203107`.
- Don't put `/` or `\` in a run name. GX raises `ValueError` when it saves the result.
- Don't put `profiling` in a run name. Data Docs leave out every result whose run name contains it.

### Data Docs have one writer

Workers don't build or upload Data Docs. A worker's copy of the project holds only the Validation Results from its own run, so a site built there covers that one run. Instead, the Data Docs publisher downloads the configuration and every stored Validation Result, builds the site, and uploads it. Run the Data Docs publisher on a schedule, after a group of validation runs finishes, or by hand. Each run downloads every stored result, rebuilds every page, and uploads the whole site again. Its download, build time, and uploads all grow with the number of stored results.

Leave `UpdateDataDocsAction` out of the Checkpoints that workers run. In a worker, the Action builds pages in a temporary folder that the worker deletes without uploading.

## Avoid sync commands that delete

Command-line sync tools can delete objects in the destination that aren't in the source. `aws s3 sync --delete`, `gcloud storage rsync --delete-unmatched-destination-objects`, and `azcopy sync --delete-destination=true` all do this. Don't use them to copy files into the bucket. No process in this pattern has a copy that stays complete. The configuration publisher has no Validation Results, a worker has only the results from its own run, and the Data Docs publisher's copy lacks every result uploaded after its download. A deleting sync from any of them removes results that other runs uploaded.

The functions on this page only upload, so they never remove configuration that you delete from your project. When you delete an Expectation Suite, Validation Definition, or Checkpoint, delete its file from the bucket too. Dots in an Expectation Suite's name start new folders here as well, so a suite named `orders.daily` is saved as `expectations/orders/daily.json`.

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

   The configuration publisher, each worker, and the Data Docs publisher all need the code from this step.

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

   `publish_configuration()` uploads every file in the project's `gx/` folder except those in `uncommitted/`. GX adds `uncommitted/` to the `.gitignore` file it creates, so from a clean checkout the upload holds the same files you keep in version control. Call it only from the configuration publisher, with the path to the `gx/` folder:

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

   `run_checkpoint()` downloads the configuration, without `uncommitted/`, into a new temporary folder and loads a File Data Context from it. To find the configuration it lists every object below `project_prefix`, including the stored Validation Results it skips, so a worker's listing grows with the number of stored results. It runs the Checkpoint with the run name and the Batch parameters you pass, then uploads the files that appeared in the Validation Results folder during the run. GX writes a store identifier file into that folder when it loads the project. That is why the function compares the folder before and after the run instead of uploading all of it. The temporary folder is deleted when the function returns.

   Call it in each worker with the Checkpoint's name and a run name. Pass `batch_parameters` when the Checkpoint's Batch Definition needs them to choose a Batch, such as the `year` and `month` of a monthly Batch Definition:

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

   The function returns the Checkpoint's result, so the worker can act on it, for example by failing when `result.success` is `False`.

   If you moved the Validation Results Store's `base_directory` to another folder inside `uncommitted/`, change the `results_dir` assignment in `run_checkpoint()` to that folder. The functions on this page work only when that folder is inside `uncommitted/`. Anywhere else in `gx/`, `publish_configuration()` uploads Validation Results as configuration and every worker downloads them. Outside `gx/`, `upload_files()` raises `ValueError`. See [Configure project Stores](/core/configure_project_settings/configure_metadata_stores/configure_metadata_stores.md).

4. Publish Data Docs.

   `publish_data_docs()` downloads the configuration and every stored Validation Result into a new temporary folder. It builds Data Docs there and uploads the site below `data_docs_prefix`. Call it only from the Data Docs publisher:

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

   The function uploads the project's default Data Docs site, `local_site`, from `uncommitted/data_docs/local_site/`. If your site uses a different `base_directory`, change the `site_dir` assignment in `publish_data_docs()` to that folder. See [Configure Data Docs](/core/configure_project_settings/configure_data_docs/configure_data_docs.md).

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
