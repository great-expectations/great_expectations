---
title: Configure Expectations Stores
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqGxInstallation from '../../_core_components/prerequisites/_gx_installation.md'
import PrereqAmazonS3Dependencies from '../../_core_components/prerequisites/_amazon_s3_dependencies.md'
import PrereqFileDataContext from '../../_core_components/prerequisites/_file_data_context.md'
import PrereqPopulatedExpectationSuite from '../../_core_components/prerequisites/_expectation_suite_with_expectations.md'

import ExpectationStoreAmazonS3 from './_expectation_stores/_amazon_s3.md'
import ExpectationStoreAZB from './_expectation_stores/_azure_blob_storage.md'
import ExpectationStoreGCS from './_expectation_stores/_google_cloud_service.md'
import ExpectationStoreFilesystem from './_expectation_stores/_filesystem.md'

An Expectations Store stores and retrieves information about Expectation Suites, their Expectations, and their associated metadata.

By default, File Data Contexts store Expectation Suites in JSON format in the `expectations/` subdirectory of your `gx/` folder.  

## Add an Expectations Store configuration

Use the information provided here to configure an alternative storage location for your Expectation Suites.

<Tabs
  queryString="new-store-location"
  groupId="configure-expectation-stores"
  defaultValue='amazon'
  values={[
  {label: 'Amazon S3', value:'amazon'},
  {label: 'Microsoft Azure Blob Storage', value:'azure'},
  {label: 'Google Cloud Service', value:'gcs'},
  {label: 'Filesystem', value:'filesystem'},
  ]}>

<TabItem value="amazon">

<ExpectationStoreAmazonS3/>

</TabItem>
<TabItem value="azure">

<ExpectationStoreAZB/>

</TabItem>
<TabItem value="gcs">

<ExpectationStoreGCS/>

</TabItem>
<TabItem value="filesystem">

<ExpectationStoreFilesystem/>

</TabItem>

</Tabs>

## Change the Expectations Store configuration used by a Data Context

1. Look for the `expectations_store_name` section of your `great_expectations.yml`.  

  The `expectations_store_name` value tells the Data Context which configuration to use for the Expectations Store.  The default entry is:

  ```yaml title="YAML file content"
    expectations_store_name: expectations_store
  ```
  
2. Update the `expectations_store_name` value.

  Update the `expectations_store_name` value to match the configuration that should be used.  For instance, if the new Expectations Store is named `expectations_S3_store` the `expectations_store_name` entry should be:

  ```yaml title="YAML file content"
     expectations_store_name: expectations_S3_store
  ```

## Copy existing Expectations to a new Store location

If you are converting an existing default Expectations Store to one in a new location, you might have Expectations saved that you want to transfer.  This can be done by copying the content of the default Expectations Store `base_directory` to a new Expectations Store in one of the following environments:

<Tabs
  queryString="copy-store-location"
  groupId="transfer-expectation-store-data"
  defaultValue='amazon'
  values={[
  {label: 'Amazon S3', value:'amazon'},
  {label: 'Microsoft Azure Blob Storage', value:'azure'},
  {label: 'Google Cloud Service', value:'gcs'},
  {label: 'Filesystem', value:'filesystem'},
  ]}>

<TabItem value="amazon">

Use the information provided here to copy Expectation Suites from a default Expectations Store to an Expectations Store configured for Amazon S3.

### Prerequisites

- <PrereqGxInstallation/> with <PrereqAmazonS3Dependencies/>.
- <PrereqFileDataContext/>.
- <PrereqPopulatedExpectationSuite/> to transfer to the new Store location.
- [An S3 Expectations Store configuration](?copy-store-location=amazon#add-an-expectations-store-configuration).

### Procedure

1. Run the following `aws s3 sync` command to copy Expectations into Amazon S3:

  ```bash title="Terminal input"
  aws s3 sync '<base_directory>' s3://'<your_s3_bucket_name>'/'<your_s3_bucket_folder_name>'
  ```

  The base directory is set to `expectations/` by default.

  After executing the `aws s3 sync` command you should see a confirmation message.  In the following example, the confirmation message indicates that Expectations `exp1` and `exp2` are copied to Amazon S3:

  ```bash title="Terminal output"
  upload: ./exp1.json to s3://'<your_s3_bucket_name>'/'<your_s3_bucket_folder_name>'/exp1.json
  upload: ./exp2.json to s3://'<your_s3_bucket_name>'/'<your_s3_bucket_folder_name>'/exp2.json
  ```

2. Optional. Test access to the copied Expectation Suites.

  If you copied your existing Expectation Suites to the S3 bucket, run the following Python code to confirm that GX 1.0 can find them:

  ```python title="Python"
  import great_expectations as gx

  context = gx.get_context()
  context.list_expectation_suite_names()
  ```

  The Expectations you copied to S3 are returned as a list. Expectations that weren't copied to the new Store aren't listed.

</TabItem>
<TabItem value="azure">

<ExpectationStoreAZB/>

</TabItem>
<TabItem value="gcs">

<ExpectationStoreGCS/>

</TabItem>
<TabItem value="filesystem">

<ExpectationStoreFilesystem/>

</TabItem>

</Tabs>
