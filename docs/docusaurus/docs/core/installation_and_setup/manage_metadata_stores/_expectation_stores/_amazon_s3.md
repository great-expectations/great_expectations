import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqGxInstallation from '../../../_core_components/prerequisites/_gx_installation.md'
import PrereqAmazonS3Dependencies from '../../../_core_components/prerequisites/_amazon_s3_dependencies.md'
import PrereqFileDataContext from '../../../_core_components/prerequisites/_file_data_context.md'

import AmazonS3Boto3Options from './_amazon_s3_boto3_options.md'

Use the information provided here to configure a new storage location for Expectations in Amazon S3.

### Prerequisites

- <PrereqGxInstallation/> with <PrereqAmazonS3Dependencies/>.
- <PrereqFileDataContext/>.
- An S3 bucket and prefix to store Expectations.

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Identify your File Data Context Expectations Store.

  Look for the `stores` section in your Data Context's `great_expectations.yml` file.  The default Expectation Store configuration should resemble:

  ```yaml title="YAML file content"
  stores:
  # highlight-start
    expectations_store:
      class_name: ExpectationsStore
      store_backend:
        class_name: TupleFilesystemStoreBackend
        base_directory: expectations/
  # highlight-end
  ```

2. Add an entry for the new Expectation Store to the `stores` section of `great_expectations.yml`.

  The simplest way to do this is to copy the existing default configuration and rename it.

  ```yaml title="YAML file content"
  stores:
    expectations_store:
      class_name: ExpectationsStore
      store_backend:
        class_name: TupleFilesystemStoreBackend
        base_directory: expectations/
  # highlight-start
    expectations_S3_store:  # This is the store's name.  It should be unique.
      class_name: ExpectationsStore
      store_backend:
        class_name: TupleFilesystemStoreBackend
        base_directory: expectations/
  # highlight-end
  ```

3. Update the new Store's configuration for Amazon S3.

  To make a Store that works with S3, change the default `class_name` and `store_backend` settings.
  - `class_name`: set this to `TupleS3StoreBackend`
  - `bucket`: set this to the address of your S3 bucket
  - `prefix`: set this to the folder in your S3 bucket where the Store's JSON files are located.

  :::caution 
  
  If you're using other Stores or DataDocs in S3, make sure that the `prefix` values are disjoint and none are substrings of another.
  
  :::

  The configuration you add to the `stores` section of your `great_expectations.yml` file should be similar to:

  ```yaml  title="YAML file content"
    stores:
      # highlight-start
      expectations_S3_store:
        class_name: ExpectationsStore
        store_backend:
          class_name: TupleS3StoreBackend
          bucket: '<your_s3_bucket_name>'
          prefix: '<your_s3_bucket_folder_name>'  # Bucket and prefix in combination must be unique across all Stores
      # highlight-end
  ```

4. Optional. Customize the `boto3` access options for the `TupleS3StoreBackend`.

  To implement a more fine-grained customization of the `TupleS3StoreBackend` choose among the following additional options:

  <AmazonS3Boto3Options/>

</TabItem>

<TabItem value="sample_code" label="Sample code">

The following example of an S3 Expectations Store configuration does not utilize the `boto3_options` section to specify the method of accessing the Expectations Store:

```yaml showLineNumbers title="YAML file content"
    stores:
      # highlight-start
      expectations_S3_store:
        class_name: ExpectationsStore
        store_backend:
          class_name: TupleS3StoreBackend
          bucket: '<your_s3_bucket_name>'
          prefix: '<your_s3_bucket_folder_name>'  # Bucket and prefix in combination must be unique across all Stores
      # highlight-end
```

The following examples of an S3 Expectations Store configuration utilize the `boto3_options` section to further customize the method of accessing the Expectations Store:

<Tabs queryString="TupleS3StoreBackendOptions" groupId="TupleS3StoreBackendOptions">
  
<TabItem value="endpoint_url" label="S3 Endpoint">

```yaml showLineNumbers title="YAML file content"
  stores:
    expectations_S3_store:
      class_name: ExpectationsStore
      store_backend:
        class_name: TupleS3StoreBackend
        bucket: '<your_s3_bucket_name>'
        prefix: '<your_s3_bucket_folder_name>'  # Bucket and prefix in combination must be unique across all stores
        # highlight-start
        boto3_options:
          endpoint_url: ${S3_ENDPOINT} # Uses the S3_ENDPOINT environment variable to determine which endpoint to use.
          region_name: '<your_aws_region_name>'
        # highlight-end
```

</TabItem>

<TabItem value="IamUser" label="IAM User">

```yaml showLineNumbers title="YAML file content"
  stores:
    expectations_S3_store:
        class_name: ExpectationsStore
        store_backend:
          class_name: TupleS3StoreBackend
          bucket: '<your_s3_bucket_name>'
          prefix: '<your_s3_bucket_folder_name>'  
          # highlight-start
          boto3_options:
            aws_access_key_id: ${AWS_ACCESS_KEY_ID} # Uses the AWS_ACCESS_KEY_ID environment variable to get aws_access_key_id.
            aws_secret_access_key: ${AWS_ACCESS_KEY_ID}
            aws_session_token: ${AWS_ACCESS_KEY_ID}
          # highlight-end
```

</TabItem>

<TabItem showLineNumbers value="IamAssumeRole" label="IAM assume role">

```yaml title="YAML file content"
  stores:
    expectations_S3_store:
        class_name: ExpectationsStore
        store_backend:
          class_name: TupleS3StoreBackend
          bucket: '<your_s3_bucket_name>'
          prefix: '<your_s3_bucket_folder_name>'  # Bucket and prefix in combination must be unique across all stores
          # highlight-start
          boto3_options:
            assume_role_arn: '<your_role_to_assume>'
            region_name: '<your_aws_region_name>'
            assume_role_duration: session_duration_in_seconds
          # highlight-end
```
  
</TabItem>

</Tabs>

</TabItem>

</Tabs>



