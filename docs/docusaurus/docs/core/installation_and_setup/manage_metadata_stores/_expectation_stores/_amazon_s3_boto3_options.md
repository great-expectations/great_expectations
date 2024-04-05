import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

<Tabs queryString="TupleS3StoreBackendOptions" groupId="TupleS3StoreBackendOptions">
  
<TabItem value="endpoint_url" label="S3 Endpoint">

To specify an S3 endpoint, you need to provide the following `boto3_options`:

- `endpoint_url`: The S3 endpoint that should be used.  In the following example, this is provided securely by pulling the value from a referenced environment variable.  For more information securely storing access credentials and information outside of version control, see [Manage credentials](/core/installation_and_setup/manage_credentials.md).
- `region_name`: The name that corresponds to your AWS data center's region.

Your `boto3_options` should resemble:

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
          endpoint_url: ${S3_ENDPOINT} # Uses the S3_ENDPOINT environment variable to determine which endpoint to use.
          region_name: '<your_aws_region_name>'
        # highlight-end
```

</TabItem>

<TabItem value="IamUser" label="IAM User">

To specify an IAM User, you need to provide the following `boto3_options`:

- `aws_access_key_id`: Unique identifier for an IAM user.
- `aws_secret_access_key`: Private key for the IAM user.
- `aws_session_token`: Token used to authenticate temporary security credentials.

:::caution 
  
These values should be provided securely and kept outside of version control.  In the following example, this will be done by referencing environment variables to retrieve the actual values for these keys.  For more information securely storing access credentials and information outside of version control, see [Manage credentials](/core/installation_and_setup/manage_credentials.md).
  
:::

Your `boto3_options` for an IAM user should resemble:

```yaml title="YAML file content"
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

<TabItem value="IamAssumeRole" label="IAM assume role">

To specify an IAM Assume Role, you need to provide the following `boto3_options`:

- `assume_role_arn`: The Amazon Resource Name (ARN) of the role.
- `region_name`: The name that corresponds to your AWS data center's region.
- `assume_role_duration`: The permitted duration, in seconds, of the role session.

Your `boto3_options` for an IAM Assume Role should resemble:

```yaml title="YAML file content"
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