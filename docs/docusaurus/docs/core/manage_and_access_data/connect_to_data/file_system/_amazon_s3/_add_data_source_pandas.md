
1. In a Python environment [import GX and instantiate a Data Context](core/installation_and_setup/manage_data_contexts.md).  This example assumes the variable `context` points to your Data Context.

2. Define the parameters for an Amazon S3 Data Source that uses pandas to access data.

    - `name`: The Data Source name.  In the following example, this is `"my_s3_datasource"`.
    - `bucket_name`: The Amazon S3 bucket name.
    - `boto3_options`: Optional.  Additional options for the Data Source.  The following example uses the default values.
    
    ```python title="Python"
    datasource_name = "my_s3_datasource"
    bucket_name = "my_bucket"
    boto3_options = {}
    ```

    ```python title="Python code" name="docs/docusaurus/docs/core/manage_and_access_data/connect_to_data/file_system/_amazon_s3/example_connect_using_pandas.py Data Source args"
    ```

    :::info Additional options for `boto3_options`

    The parameter `boto3_options` allows you to pass the following information:

    - `endpoint_url`: specifies an S3 endpoint.  You can use an environment variable such as `"${S3_ENDPOINT}"` to securely include this in your code.  The string `"${S3_ENDPOINT}"` will be replaced with the value of the corresponding environment variable.  See [manage credentials](/core/installation_and_setup/manage_credentials.md?credential-style=environment_variables) for more information on this feature.
    - `region_name`: Your AWS region name.

    :::

4. Run the following Python code to pass `datasource_name`, `bucket_name`, and `boto3_options` as parameters and create your Amazon S3 Data Source:

    ```python title="Python"
    datasource = context.sources.add_pandas_s3(
        name=datasource_name,
        bucket=bucket_name,
        boto3_options=boto3_options
    ) 
    ```

    ```python title="Python code" name="docs/docusaurus/docs/core/manage_and_access_data/connect_to_data/file_system/_amazon_s3/example_connect_using_pandas.py Create Data Source"
    ```