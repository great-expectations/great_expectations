
1. In a Python environment [import GX and instantiate a Data Context](core/installation_and_setup/manage_data_contexts.md).  This example assumes the variable `context` points to your Data Context.

2. Define the parameters for a GCS Data Source that uses Spark to access data.

    - `name`: The Data Source name.  In the following example, this is `"my_gcs_datasource"`.
    - `bucket_or_name`: The GCS bucket or instnace name.
    - `gcs_options`: Optional.  Additional options for the Data Source.  The following example uses the default values.
    
    ```python title="Python"
    datasource_name = "my_gcs_datasource"
    bucket_name = "my_bucket"
    gcs_options = {}
    ```

   :::info Additional options for `gcs_options`
   
   The recommended method of generating credentials is to reference the `GOOGLE_APPLICATION_CREDENTIALS` environment variable that was set when you [installed support for GCS and GX](/core/installation_and_setup/additional_dependencies/google_cloud_storage.md).  However, the parameter `gcs_options` provides alternative methods to generate credentials for your GCS bucket.  You can also generate these credentials by providing one of the following key/value pairs in the `gcs_options` dictionary:

    - `filename`: The path to a GCS Account JSON file.
    - `info`: The GCS Account information in Google format.

    :::

4. Run the following Python code to pass `datasource_name`, `bucket_or_name`, and `gcs_options` as parameters and create your Amazon S3 Data Source:

  ```python title="Python"
datasource = context.sources.add_spark_gcs(
    name=datasource_name,
    bucket_or_name=bucket_or_name,
    gcs_options=gcs_options
) 
  ```