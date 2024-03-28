1. In a Python environment [import GX and instantiate a Data Context](core/installation_and_setup/manage_data_contexts.md).  This example assumes the variable `context` points to your Data Context.

2. Define the parameters for your Microsoft Azure Blob Storage Data Source.

    - `name`: The Data Source name.  In the following example, this is `"my_datasource"`.
    - `azure_options`: The authentication information for your Microsoft Azure Blob Storage account.  This will contain one of (but not both) two key/value pairs:
      - `account_url`: The URL for your Azure Storage account.
      - `conn_str`: Your Azure Storage account's connection string.
    
    ```python title="Python"
    datasource_name = "my_datasource"
    azure_options = {
        "conn_str": "${AZURE_STORAGE_CONNECTION_STRING}",
    }
    ```
   
    :::info Secure `account_url` and `conn_str` values

    In the previous example, the value for `conn_str` is substituted for the contents of the `AZURE_STORAGE_CONNECTION_STRING` environment variable you configured when you installed GX and set up your Azure Blob Storage dependencies and credentials.  For more information on storing credentials in a secure fashion with GX, see [Manage credentials](/core/installation_and_setup/manage_credentials.md).

    :::

3. Run the following Python code to pass `name` and `azure_options` as parameters and create your Azure Blob Storage Data Source:

  ```python title="Python"
datasource = context.sources.add_spark_abs(
    name=datasource_name,
    azure_options=azure_options,
) 
  ```