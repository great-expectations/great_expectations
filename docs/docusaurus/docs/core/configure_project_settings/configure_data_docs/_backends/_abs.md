An Azure Blob Storage Data Doc site requires the following `store_backend` information:

- `class_name`: The name of the class to implement for accessing the target environment.  For Azure Blob Storage this will be `TupleAzureBlobStoreBackend`.
- `container`: The name of the Azure Blob Storage container that will host the Data Docs site.
- `prefix`: The path of the folder that will contain the Data Docs pages relative to the root of the Azure Blob Storage container.  The combination of `container` and `prefix` must be unique accross all Stores used by a Data Context.
- `connection_string`: The connection string for your Azure Blob Storage.  For more information on how to securely store your connection string, see [Configure credentials](/core/configure_project_settings/configure_credentials/configure_credentials.md).

To add a local or networked filesystem backend for your Data Docs configuration, update the values of `container`, `prefix`, and `connection_string` in the following code and execute it:

```python title="Python"
container = "my_abs_container"
prefix = "data_docs/"
connection_string = "${AZURE_STORAGE_CONNECTION_STRING}"  # This uses string substitution to get the actual connection string from an environment variable or config file.

site_config["store_backend"] = {
  "class_name": "TupleAzureBlobStoreBackend",
  "container": container,
  "prefix": prefix,
  "connection_string": connection_string
}
```