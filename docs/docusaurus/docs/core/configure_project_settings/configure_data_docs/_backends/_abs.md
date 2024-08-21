An Azure Blob Storage Data Doc site requires the following `store_backend` information:

- `class_name`: The name of the class to implement for accessing the target environment.  For Azure Blob Storage this will be `TupleAzureBlobStoreBackend`.
- `container`: The name of the Azure Blob Storage container that will host the Data Docs site.
- `prefix`: The path of the folder that will contain the Data Docs pages relative to the root of the Azure Blob Storage container.  The combination of `container` and `prefix` must be unique accross all Stores used by a Data Context.
- `connection_string`: The connection string for your Azure Blob Storage.  For more information on how to securely store your connection string, see [Configure credentials](/core/configure_project_settings/configure_credentials/configure_credentials.md).

To define a Data Docs site configuration in Azure Blob Storage, update the values of `container`, `prefix`, and `connection_string` in the following code and execute it:

```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_abs.py - define a Data Docs configuratin dictionary"
```