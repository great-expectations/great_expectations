The Azure Blob Storage examples use `azure-storage-blob`. They read the storage account URL from the `AZURE_STORAGE_ACCOUNT_URL` environment variable and the credential from `AZURE_CREDENTIAL`. The credential string can be a SAS token or an account key. Neither can give one process different access to different prefixes. An account key gives full access to the storage account's data, and a SAS token applies one set of permissions to everything it covers, such as a whole container. To limit each process to the locations that [How the work is divided](#how-the-work-is-divided) lists, sign in with Microsoft Entra ID instead and add a condition on the blob path to each role assignment. To sign in that way, pass a credential object from `azure-identity` as `credential`. See [BlobServiceClient](https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient) in the Azure SDK documentation.

Replace `my-container` with the name of your container. `project_prefix` is where the project's files go in the container, and `data_docs_prefix` is where the Data Docs site goes. Then add the following code:

```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_abs.py - define the storage location"
```

The following functions copy files between a local folder and the container, and the next steps use them. `upload_files()` keeps each file's path relative to the local folder. It also sets each file's content type from the file name. A browser needs the content type to display the Data Docs pages. `download_project()` skips `uncommitted/` unless you pass `include_results=True`. It also skips folder entries, including the ones in listings from accounts with a hierarchical namespace, and blobs whose names contain a `..` segment, which would land outside the download folder.

```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/sync_with_cloud_storage/_examples/sync_with_cloud_storage_abs.py - define transfer functions"
```
