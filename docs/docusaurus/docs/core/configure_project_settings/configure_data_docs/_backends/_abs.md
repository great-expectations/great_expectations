Azure
   ```python title="Python"
   site_config["store_backend"] = {
      "class_name": "TupleAzureBlobStoreBackend",
      "container": "another",
      "prefix": "path_relative_to_container_root",
      "connection_string": "${AZURE_STORAGE_CONNECTION_STRING}"
   }
   ```