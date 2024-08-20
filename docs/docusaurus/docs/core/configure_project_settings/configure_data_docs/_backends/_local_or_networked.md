Local or networked filesystem
   ```python title="Python"
   site_config["store_backend"] = {
      "class_name": "TupleFilesystemStoreBackend"
      "base_directory": "uncommitted/data_docs/local_site/" # this is the default path (relative to the root folder of the Data Context) but can be changed as required
   }
   ```