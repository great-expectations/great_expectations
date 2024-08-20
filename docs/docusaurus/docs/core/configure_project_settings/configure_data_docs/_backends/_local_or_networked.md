
A local or networked filesystem Data Doc site requires the following `store_backend` information:

- `class_name`: The name of the class to implement for accessing the target environment.  For a local or networked filesystem this will be `TupleFilesystemStoreBackend`.
- `base_directory`: A path to the folder where the static sites should be created.  This can be an absolute path, or a path relative to the root folder of the Data Context.

To add a local or networked filesystem backend for your Data Docs configuration, update the value of `base_directory` in the following code and execute it:

```python title="Python"
base_directory = "uncommitted/data_docs/local_site/" # this is the default path (relative to the root folder of the Data Context) but can be changed as required

site_config["store_backend"] = {
  "class_name": "TupleFilesystemStoreBackend"
  "base_directory": base_directory
}
```