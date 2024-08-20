
A local or networked filesystem Data Doc site requires the following `store_backend` information:

- `class_name`: The name of the class to implement for accessing the target environment.  For a local or networked filesystem this will be `TupleFilesystemStoreBackend`.
- `base_directory`: A path to the folder where the static sites should be created.  This can be an absolute path, or a path relative to the root folder of the Data Context.

To add a local or networked filesystem backend for your Data Docs configuration, update the value of `base_directory` in the following code and execute it:

```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - add store backend"
```