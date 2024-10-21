
A local or networked filesystem Data Doc site requires the following `store_backend` information:

- `base_directory`: A path to the folder where the static sites should be created.  This can be an absolute path, or a path relative to the root folder of the Data Context.
- `class_name`: This value must be `TupleFilesystemStoreBackend`, and is not user-configurable.


To define a Data Docs site configuration for a local or networked filesystem environment, update the value of `base_directory` in the following code and execute it:

```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - define a data docs config dictionary"
```