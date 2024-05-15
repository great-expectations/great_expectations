import InProgress from '../../_core_components/_in_progress.md';

Databricks is a web-based platform automating Spark cluster management and working with them through Python notebooks.

To avoid configuring external resources, you'll use the [Databricks File System (DBFS)](https://docs.databricks.com/data/databricks-file-system.html) for your Metadata Stores and Data Docs store.

DBFS is a distributed file system mounted in a Databricks workspace and available on Databricks clusters. Files on DBFS can be written and read as if they were on a local filesystem by <a href="https://docs.databricks.com/data/databricks-file-system.html#local-file-apis">adding the /dbfs/ prefix to the path</a>. It also persists in object storage, so you won’t lose data after terminating a cluster. See the Databricks documentation for best practices, including mounting object stores.

## Additional prerequisites

- A complete Databricks setup, including a running Databricks cluster with an attached notebook
- Access to [DBFS](https://docs.databricks.com/dbfs/index.html)

## Installation and setup

1. Run the following command in your notebook to install GX as a notebook-scoped library:

    ```bash title="Terminal input"
    %pip install great-expectations
    ```

  A notebook-scoped library is a custom Python environment that is specific to a notebook. You can also install a library at the cluster or workspace level. See [Databricks Libraries](https://docs.databricks.com/data/databricks-file-system.html).

2. Run the following command to import the Python configurations you'll use in the following steps:

  ```python title="Python" name="docs/docusaurus/docs/snippets/databricks_deployment_patterns_file_python_configs.py imports"
  ```

3. Run the following code to specify a `/dbfs/` path for your Data Context:

  ```python title="Python" name="docs/docusaurus/docs/snippets/databricks_deployment_patterns_file_python_configs.py choose context_root_dir"
  ```
4. Run the following code to instantiate your Data Context:

  ```python title="Python" name="docs/docusaurus/docs/snippets/databricks_deployment_patterns_file_python_configs.py set up context"
  ```

## Next steps

<InProgress />

- Connect to data in files stored in the DBFS
- Connect to data in an in-memory Spark Dataframe