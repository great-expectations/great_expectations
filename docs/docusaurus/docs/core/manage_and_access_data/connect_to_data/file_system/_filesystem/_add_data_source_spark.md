
1. In a Python environment [import GX and instantiate a Data Context](core/installation_and_setup/manage_data_contexts.md).  This example assumes the variable `context` points to your Data Context.

2. Define the parameters for an Amazon S3 Data Source that uses Spark to access data.

    - `name`: The Data Source name.  In the following example, this is `"my_filesystem_datasource"`.
    - `base_directory`: The path to the folder that contains your data files.
    
    ```python title="Python"
    datasource_name = "my_filesystem_datasource"
    path_to_folder_containing_csv_files = "<PATH_TO_FOLDER_WITH_YOUR_CSV_FILES>"
    ```

   :::info Using relative paths as the `base_directory` of a filesystem Data Source
   If you are using a File Data Context you can provide a path for `base_directory` that is relative to the folder containing your Data Context.

   However, in-memory Ephemeral Data Contexts and Cloud Data Contexts do not exist in a folder.  Therefore, when using an Ephemeral Data Context or a Cloud Data Context relative paths will be determined based on the folder your Python code is being executed in, instead.
   :::

3. Run the following Python code to pass `datasource_name` and `path_to_folder_containing_csv_files` as parameters and create your filesystem Data Source:

  ```python title="Python"
datasource = context.sources.add_spark_filesystem(
    name=datasource_name,
    base_directory=path_to_folder_containing_csv_files,
) 
  ```