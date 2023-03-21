
:::info Using relative paths as the `base_directory` of a Filesystem Datasource

If you are using a Filesystem Data Context you can provide a path for `base_directory` that is relative to the folder containing your Data Context.

However, an in-memory Ephemeral Data Context doesn't exist in a folder.  Therefore, when using an Ephemeral Data Context, relative paths will be determined based on the folder your Python code is being executed in, instead.

:::