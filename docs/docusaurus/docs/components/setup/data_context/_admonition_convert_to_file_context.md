An Ephemeral Data Context is an in-memory Data Context that is not intended to persist beyond the current Python session.  However, if you decide that you would like to save its contents for future use you can do so by converting it to a Filesystem Data Context:

```python title="Python code"
context = context.convert_to_file_context()
```

This method will initialize a Filesystem Data Context in the current working directory of the Python process that contains the Ephemeral Data Context.  For more detailed explanation of this method, please see our guide on [how to convert an ephemeral data context to a filesystem data context](/docs/guides/setup/configuring_data_contexts/how_to_convert_an_ephemeral_data_context_to_a_filesystem_data_context)