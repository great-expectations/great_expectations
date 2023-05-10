In the absence of provided parameters, the `get_context(...)` command will return your Cloud or Filesystem Data Context if you have previously initialized one.  If you have not, it will return an Ephemeral Data Context.  Ephemeral Data Contexts do not persist the Datasources and 

If you have not previously initialized a Data Context, you can easily do so by passing `get_context(...)` a path to a folder in which you would like a Filesystem Data Context to be initialized, like so:

```python title="Python code"
context = gx.get_context("/path/to/a/folder")
```

After initializing the Filesystem Data Context in the specified folder, `get_context(...)` will then instantiate and return it.

You can also use the above command to specify which Filesystem Data Context to instantiate, if you happen to have initialized more than one for different projects.  If the path provided corresponds to a previously initialized Data Context, `get_context(...)` will simply instantiate and return the Data Context found at that path.