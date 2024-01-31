We recommend that database credentials be stored in the `config_variables.yml` file, which is located in the `uncommitted/` folder by default, and is not part of source control.

You can store values in `config_variables.yml` such as:

```yaml title="YAML file contents"
credentials:
  username: '<your_username>'
  password: '<your_password>'
```

To reference these values in a connection string, you would simply include the relevant key in the string, denoted by containing `$` characters.  For example:

```python
my_connection_string = "${credentials}"
```


For additional options on configuring the `config_variables.yml` file or additional environment variables, please see our guide on [how to configure credentials(/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials).