We recommend that database credentials be stored in the `config_variables.yml` file, which is located in the `uncommitted/` folder by default, and is not part of source control. The following lines add database credentials under the key `db_creds`.

```yaml title="YAML file contents"
db_creds:
  drivername: postgres
  host: '<your_host_name>'
  port: '<your_port>'
  username: '<your_username>'
  password: '<your_password>'
  database: '<your_database_name>'
```

For additional options on configuring the `config_variables.yml` file or additional environment variables, please see our guide on [how to configure credentials](/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials).