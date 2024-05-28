Environment variables provide the quickest way to securely set up your credentials. 

You can set environment variables by replacing the values in `<>` with your information and entering `export <VARIABLE_NAME>=<VALUE>` commands in the terminal or adding the commands to your `~/.bashrc` file.  If you use the `export` command from the terminal, the environment variables will not persist beyond the current session.  If you add them to the `~/.bashrc` file, the variables will be exported each time you log in.

You can export individual credentials or an entire connection string.  For example:

```bash title="Terminal" name="docs/docusaurus/docs/oss/guides/setup/configuring_data_contexts/how_to_configure_credentials.py export_env_vars"
export MY_POSTGRES_PASSWORD=<PASSWORD>
export MY_POSTGRES_USERNAME=<USERNAME>
```

or:

```bash title="Terminal"
export POSTGRES_CONNECTION_STRING=postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>
```

These can then be loaded into the `connection_string` parameter when you are adding a Data Source to the Data Context by wrapping the variable name in `${` and `}`.  Using individual credentials would look like:

```python title="Python"
connection_string="postgresql+psycopg2://${MY_POSTGRES_USERNAME}:${MY_POSTGRES_PASSWORD}@<HOST>:<PORT>/<DATABASE>",
```

Or you could reference the full connection sting with:

```python title="Python"
connection_string="${POSTGRES_CONNECTION_STRING}"
```