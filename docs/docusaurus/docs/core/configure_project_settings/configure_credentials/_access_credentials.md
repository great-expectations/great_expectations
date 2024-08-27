import GxData from '../../_core_components/_data.jsx';

Securely stored credentials are implemented via string substitution.  You can reference your credentials in a Python string by wrapping the variable name they are assigned to in `${` and `}`.  Using individual credentials for a connection string would look like:

```python title="Python"
connection_string="postgresql+psycopg2://${MY_POSTGRES_USERNAME}:${MY_POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DATABASE}",
```

Or you could reference a configured variable that contains the full connection string by providing a Python string that contains just a reference to that variable:

```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_source/postgres.py - example postgresql connection string using string substitution"
```

When you pass a string that references your stored credentials to a GX Core method that requires string formatted credentials as a parameter the referenced variable in your Python string will be substituted for the corresponding stored value.
