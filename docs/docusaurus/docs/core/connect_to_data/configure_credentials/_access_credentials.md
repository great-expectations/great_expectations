import GxData from '../../_core_components/_data.jsx';

Securely stored credentials are accessed via string substitution.  You can reference your credentials in a Python string by wrapping the variable name in `${` and `}`.  Using individual credentials would look like:

```python title="Python"
connection_string="postgresql+psycopg2://${MY_POSTGRES_USERNAME}:${MY_POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DATABASE}",
```

Or you could reference a configured variable that contains the full connection string by providing a Python string that contains just a reference to that variable:

```python title="Python"
connection_string="${POSTGRES_CONNECTION_STRING}"
```

When you pass a string that references your stored credentials to a GX Core method that requires a connection string as a parameter the referenced variable will be substituted for the corresponding stored value.
