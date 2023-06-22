# Snowflake Reference Environment

This reference environment spins up a single container:

- A jupyter notebook server

The jupyter notebook server contains a notebook with a quickstart for using Great Expectations with snowflake.

GX will use the `snowflake-sqlalchemy` connector to connect to Snowflake. 

You can connect to the database by setting up your Snowflake connection string as an environment variable named `SNOWFLAKE_CONNECTION_STRING`:
```bash
SNOWFLAKE_CONNECTION_STRING='snowflake://<user_login_name>:<password>@<account_identifier>'
```

More information on the Connection Parameters you can use with the `sqlalchemy-snowflake` connector can be found at the [Snowflake developer guide](https://docs.snowflake.com/developer-guide/python-connector/sqlalchemy#connection-parameters
).

Please also note that the jupyter notebook will use the default ports, so please make sure you don't have anything else running on those ports, or take steps to avoid port conflicts.
