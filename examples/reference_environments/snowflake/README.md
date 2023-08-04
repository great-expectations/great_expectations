# Snowflake Reference Environment

This reference environment spins up two containers:

- A jupyter notebook server
- A server to host data docs (to view navigate to http://127.0.0.1:3000/)

The jupyter notebook server contains a notebook with a quickstart for using Great Expectations with snowflake.

GX will use the `snowflake-sqlalchemy` connector to connect to Snowflake.

You can connect to the database by setting up your Snowflake connection string as an environment variable named `SNOWFLAKE_CONNECTION_STRING`:
```bash
SNOWFLAKE_CONNECTION_STRING='snowflake://<user_login_name>:<password>@<account_identifier>'
```

More information on the Connection Parameters you can use with the `sqlalchemy-snowflake` connector can be found at the [Snowflake developer guide](https://docs.snowflake.com/developer-guide/python-connector/sqlalchemy#connection-parameters
).

Please also note that the jupyter notebook will use the default ports, so please make sure you don't have anything else running on those ports, or take steps to avoid port conflicts.

The container serving the jupyter notebook will install Great Expectations with the appropriate dependencies based on the latest release, then install from the latest on the `develop` branch. To modify this e.g. install from the latest release only or from a different branch, edit `jupyter.Dockerfile`.
