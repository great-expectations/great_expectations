| Database type        | Data Context method                                                          |
|----------------------|------------------------------------------------------------------------------|
| BigQuery             | `context.data_sources.add_bigquery(name: str, connection_string: str)`       |
| DataBricks SQL       | `context.data_sources.add_databricks_sql(name: str, connection_string: str)` |
| Microsoft Fabric     | `context.data_sources.add_fabric(name: str, host: str, database: str, schema: str, port: int, encrypt: Literal["Mandatory", "Optional", "Strict"], trust_server_certificate: bool = False, driver: str, tenant_id: str, client_id: str, client_secret: str,)`       |
| Microsoft SQL Server | `context.data_sources.add_sql_server(name: str, host: str, database: str, schema: str, port: int, encrypt: Literal["Mandatory", "Optional", "Strict"], trust_server_certificate: bool = False, driver: str, authentication: Literal["SQL Server", "Entra ID"], username: str, password: str, tenant_id: str, client_id: str, client_secret: str,)`       |
| Oracle               | `context.data_sources.add_sql(name: str, connection_string: str)`            |
| PostgreSQL           | `context.data_sources.add_postgres(name: str, connection_string: str)`       |
| Redshift             | `context.data_sources.add_redshift(name: str, connection_string: str)`       |
| Snowflake            | `context.data_sources.add_snowflake(name: str, account: str, user: str, database: str, schema: str, warehouse: str, role: str, private_key: str)` |
| SQLite               | `context.data_sources.add_sqlite(name: str, connection_string: str)`         |
| Other SQL            | `context.data_sources.add_sql(name: str, connection_string: str)`            |