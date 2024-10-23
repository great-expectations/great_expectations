| Database type    | Data Context method                                                          |
|------------------|------------------------------------------------------------------------------|
| PostgreSQL       | `context.data_sources.add_postgres(name: str, connection_string: str)`       |
| SQLite | `context.data_sources.add_sqlite(name: str, connection_string: str)`         |
| Snowflake        | `context.data_sources.add_snowflake(name:str, connection_string: str)`       |
| DataBricks SQL   | `context.data_sources.add_databricks_sql(name: str, connection_string: str)` |
| Other SQL        | `context.data_sources.add_sql(name: str, connection_string:str)`             |