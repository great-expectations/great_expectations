| Database type    | Data Context method                                                |
|------------------|--------------------------------------------------------------------|
| PostgreSQL       | `context.data_sources.add_postgres(name, connection_string)`       |
| SQLite | `context.data_sources.add_sqlite(name, connection_string)`         |
| DataBricks SQL   | `context.data_sources.add_databricks_sql(name, connection_string)` |
| Snowflake        | `context.data_sources.add_snowflake(name, connection_string)`      |
| Other SQL        | `context.data_sources.add_sql(name, connection_string)`            |