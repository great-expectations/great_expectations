| Database type    | Data Context method                                   |
|------------------|-------------------------------------------------------|
| PostgreSQL       | `context.add_postgres(name, connection_string)`       |
| DataBricks SQL   | `context.add_databricks_sql(name, connection_string)` |
| Snowflake        | `context.add_snowflake(name, connection_string)`      |
| Other SQL        | `context.add_sql(name, connection_string)`            |