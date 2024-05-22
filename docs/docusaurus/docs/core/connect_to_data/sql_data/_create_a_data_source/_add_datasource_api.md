------------------------
| SQL Dialect        | GX API                                                                                               |
|--------------------|------------------------------------------------------------------------------------------------------|
| PostgreSQL         | [context.sources.add_postgresql(...)](/reference/api/datasource/fluent/PostgresDatasource_class.mdx) |
| Snowflake          | [context.sources.add_snowflake(...)](/reference/api/datasource/fluent/SnowflakeDatasource_class.mdx) |
| SQLite             | context.sources.add_sqlite(...)                                                                      |
| Databricks SQL     | context.sources.add_databricks_sql(...)                                                              |
| All other dialects | context.sources.add_or_update_sql(...)                                                               |
