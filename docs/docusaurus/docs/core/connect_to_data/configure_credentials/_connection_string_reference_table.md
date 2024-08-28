
| Database type   | Connection string                                                                                                                                                |
|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| PostgreSQL      | `postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>`                                                                                           |
| SQLite          | `sqlite:///<PATH_TO_DB_FILE>`                                                                                                                                    |
| Snowflake       | `snowflake://<USER_NAME>:<PASSWORD>@<ACCOUNT_NAME>/<DATABASE_NAME>/<SCHEMA_NAME>?warehouse=<WAREHOUSE_NAME>&role=<ROLE_NAME>&application=great_expectations_oss` |
| Databricks SQL  | `databricks://token:<TOKEN>@<HOST>:<PORT>?http_path=<HTTP_PATH>&catalog=<CATALOG>&schema=<SCHEMA>`                                                   |
| BigQuery SQL    | `bigquery://<GCP_PROJECT>/<BIGQUERY_DATASET>?credentials_path=/path/to/your/credentials.json`                                                                    |


