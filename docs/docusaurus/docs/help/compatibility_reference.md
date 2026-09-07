---
title: Compatibility reference
hide_table_of_contents: true
---

The following table defines integrations and tools supported by GX Core.


| Service | GX Core | Notes |
|---|---|---|
| Data sources | AlloyDB<br/>Amazon Aurora PostgreSQL<br/>Amazon S3<br/>Azure Blob Storage<br/>BigQuery<br/>Citus<br/>Databricks SQL<br/>Google Cloud Storage<br/>Microsoft Fabric<br/>Microsoft SQL Server<br/>Neon<br/>Pandas<br/>PostgreSQL<br/>Redshift<br/>Snowflake<br/>Spark<br/>SQLite<br/>Trino  | We've seen GX Core work with the following data sources in the past, but we don't guarantee ongoing compatibility at this time: Athena, AWS Glue, Clickhouse, Databricks (Spark), Dremio, EMR Spark, MySQL, Teradata, and Vertica. |
| Actions| Email<br/>Microsoft Teams<br/>Slack<br/>Custom | We support the general workflow for creating custom Actions, but cannot help troubleshoot the domain-specific logic within a custom Action. |
| Credential stores | Environment variables<br/>`config_variables.yml` |  |
| Orchestrators | Airflow version 2.9.0+ | Although only Airflow is supported, GX Core should work with any orchestrator that executes Python code. |
| Operating systems | Mac/Linux | Though GX does not currently support Windows, we've seen users successfully deploy on Windows. |
| Python versions | 3.10 to 3.13 | GX typically follows the [Python release cycle](https://devguide.python.org/versions/). GX currently supports Python 3.10 to 3.13. Python 3.14 and later are not currently supported.|
| GX library versions | ≥1.0 | Support for 0.18 was deprecated October 25, 2024 and reached end of life on October 1, 2025. |
| Core dependencies | View [current core dependency support](https://github.com/fivetran/great_expectations/blob/develop/requirements.txt) in GitHub | GX typically supports core package dependencies for 2 years after initial release. Exceptions are made to support Amazon Managed Workflows for Apache Airflow (MWAA) - GX typically supports the versions of dependencies that are pinned in [MWAA constraints files](https://docs.aws.amazon.com/mwaa/latest/userguide/airflow-versions.html).  |
| Optional dependencies | View [current optional dependency support](https://github.com/fivetran/great_expectations/tree/develop/reqs) in GitHub | GX typically supports optional package dependencies for 1 year after initial release. Exceptions are made to support Amazon Managed Workflows for Apache Airflow (MWAA) - GX typically supports the versions of dependencies that are pinned in [MWAA constraints files](https://docs.aws.amazon.com/mwaa/latest/userguide/airflow-versions.html). |
| Web browsers | [Google Chrome](https://www.google.com/chrome/)<br/>[Mozilla Firefox](https://www.mozilla.org/en-US/firefox/)<br/>[Apple Safari](https://www.apple.com/safari/)<br/>[Microsoft Edge](https://www.microsoft.com/en-us/edge?ep=82&form=MA13KI&es=24) | Only the latest version of each browser is supported. |



