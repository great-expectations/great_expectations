---
title: Integration support policy
---

For production environments, GX recommends using GX Cloud integrations.

GX uses libraries such as Pandas, Spark, and SQLAlchemy to integrate with different Data Sources. This also allows you to deploy GX with community-supported integrations.

## Levels of support

The following are the levels of support provided by GX:

- <b>GX Cloud</b> - GX Cloud supported integrations are available in GX Cloud. They are tested and are actively maintained with new GX Cloud releases.

- <b>GX Core</b> - GX Core supported integrations are available in GX Core. They are tested and are actively maintained with new GX Core releases.

- <b>Community</b> - Community supported integrations were initially implemented by GX or the community. It is up to the community for ongoing maintenance

## Integrations

The following table defines the GX Cloud, GX Core, and Community Supported integrations.

| Integration Type      | GX Cloud                   | GX Core                                                                       | Community                         |
| --------------------- | -------------------------- | ----------------------------------------------------------------------------- | --------------------------------- |
| Data Sources¹         | Snowflake<br/> PostgreSQL² | Snowflake<br/>PostgreSQL<br/>Sqlite<br/>Databricks (SQL)<br/>Spark<br/>Pandas | Redshift<br/>MSSQL<br/>MySQL<br/> |
| Configuration Stores³ | In-app                     | File system                                                                   | None                              |
| Actions               | Slack                      | Slack ⁴<br/>Email                                                             | None                              |
| Credential Store      | Environment variables      | Environment variables <br/> YAML⁴                                             | None                              |
| Orchestrator          | Airflow ⁵                  | Airflow ⁵                                                                     | None                              |

¹ We've also seen GX work with the following data sources in the past but we can't guarentee ongoing compatibility. These data sources include Clickhouse, Vertica, Dremio, Teradata, Athena, EMR Spark, AWS Glue, Microsoft Fabric, Trino, Pandas on (S3, GCS, Azure), Databricks (Spark), and Spark on (S3, GCS, Azure).<br/>
² Support for BigQuery in GX Cloud will be available in a future release.<br/>
³ This includes configuration storage for Expectations, Checkpoints, Validation Definitions, and Validation Result<br/>
⁴ config_variables.yml<br/>
⁵ Although only Airflow is supported, GX Cloud and GX Core should work with any orchestrator that executes Python code.<br/>

### GX components

The following table defines the GX components supported by GX Cloud and GX Core.

| Component    | GX Cloud                                                                                        | GX Core                                                               | Community                                                                  |
| ------------ | ----------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| Expectations | See [Available Expectations](/cloud/expectations/manage_expectations.md#available-expectations) | See [Expectations Gallery](https://greatexpectations.io/expectations) | See [Legacy Gallery](https://greatexpectations.io/legacy/v1/expectations/) |
| GX Agent     | All versions                                                                                    | N/A                                                                   | N/A                                                                        |


### Operating systems

The following table defines the operating systems supported by GX Cloud and GX Core.

| GX Cloud    | GX Core   | Community |
| ----------- | --------- | --------- |
| Mac/Linux ¹ | Mac/Linux | Mac/Linux |

¹ GX does not currently support Windows. However, we've seen users successfuly deploying GX on Windows.

### Python versions

The following table defines the Python versions supported by GX Cloud and GX Core. GX typically follows the [Python release cycle](https://devguide.python.org/versions/).

| GX Cloud    | GX Core     | Community   |
| ----------- | ----------- | ----------- |
| 3.8 to 3.11 | 3.8 to 3.11 | 3.8 to 3.11 |

### GX versions

The following table defines the GX versions supported by GX Cloud and GX Core.

| GX Cloud | GX Core | Community     |
| -------- | ------- | ------------- |
| >1.0     | >1.0    | 0.17<br/>0.18 |
