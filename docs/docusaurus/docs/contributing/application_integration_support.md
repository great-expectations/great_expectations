---
title: Integration support policy
---

For production environments, GX recommends using the integrations defined as GX-supported. 

GX uses libraries such as Pandas, Spark, and SQLAlchemy to integrate with a wide range of data sources. This also allows you to deploy GX with Community-supported integrations.

## Support categories

The following are the levels of support provided by GX:

- GX-supported - integrations are tested throughout the development lifecycle, and actively maintained and updated when new versions of GX Cloud or GX OSS are released.

- Community-supported - integrations are implemented by community members. They are not tested or updated by GX, and GX is not responsible for ensuring reliability or compatibility.

## GX supported

The following are the levels of support offered by GX for integrated applications, operating systems, and programming languages.

### Operating systems

The following table defines the operating systems supported by GX Cloud and GX OSS.

| GX Cloud                     | GX OSS                        |
|------------------------------|-------------------------------|
| Mac/Linux ¹                  | Mac/Linux                     | 

¹ Required to run the GX Agent.

### Python versions

The following table defines the Python versions supported by GX Cloud and GX OSS. GX typically follows the [Python release cycle](https://devguide.python.org/versions/).

| GX Cloud                    | GX OSS                          |
|-----------------------------|---------------------------------|
| N/A                         | 3.8 to 3.11                     | 

### GX versions

The following table defines the GX versions supported by GX Cloud and GX OSS.

| GX Cloud                    | GX OSS                          |
|-----------------------------|---------------------------------|
| N/A                         | - 0.17<br/> - 0.18              | 

### Integrations

The following table defines the supported GX Cloud and GX OSS integrations.


| Integration Type                         | GX Cloud             | GX OSS          |
|------------------------------------------|----------------------|-----------------|
| Data Source                              | Snowflake ¹          | - Snowflake<br/> - Generic SQL ²               |
| Configuration Stores                     | N/A                  | - Filesystem<br/> - AWS S3GCS Buckets<br/> - Azure Blob Store                |
| Notifications                            | N/A ³                | - Slack (local) ⁴<br/> - email (local)<br/> - Microsoft Teams (local)<br/> - PagerDuty (local)                 |
| Orchestrator                              | Airflow ⁵           | Airflow ⁵       |


¹ Support for BigQuery will be available in a future release.<br/>
² Connection strings for various SQL Data Sources are supported, but dialect-specific SQL commands are not.<br/>
³ Support for Zapier will be available in a future release.<br/>
⁴ When used with the GX OSS library.<br/>
⁵ Although only Airflow is supported, GX Cloud and GX OSS should work with any orchestrator that executes Python code.

### GX components

The following table defines the GX components supported by GX Cloud and GX OSS.

| Component                                | GX Cloud             | GX OSS          |
|------------------------------------------|----------------------|-----------------|
| Expectations                             | See [Available Expectations](/docs/cloud/expectations/manage_expectations#available-expectations). | See [Create Expectations](/docs/guides/expectations/expectations_lp).                |
| GX Agent                                 | All versions               | N/A        |


## Community supported

The following integrated applications, operating systems, and programming languages are supported by the community.

### Operating systems

The following table lists the operating systems supported by the community.

| GX Cloud                       | GX OSS                        |
|--------------------------------|-------------------------------|
| Windows ¹                      | Windows ¹                     | 

¹ Untested and unsupported by GX.

### Integrations

The following table lists the GX Cloud and GX OSS integrations supported by the community.


| Integration Type                         | GX Cloud             | GX OSS          |
|------------------------------------------|----------------------|-----------------|
| Data Source                              | N/A          | - Pandas<br/>- Spark<br/>- Databricks (Spark)<br/>- Databricks (SQL)<br/>- Trino<br/>- Clickhouse<br/>- Dremio<br/>- Teradata<br/>- Vertica<br/>- EMR Spark<br/>- AWS Glue<br/>- Google Cloud Storage<br/>- Azure Blog Storage<br/>- AWS S3|
| Notifications                             | N/A            | - Opsgenie<br/>- Amazon SNS<br/>- DataHub |
| Orchestrator                              | N/A            | - Prefect<br/>- Dagster <br/>- Flyte <br/>- mage.ai  |
