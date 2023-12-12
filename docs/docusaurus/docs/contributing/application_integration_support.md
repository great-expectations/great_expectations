---
title: Integration support policy
---

A defined integration support policy allows Great Expectations (GX) to prioritize its response to application integration, operating system, and programming language issues.

For production environments where uptime is critical, GX recommends using the applications, operating systems, and programming languages defined as GX-supported. Community-supported platforms such as Pandas, Spark, and SQLAlchemy allow you to integrate your existing datasets with GX Cloud and GX OSS.

## Support categories

The following are the levels of support provided by GX:

- GX-supported - integrations are tested throughout the development lifecycle, and actively maintained and updated when new versions of GX Cloud or GX OSS are released.

- Community-supported - integrations are successfully implemented by community members. They are not tested or updated by GX, and GX is not responsible for ensuring reliability or compatibility.

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


| Application Type                         | GX Cloud             | GX OSS          |
|------------------------------------------|----------------------|-----------------|
| Data Source                              | Snowflake           | - Snowflake<br/> - Generic SQL ¹               |
| Configuration Stores                     | N/A                 | - Filesystem<br/> - AWS S3GCS Buckets<br/> - Azure Blob Store                |
| Communication (notifications)            | N/A ²               | - Slack (local) ³<br/> - email (local)<br/> - Microsoft Teams (local)<br/> - PagerDuty (local)                 |
| Orchestrator                              | Airflow ⁴           | Airflow ⁴       |


¹ Connection strings for various SQL Data Sources are supported, but dialect-specific SQL commands are not.<br/>
² Support for Slack, email, and Zapier are planned for future releases.<br/>
³ When used with the GX OSS library.<br/>
⁴ Although only Airflow is supported, GX Cloud and GX OSS should work with any orchestrator that executes Python code.

### GX components

The following table defines the GX components supported by GX Cloud and GX OSS.

| Component                                | GX Cloud             | GX OSS          |
|------------------------------------------|----------------------|-----------------|
| Expectations                             | See [Available Expectations](/docs/cloud/expectations/manage_expectations#available-expectations). | See [Create Expectations](/docs/guides/expectations/expectations_lp).                |
| GX Agent                                 | All versions               | N/A        |


## Community supported

The following integrated applications, operating systems, and programming languages are supported by the community.

### Operating systems

The following table defines the operating systems supported by the community.

| GX Cloud                       | GX OSS                        |
|--------------------------------|-------------------------------|
| Windows ¹                      | Windows ¹                     | 

¹ Untested and unsupported by GX.

### Integrations

The following table defines the GX Cloud and GX OSS integrations supported by the community.


| Application Type                         | GX Cloud             | GX OSS          |
|------------------------------------------|----------------------|-----------------|
| Data Source                              | Snowflake          | - Redshift<br/>- MSSQL<br/>- MySQL<br/>- SQLite<br/>- Trino<br/>- AWS S3<br/>- Google Cloud Storage<br/>- Azure Blog Storage<br/>- Clickhouse ¹<br/>- Athena ¹<br/>- Dremio ¹<br/>- Teradata ¹<br/>- Vertica ¹<br/>- EMR Spark ²<br/>- AWS Glue ²               |
| Configuration Stores                     | N/A                | Azure Blob Store                |
| Communication (notifications)            | N/A                | - Slack (local) ³<br/> - email (local)<br/> - Microsoft Teams (local)<br/> - PagerDuty (local)<br/>- Opsgenie<br/>- Amazon SNS<br/>- General API<br/>- DataHub ⁴                   |
| Orchestrator                              | Airflow ⁵           | - Airflow<br/>- Prefect<br/>- Dagster ⁶<br/>- Flyte ⁶<br/>- mage.ai ⁶<br/>- Github Action ⁶        |


¹ Partially tested and supported by GX.<br/>
² Untested and unsupported by GX.<br/>
³ When used with the GX OSS library.<br/>
⁴ Untested and unsupported by GX.<br/>
⁵ Other Orchestrator applications that execute Python code are not supported.<br/>
⁶ Untested and unsupported by GX.

## Unsupported applications

The following applications are not supported by GX or the community:

- Acryl Datahub
- Atlan
- Amundsen
- Azure Databricks
- Meltano
- dbt
- Capital One Data Profiler
- AWS Databricks
