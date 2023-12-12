---
title: Application support policy
---

A defined application support policy allows Great Expectations (GX) to better prioritize its response to application integration, operating system, and programming language issues. Formally defining the support policy also provides contributors with a better understanding of who is responsible for resolving application integration, operating system, and programming language issues.

## Support categories

The following are the types of support available for application integration, operating system, and programming language issues:

- **GX supported** - The GX Developer Relations (DevRel) team triages, reproduces, and then assigns issues to the appropriate GX Engineering team. Issues with application integration, operating system, and programming languages in maintenance mode are reviewed, but new features or functionality are not implemented as part of the resolution.

- **Community supported** - The GX DevRel team triages and reproduces issues, and then determines if GX should implement new features or functionality to resolve the issue. If the DevRel team determines that new features or functionality are not warranted, the GX community is responsible for issue resolution. If new features or functionality are required, DevRel assigns the issue to the appropriate GX Engineering team.

## GX supported

The following are the levels of support offered by GX for integrated applications, operating systems, and programming languages.

### Operating systems

The following table defines the operating systems supported by GX Cloud and GX OSS.

| GX Cloud                     | GX OSS                        |
|------------------------------|-------------------------------|
| - Mac/Linux ¹                | - Mac/Linux                   | 

¹ Required to run the GX Agent.

### Python versions

The following table defines the Python versions supported by GX Cloud and GX OSS. GX typically follows the [Python release cycle](https://devguide.python.org/versions/).

| GX Cloud                    | GX OSS                          |
|-----------------------------|---------------------------------|
| - N/A                       | - 3.8 to 3.11                   | 

### GX versions

The following table defines the GX versions supported by GX Cloud and GX OSS.

| GX Cloud                    | GX OSS                          |
|-----------------------------|---------------------------------|
| - N/A                       | - 0.17<br/> - 0.18              | 

### Integrations

The following table defines the supported GX Cloud and GX OSS integrations.


| Application Type                         | GX Cloud             | GX OSS          |
|------------------------------------------|----------------------|-----------------|
| Data Source                              | - Snowflake          | - Snowflake<br/> - Generic SQL ¹               |
| Configuration Stores                     | - N/A                | - Filesystem<br/> - AWS S3GCS Buckets<br/> - Azure Blob Store                |
| Communication (notifications)            | - N/A ²               | - Slack (local) ³<br/> - email (local)<br/> - Microsoft Teams (local)<br/> - PagerDuty (local)                 |
| Orchestrator                              | - Airflow ⁴           | - Airflow ⁴       |


¹ Connection strings for various SQL Data Sources are supported, but dialect-specific SQL commands are not.<br/>
² Support for Slack, email, and Zapier are planned for future releases.<br/>
³ When used with the GX OSS library.<br/>
⁴ Other Orchestrator applications that execute Python code are not supported.

### GX components

The following table defines the GX components supported by GX Cloud and GX OSS.

| Component                                | GX Cloud             | GX OSS          |
|------------------------------------------|----------------------|-----------------|
| Expectations                             | - See [Available Expectations](/docs/cloud/expectations/manage_expectations#available-expectations) ¹. | - See [Create Expectations](/docs/guides/expectations/expectations_lp) ¹.                |
| GX Agent                                 | - All versions               | - N/A        |

¹ The creation and execution of custom Expectations are supported, but inner queries are not.

## Community supported

The following integrated applications, operating systems, and programming languages are supported by the community.

### Operating systems

The following table defines the operating systems supported by the community.

| GX Cloud                       | GX OSS                        |
|--------------------------------|-------------------------------|
| - Mac/Linux ¹<br/>- Windows ²  | - Mac/Linux<br/>- Windows ²   | 

¹ Required to run the GX Agent.<br/>
² Untested and unsupported by GX

### Python versions

The following table defines the Python versions supported by the community.

| GX Cloud                    | GX OSS                          |
|-----------------------------|---------------------------------|
| - N/A                       | - 3.7.X and earlier             | 
 

### GX versions

The following table defines the GX versions supported by the community.

| GX Cloud                    | GX OSS                          |
|-----------------------------|---------------------------------|
| - N/A                       | - Previous two minor releases. For example, 0.15.X, 0.16.X              | 

### Integrations

The following table defines the GX Cloud and GX OSS integrations supported by the community.


| Application Type                         | GX Cloud             | GX OSS          |
|------------------------------------------|----------------------|-----------------|
| Data Source                              | - Snowflake          | - Redshift<br/>- MSSQL<br/>- MySQL<br/>- SQLite<br/>- Trino<br/>- AWS S3<br/>- Google Cloud Storage<br/>- Azure Blog Storage<br/>- Clickhouse ¹<br/>- Athena ¹<br/>- Dremio ¹<br/>- Teradata ¹<br/>- Vertica ¹<br/>- EMR Spark ²<br/>- AWS Glue ²               |
| Configuration Stores                     | - N/A                | - Azure Blob Store                |
| Communication (notifications)            | - N/A                | - Slack (local) ³<br/> - email (local)<br/> - Microsoft Teams (local)<br/> - PagerDuty (local)<br/>- Opsgenie<br/>- Amazon SNS<br/>- General API<br/>- DataHub ⁴                   |
| Orchestrator                              | - Airflow ⁵           | - Airflow<br/>- Prefect<br/>- Dagster ⁶<br/>- Flyte ⁶<br/>- mage.ai ⁶<br/>- Github Action ⁶        |


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
