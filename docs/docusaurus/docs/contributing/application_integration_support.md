---
title: Application support policy
---

The following are the levels of support offered by Great Expectations (GX) for integrated applications, operating systems, and programming languages.

These classifications allow GX to better prioritize its response to application integration, operating system, and programming language issues. Formally defining the support policy also provides contributors with a better understanding of who is responsible for resolving application integration, operating system, and programming language issues.

## Integrations

The following table defines the integrations supported by GX Cloud and GX OSS.


| Application Type                         | GX Cloud             | GX OSS          |
|------------------------------------------|----------------------|-----------------|
| Data Source                              | - Snowflake          | - Snowflake<br/> - Generic SQL ¹               |
| Configuration Stores                     | - N/A                | - Filesystem<br/> - AWS S3GCS Buckets<br/> - Azure Blob Store                |
| Communication (notifications)            | - N/A ²               | - Slack (local) ³<br/> - email (local)<br/> - Microsoft Teams (local)<br/> - PagerDuty (local)                 |
| Workflow Management                      | - Airflow ⁴           | - Airflow ⁴       |


¹ Connection strings for various SQL Data Sources are supported, but dialect-specific SQL commands are not.<br/>
² Support for Slack, email, and Zapier are planned for future releases.<br/>
³ When used with the GX OSS library.<br/>
⁴ Other Workflow Management applications that execute Python code are not supported.

## GX components

The following table defines the GX components supported by GX Cloud and GX OSS.

| Component                                | GX Cloud             | GX OSS          |
|------------------------------------------|----------------------|-----------------|
| Expectations                             | - See [Available Expectations](/docs/cloud/expectations/manage_expectations#available-expectations) ¹. | - See [Create Expectations](/docs/guides/expectations/expectations_lp) ¹.                |
| Checkpoints                              | - Running Validations<br/> - Running code snippets within an orchestrator.        | - Running code snippets within an orchestrator.                |
| GX Agent                                 | - All versions               | - N/A        |

¹ The creation and execution of custom Expectations are supported, but inner queries are not.

## Operating systems

The following table defines the operating systems supported by GX Cloud and GX OSS.

| GX Cloud                     | GX OSS                        |
|------------------------------|-------------------------------|
| - Mac/Linux ¹                | - Mac/Linux                   | 

¹ Required to run the GX Agent.

## Python versions

The following table defines the Python versions supported by GX Cloud and GX OSS. GX typically follows the [Python release cycle](https://devguide.python.org/versions/).

| GX Cloud                    | GX OSS                          |
|-----------------------------|---------------------------------|
| - N/A                       | - 3.8 to 3.11                   | 

## GX versions

The following table defines the GX versions supported by GX Cloud and GX OSS.

| GX Cloud                    | GX OSS                          |
|-----------------------------|---------------------------------|
| - N/A                       | - 0.17<br/> - 0.18              | 
