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

| Integration Type                 | GX Cloud                              | GX Core                                                                                    | Community            |
| -------------------------------- | ------------------------------------- | ------------------------------------------------------------------------------------------ | -------------------- |
| Data Sources<sup>1</sup>         | Snowflake<br/>Databricks (SQL)<br/> PostgreSQL<sup>2</sup> | Snowflake<br/>Databricks (SQL)<br/>PostgreSQL<br/>Sqlite<br/>BigQuery<br/>Spark<br/>Pandas | MSSQL<br/>MySQL<br/> |
| Configuration Stores<sup>3</sup> | In-app                                | File system                                                                                | None                 |
| Data Doc Stores                  | In-app                                | File system                                                                                | None                 |
| Actions                          | Slack                                 | Slack <br/>Email                                                                           | None                 |
| Credential Stores                | Environment variables                 | Environment variables <br/> YAML<sup>4</sup>                                               | None                 |
| Orchestrator                     | Airflow <sup>5</sup> <sup>6</sup>     | Airflow <sup>5</sup> <sup>6</sup>                                                          | None                 |

<sup>1</sup> We've also seen GX work with the following data sources in the past but we can't guarantee ongoing compatibility. These data sources include Clickhouse, Vertica, Dremio, Teradata, Athena, EMR Spark, AWS Glue, Microsoft Fabric, Trino, Pandas on (S3, GCS, Azure), Databricks (Spark), and Spark on (S3, GCS, Azure).<br/>
<sup>2</sup> Support for BigQuery in GX Cloud will be available in a future release.<br/>
<sup>3</sup> This includes configuration storage for Expectations, Checkpoints, Validation Definitions, and Validation Result<br/>
<sup>4</sup> config_variables.yml<br/>
<sup>5</sup> Although only Airflow is supported, GX Cloud and GX Core should work with any orchestrator that executes Python code.<br/>
<sup>6</sup> Airflow version 2.9.0+ required<br/>

### GX components

The following table defines the GX components supported by GX Cloud and GX Core.

| Component    | GX Cloud                                                                                        | GX Core                                                               | Community                                                                  |
| ------------ | ----------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| Expectations | See [Available Expectations](/cloud/expectations/manage_expectations.md#available-expectations) | See [Expectations Gallery](https://greatexpectations.io/expectations) | See [Legacy Gallery](https://greatexpectations.io/legacy/v1/expectations/) |
| GX Agent     | All versions                                                                                    | N/A                                                                   | N/A                                                                        |

### Operating systems

The following table defines the operating systems supported by GX Cloud and GX Core.

| GX Cloud               | GX Core   | Community |
| ---------------------- | --------- | --------- |
| Mac/Linux <sup>1</sup> | Mac/Linux | Mac/Linux |

<sup>1</sup> GX does not currently support Windows. However, we've seen users successfully deploying GX on Windows.

### Python versions

The following table defines the Python versions supported by GX Cloud and GX Core. GX typically follows the [Python release cycle](https://devguide.python.org/versions/).

| GX Cloud    | GX Core     | Community   |
| ----------- | ----------- | ----------- |
| 3.8 to 3.11 | 3.8 to 3.11 | 3.8 to 3.11 |

### GX versions

The following table defines the GX versions supported by GX Cloud and GX Core.

| GX Cloud | GX Core | Community |
| -------- | ------- | --------- |
| ≥1.0     | ≥1.0    | ≥1.0      |

### Web browsers

The following web browsers are supported by GX Cloud.

- [Google Chrome](https://www.google.com/chrome/) — the latest version is fully supported

- [Mozilla Firefox](https://www.mozilla.org/en-US/firefox/) — the latest version is fully supported

- [Apple Safari](https://www.apple.com/safari/) — the latest version is fully supported

- [Microsoft Edge](https://www.microsoft.com/en-us/edge?ep=82&form=MA13KI&es=24) — the latest version is fully supported
