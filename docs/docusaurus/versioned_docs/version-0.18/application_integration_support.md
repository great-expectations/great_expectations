---
title: Integration support policy
---

For production environments, GX recommends using the integrations defined as GX-supported.

GX uses libraries such as Pandas, Spark, and SQLAlchemy to integrate with different Data Sources. This also allows you to deploy GX with community-supported integrations.

## Support categories

The following are the levels of support provided by GX:

- GX-supported - integrations are tested throughout the development lifecycle and are actively maintained and updated when new GX Cloud or GX OSS versions are released.

- Community-supported - integrations are implemented by the community or GX. GX is not responsible for ensuring integration reliability or compatibility.

## GX-supported

The following are the levels of support offered by GX for integrated applications, operating systems, and programming languages.

### Operating systems

The following table defines the operating systems supported by GX Cloud and GX OSS.

| GX Cloud    | GX OSS    |
| ----------- | --------- |
| Mac/Linux ¹ | Mac/Linux |

¹ Required to run the GX Agent.

### Python versions

The following table defines the Python versions supported by GX Cloud and GX OSS. GX typically follows the [Python release cycle](https://devguide.python.org/versions/).

| GX Cloud | GX OSS      |
| -------- | ----------- |
| N/A      | 3.8 to 3.11 |

### GX versions

The following table defines the GX versions supported by GX Cloud and GX OSS.

| GX Cloud | GX OSS        |
| -------- | ------------- |
| N/A      | 0.17<br/>0.18 |

### Integrations

The following table defines the supported GX Cloud and GX OSS integrations.

| Integration Type                 | GX Cloud                              | GX OSS                                                                        |
|----------------------------------|---------------------------------------|-------------------------------------------------------------------------------|
| Data Sources<sup>1</sup>         | Snowflake<br/> PostgreSQL<sup>2</sup> | Snowflake<br/>PostgreSQL<br/>Sqlite<br/>Databricks (SQL)<br/>Spark<br/>Pandas |
| Configuration Stores<sup>3</sup> | In-app                                | File system                                                                   |
| Actions                          | Slack                                 | Slack <br/>email<br/>Microsoft Teams<br/>PagerDuty                            |
| Credential Store                 | Environment variables                 | Environment variables <br/> YAML<sup>4</sup>                                  |
| Orchestrator                     | Airflow <sup>5</sup><sup>6</sup>      | Airflow <sup>5</sup><sup>6</sup>                                              |

<sup>1</sup> We've also seen GX work with the following data sources in the past but we can't guarentee ongoing compatibility. These data sources include Clickhouse, Vertica, Dremio, Teradata, Athena, EMR Spark, AWS Glue, Microsoft Fabric, Trino, Pandas on (S3, GCS, Azure), Databricks (Spark), and Spark on (S3, GCS, Azure).<br/>
<sup>2</sup> Support for BigQuery in GX Cloud will be available in a future release.<br/>
<sup>3</sup> This includes configuration storage for Expectations, Checkpoints, Validation Definitions, and Validation Result<br/>
<sup>4</sup> config_variables.yml<br/>
<sup>5</sup> Although only Airflow is supported, GX Cloud and GX Core should work with any orchestrator that executes Python code.<br/>
<sup>6</sup> Airflow version 2.9.0+ required<br/>

### GX components

The following table defines the GX components supported by GX Cloud and GX OSS.

| Component    | GX Cloud                                                                                         | GX OSS                                                                  |
| ------------ | ------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------- |
| Expectations | See [Available Expectations](/cloud/expectations/manage_expectations.md#available-expectations). | See [Create Expectations](/oss/guides/expectations/expectations_lp.md). |
| GX Agent     | All versions                                                                                     | N/A                                                                     |

## Community-supported

The following integrated applications, operating systems, and programming languages are supported by the community.

### Operating systems

The following table lists the operating systems supported by the community.

| GX Cloud | GX OSS   |
| -------- | -------- |
| N/A      | Windows <sup>1</sup> |

<sup>1</sup> Untested and unsupported by GX.

### Integrations

The following table lists the GX Cloud and GX OSS integrations supported by the community.

| Integration Type | GX Cloud | GX OSS                                                                                                                                                                                                           |
| ---------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Data Sources     | N/A      | Pandas<br/>Spark<br/>Databricks (Spark)<br/>Databricks (SQL)<br/>Trino<br/>Clickhouse<br/>Dremio<br/> Teradata<br/>Vertica<br/>EMR Spark<br/>AWS Glue<br/>Google Cloud Storage<br/>Azure Blog Storage<br/>AWS S3 |
| Notifications    | N/A      | Opsgenie<br/>Amazon SNS<br/>DataHub                                                                                                                                                                              |
| Orchestrators    | N/A      | Prefect<br/>Dagster <br/>Flyte <br/>mage.ai                                                                                                                                                                      |
