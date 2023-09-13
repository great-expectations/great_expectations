---
title: Application support policy
---

The following are the levels of support offered by Great Expectations (GX) for integrated applications, operating systems, and programming languages:

<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css" crossorigin="anonymous" referrerpolicy="no-referrer" />
<div>
    <ul style={{
        "list-style-type": "none"
    }}>
        <li><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> &nbsp; Fully tested and supported by GX</li>
        <li><i class="fas fa-circle" style={{color: "#ffc107"}}></i> &nbsp; Partially tested and supported by GX</li>
        <li><i class="fas fa-circle" style={{color: "#dc3545"}}></i> &nbsp; Untested and unsupported by GX </li>       
    </ul>
</div>

These classifications allow GX to better prioritize its response to application integration, operating system, and programming language issues. Formally defining the support policy also provides contributors with a better understanding of who is responsible for resolving application integration, operating system, and programming language issues.

## Support categories

The following are the types of support available for application integration, operating system, and programming language issues:

- **GX supported** - The GX Developer Relations (DevRel) team triages, reproduces, and then assigns issues to the appropriate GX Engineering team. Issues with application integration, operating system, and programming languages in maintenance mode are reviewed, but new features or functionality are not implemented as part of the resolution.

- **Community supported** - The GX DevRel team triages and reproduces issues, and then determines if GX should implement new features or functionality to resolve the issue. If the DevRel team determines that new features or functionality are not warranted, the GX community is responsible for issue resolution. If new features or functionality are required, DevRel assigns the issue to the appropriate GX Engineering team.

## GX versions

GX supports the current and two previous releases of GX. The following table defines who is responsible for providing GX version support.


| GX supported                             | Community supported                                                |
|------------------------------------------|--------------------------------------------------------------------|
| <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Current minor release. For example, 0.17.X    | <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Previous two minor releases. For example, 0.15.X, 0.16.X <br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> Three minor releases. For example, 0.14.X              | 

## Operating systems

The following table defines the operating systems supported by GX and the community.

| GX supported                             | Community supported                                                |
|------------------------------------------|--------------------------------------------------------------------|
| <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> macOS<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Linux    | <i class="fas fa-circle" style={{color: "#dc3545"}}></i> Windows              | 

## Python versions

The following table defines the Python versions supported by GX and the community. GX typically follows the [Python release cycle](https://devguide.python.org/versions/).

| GX supported                             | Community supported                                                |
|------------------------------------------|--------------------------------------------------------------------|
| <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> 3.8 to 3.11    | <i class="fas fa-circle" style={{color: "#dc3545"}}></i> 3.7.X and earlier              | 

## Datasources

The following table defines the Datasources supported by GX and the community.

| GX supported                             | Community supported                                                |
|------------------------------------------|--------------------------------------------------------------------|
| <i class="fas fa-circle" style={{color: "#dc3545"}}></i> Snowflake<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> Databricks<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> BigQuery<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> PostgreSQL<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> pandas (maintenance mode)<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Spark (maintenance mode)  | <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Redshift<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> MSSQL<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> MySQL<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> SQLite<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Trino<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> AWS S3<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Google Cloud Storage<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Azure Blog Storage<br/><i class="fas fa-circle" style={{color: "#ffc107"}}></i> Clickhouse<br/><i class="fas fa-circle" style={{color: "#ffc107"}}></i> Athena<br/><i class="fas fa-circle" style={{color: "#ffc107"}}></i> Dremio<br/><i class="fas fa-circle" style={{color: "#ffc107"}}></i> Teradata<br/><i class="fas fa-circle" style={{color: "#ffc107"}}></i> Vertica<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> EMR Spark<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> AWS Glue             | 

## Data stores

The following table defines the data stores supported by GX and the community.

| GX supported                             | Community supported                                                |
|------------------------------------------|--------------------------------------------------------------------|
| <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> AWS S3<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> GCS Buckets     | <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Azure Blob Store              | 

## Checkpoint integrations

The following table defines the Checkpoint integrations supported by GX and the community.

| GX supported                             | Community supported                                                |
|------------------------------------------|--------------------------------------------------------------------|
| <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Slack<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Email     | <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> MS Teams<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Pager Duty<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Opsgenie<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Amazon SNS<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> General API<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> DataHub        | 

## Credential stores

The following table defines the credential stores supported by GX and the community.

| GX supported                             | Community supported                                                |
|------------------------------------------|--------------------------------------------------------------------|
| <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> YAML<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> AWS Secrets Store<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Azure Blob Store<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> GCP Secrets Manager<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Azure Key Vault<br/><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Databricks Secrets Store<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> HashiCorp Vault    |               |

## Orchestrators

The following table defines the orchestrators supported by GX and the community.

| GX supported                             | Community supported                                                |
|------------------------------------------|--------------------------------------------------------------------|
| <i class="fas fa-circle" style={{color: "#ffc107"}}></i> Airflow     | <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Prefect<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> Dagster<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> Flyte<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> mage.ai<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> Github Action              | 

## Unsupported applications

The following table defines the applications that are not supported by GX or the community.

| Application                             |
|-----------------------------------------|
| <i class="fas fa-circle" style={{color: "#dc3545"}}></i> Acryl Datahub<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> Atlan<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> Amundsen<br/><i class="fas fa-circle" style={{color: "#ffc107"}}></i> AWS Databricks<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> Azure Databricks<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> Meltano<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> dbt<br/><i class="fas fa-circle" style={{color: "#dc3545"}}></i> Capital One Data Profile  |

