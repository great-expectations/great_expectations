export const installOptions = [
  { value: 'install-locally', label: 'locally' },
  { value: 'install-databricks', label: 'DataBricks' }
]

export const metadataOptions = [
  { value: 'metadata-store-filesystem', label: 'filesystem' },
  { value: 'metadata-store-s3', label: 's3' },
  { value: 'metadata-store-azure', label: 'azure' },
  { value: 'metadata-store-gcs', label: 'gcs' },
  { value: 'metadata-store-database', label: 'database' }
]

export const dataDocsOptions = [
  { value: 'datadocs-filesystem', label: 'filesystem' },
  { value: 'datadocs-s3', label: 's3' },
  { value: 'datadocs-azure', label: 'azure' },
  { value: 'datadocs-gcs', label: 'gcs' }
]

export const dataLocationOptions = [
  { value: 'data-location-database', label: 'database' },
  { value: 'data-location-filesystem', label: 'filesystem' },
  { value: 'data-location-s3', label: 's3' },
  { value: 'data-location-azure', label: 'azure' },
  { value: 'data-location-gcs', label: 'gcs' }
]

export const databaseOptions = [
  { value: 'compute-postgres', label: 'postgres' },
  { value: 'compute-mysql', label: 'mysql' },
  { value: 'compute-mssql', label: 'mssql' },
  { value: 'compute-bigquery', label: 'bigquery' },
  { value: 'compute-redshift', label: 'redshift' },
  { value: 'compute-snowflake', label: 'snowflake' },
  { value: 'compute-athena', label: 'athena' }
]
export const computeOptions = databaseOptions.concat(
  [
    { value: 'compute-pandas', label: 'pandas' },
    { value: 'compute-spark', label: 'spark' }
  ]
)
