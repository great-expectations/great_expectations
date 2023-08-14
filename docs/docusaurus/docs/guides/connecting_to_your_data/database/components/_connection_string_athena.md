import SqlAlchemy2 from '/docs/components/warnings/_sql_alchemy2.md'

To connect Great Expectations to Athena, you need to provide a connection string. To determine your connection string, reference the following examples and the [PyAthena documentation](https://github.com/laughingman7743/PyAthena#sqlalchemy).


<SqlAlchemy2 />

The following urls don't include credentials as it is recommended to use either the instance profile or the boto3 configuration file.

To connect Great Expectations to your Athena instance (without specifying a particular database), the URL is:

```bash
awsathena+rest://@athena.{region}.amazonaws.com/?s3_staging_dir={s3_path}
```

Note the url parameter "s3_staging_dir" needed for storing query results in S3.

To connect Great Expectations to a specific Athena database, the URL is:

```bash
awsathena+rest://@athena.{region}.amazonaws.com/{database}?s3_staging_dir={s3_path}
```

:::tip Tip: Using `credentials` instead of `connection_string`

The `credentials` key uses a dictionary to provide the elements of your connection string as separate, individual values.  For information on how to populate the `credentials` dictionary and how to configure your `great_expectations.yml` project config file to populate credentials from either a YAML file or a secret manager, see [Configure credentials](../../../setup/configuring_data_contexts/how_to_configure_credentials.md).

:::