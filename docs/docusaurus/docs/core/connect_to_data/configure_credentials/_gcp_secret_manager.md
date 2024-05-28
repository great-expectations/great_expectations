Configure your Great Expectations project to substitute variables from the Google Cloud Secret Manager. Secrets store substitution uses the configurations from your ``config_variables.yml`` file after all other types of substitution are applied with environment variables.

Secrets store substitution uses keywords and retrieves secrets from the secrets store for values matching the following regex ``^secret\|projects\/[a-z0-9\_\-]{6,30}\/secrets``. If the values you provide don't match the keywords, the values aren't substituted.

1. Run the following code to install the ``great_expectations`` package with the ``gcp`` requirement:

    ```bash
    pip install 'great_expectations[gcp]'
    ```

2. Provide the name of the secret you want to substitute in GCP Secret Manager. For example, ``secret|projects/project_id/secrets/my_secret``. 

    The latest version of the secret is returned by default.

3. Optional. To get a specific version of the secret, specify its version id. For example, ``secret|projects/project_id/secrets/my_secret/versions/1``.

4. Optional. To retrieve a specific secret value for a JSON string, use ``secret|projects/project_id/secrets/my_secret|key`` or ``secret|projects/project_id/secrets/my_secret/versions/1|key``.

5. Save your access credentials or the database connection string to ``great_expectations/uncommitted/config_variables.yml``. For example:

    ```yaml
    # We can configure a single connection string
    my_gcp_creds: secret|projects/${PROJECT_ID}/secrets/dev_db_credentials|connection_string

    # Or each component of the connection string separately
    drivername: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_DRIVERNAME
    host: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_HOST
    port: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_PORT
    username: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_USERNAME
    password: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_PASSWORD
    database: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_DATABASE
    ```

6. Run the following code to use the `connection_string` parameter values when you add a `datasource` to a Data Context:

    ```python 
    # We can use a single connection string 
    pg_datasource = context.data_sources.add_or_update_sql(
        name="my_postgres_db", connection_string="${my_gcp_creds}"
    )

    # Or each component of the connection string separately
    pg_datasource = context.data_sources.add_or_update_sql(
        name="my_postgres_db", connection_string="${drivername}://${username}:${password}@${host}:${port}/${database}"
    )
    ```