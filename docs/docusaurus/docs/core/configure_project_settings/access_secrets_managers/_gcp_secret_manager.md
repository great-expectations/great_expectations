import GxData from '../../_core_components/_data.jsx'
import PreReqFileDataContext from '../../_core_components/prerequisites/_file_data_context.md'

### Prerequisites

- A [GCP Secret Manager instance with configured secrets](https://cloud.google.com/secret-manager/docs/quickstart).
- The ability to install Python packages with `pip`.
- <PreReqFileDataContext/>.

### Procedure

1. Set up GCP Secret Manager support.
   
   To use GCP Secret Manager with GX Core you will first need to install the `great_expectations` Python package with the `gcp` requirement.  To do this, run the following command:

   ```bash title="Terminal"
   pip install 'great_expectations[gcp]'
   ```

2. Reference GCP Secret Manager variables in `config_variables.yml`.

   By default, `config_variables.yml` is located at: 'gx/uncomitted/config_variables.yml' in your File Data Context.

   Values in `config_variables.yml` that match the regex `^secret\|projects\/[a-z0-9\_\-]{6,30}\/secrets` will be substituted with corresponding values from GCP Secret Manager.  However, if the keywords in the matching regex do not correspond to keywords in GCP Secret Manager no substitution will occur.

   You can reference other stored credentials within the regex by wrapping their corresponding variable in `${` and `}`.  When multiple references are present in a value, the secrets manager substitution takes place after all other substitutions have occurred.

   An entire connection string can be referenced from the secrets manager:

   ```yaml title="config_variables.yml"
    my_gcp_creds: secret|projects/${PROJECT_ID}/secrets/dev_db_credentials|connection_string
   ```

   Or each component of the connection string can be referenced separately:
   
   ```yaml title="config_variables.yml"
    drivername: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_DRIVERNAME
    host: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_HOST
    port: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_PORT
    username: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_USERNAME
    password: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_PASSWORD
    database: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_DATABASE
    ```
   
3. Optional. Reference versioned secrets.

   Unless otherwise specified, the latest version of the secret is returned by default. To get a specific version of the secret you want to retrieve, specify its version id. For example:

   ```yaml title="config_variables.yml"
   versioned_secret: secret|projects/${PROJECT_ID}/secrets/my_secret/versions/1
   ```

4. Optional. Retrieve specific secrets for a JSON string.
 
   To retrieve a specific secret for a JSON string, include the JSON key after a pipe character `|` at the end of the secrets regex.  For example:

   ```yaml title="config_variables.yml"
   json_secret: secret|projects/${PROJECT_ID}/secrets/my_secret|<KEY>
   versioned_json_secret: secret|projects/${PROJECT_ID}/secrets/my_secret/versions/1|<KEY>
   ``` 






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