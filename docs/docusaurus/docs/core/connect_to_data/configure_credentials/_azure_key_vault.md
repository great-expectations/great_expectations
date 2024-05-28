Configure your Great Expectations project to substitute variables from the Azure Key Vault. Secrets store substitution uses the configurations from your ``config_variables.yml`` file after all other types of substitution are applied with environment variables.

Secrets store substitution uses keywords and retrieves secrets from the secrets store for values matching the following regex ``^secret\|https:\/\/[a-zA-Z0-9\-]{3,24}\.vault\.azure\.net``. If the values you provide don't match the keywords, the values aren't substituted.

1. Run the following code to install the ``great_expectations`` package with the ``azure_secrets`` requirement:

    ```bash
    pip install 'great_expectations[azure_secrets]'
    ```

2. Provide the name of the secret you want to substitute in Azure Key Vault. For example, ``secret|https://my-vault-name.vault.azure.net/secrets/my-secret``. 

    The latest version of the secret is returned by default.

3. Optional. To get a specific version of the secret, specify its version id (32 lowercase alphanumeric characters). For example, ``secret|https://my-vault-name.vault.azure.net/secrets/my-secret/a0b00aba001aaab10b111001100a11ab``.

4. Optional. To retrieve a specific secret value for a JSON string, use ``secret|https://my-vault-name.vault.azure.net/secrets/my-secret|key`` or ``secret|https://my-vault-name.vault.azure.net/secrets/my-secret/a0b00aba001aaab10b111001100a11ab|key``.

5. Save your access credentials or the database connection string to ``great_expectations/uncommitted/config_variables.yml``. For example:

    ```yaml
    # We can configure a single connection string
    my_abs_creds: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|connection_string

    # Or each component of the connection string separately
    drivername: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|host
    host: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|host
    port: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|port
    username: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|username
    password: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|password
    database: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|database
    ```

6. Run the following code to use the `connection_string` parameter values when you add a `datasource` to a Data Context:

    ```python 
    # We can use a single connection string
    pg_datasource = context.data_sources.add_or_update_sql(
        name="my_postgres_db", connection_string="${my_azure_creds}"
    )

    # Or each component of the connection string separately
    pg_datasource = context.data_sources.add_or_update_sql(
        name="my_postgres_db", connection_string="${drivername}://${username}:${password}@${host}:${port}/${database}"
    )
    ```