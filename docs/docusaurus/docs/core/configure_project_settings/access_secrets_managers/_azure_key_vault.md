import GxData from '../../_core_components/_data.jsx'
import PreReqFileDataContext from '../../_core_components/prerequisites/_file_data_context.md'

### Prerequisites

- An [Azure Key Vault instance with configured secrets](https://docs.microsoft.com/en-us/azure/key-vault/general/overview).
- The ability to install Python packages with `pip`.
- <PreReqFileDataContext/>.

### Procedure

1. Set up Azure Key Vault support.

   To use Azure Key Vault with GX Core you will first need to install the `great_expectations` Python package with the `azure_secrets` requirement.  To do this, run the following command:

   ```bash title="Terminal"
   pip install 'great_expectations[azure_secrets]'
   ```

2. Reference Azure Key Vault variables in `config_variables.yml`.

   By default, `config_variables.yml` is located at: 'gx/uncomitted/config_variables.yml' in your File Data Context.

   Values in `config_variables.yml` that match the regex `^secret\|https:\/\/[a-zA-Z0-9\-]{3,24}\.vault\.azure\.net` will be substituted with corresponding values from Azure Key Vault.  However, if the keywords in the matching regex do not correspond to keywords in Azure Key Vault no substitution will occur.

   You can reference other stored credentials within the regex by wrapping their corresponding variable in `${` and `}`.  When multiple references are present in a value, the secrets manager substitution takes place after all other substitutions have occurred.

   An entire connection string can be referenced from the secrets manager:

   ```yaml title="config_variables.yml"
    my_abs_creds: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|connection_string
   ```

   Or each component of the connection string can be referenced separately:
   
   ```yaml title="config_variables.yml"
    drivername: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|host
    host: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|host
    port: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|port
    username: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|username
    password: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|password
    database: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|database
    ```
   
3. Optional. Reference versioned secrets.

   Unless otherwise specified, the latest version of the secret is returned by default. To get a specific version of the secret you want to retrieve, specify its version id (32 alphanumeric characters). For example:

   ```yaml title="config_variables.yml"
   versioned_secret: secret|https://${VAULT_NAME}.vault.azure.net/secrets/my-secret/a0b00aba001aaab10b111001100a11ab
   ```

4. Optional. Retrieve specific secrets for a JSON string.
 
   To retrieve a specific secret for a JSON string, include the JSON key after a pipe character `|` at the end of the secrets regex.  For example:

   ```yaml title="config_variables.yml"
   json_secret: secret|https://${VAULT_NAME}.vault.azure.net/secrets/my-secret|<KEY>
   versioned_json_secret: secret|https://${VAULT_NAME}.vault.azure.net/secrets/my-secret/a0b00aba001aaab10b111001100a11ab|<KEY>
   ``` 


