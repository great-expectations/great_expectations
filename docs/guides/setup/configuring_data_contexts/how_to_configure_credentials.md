---
title: How to configure credentials
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'
import TechnicalTag from '/docs/term_tags/_tag.mdx';

This guide will explain how to configure your ``great_expectations.yml`` project config to populate credentials from either a YAML file or a secret manager.

If your Great Expectations deployment is in an environment without a file system, refer to [How to instantiate a Data Context without a yml file](./how_to_instantiate_a_data_context_without_a_yml_file.md) for credential configuration examples.

<Tabs
  groupId="yaml-or-secret-manager"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Secret Manager', value:'secret-manager'},
  ]}>

<TabItem value="yaml">

<Prerequisites></Prerequisites>

## Steps

### 1. Decide where you would like to save the desired credentials or config values - in a YAML file, environment variables, or a combination - then save the values.

In most cases, we suggest using a config variables YAML file. YAML files make variables more visible, easily editable, and allow for modularization (e.g. one file for dev, another for prod).

:::note

  - In the ``great_expectations.yml`` config file, environment variables take precedence over variables defined in a config variables YAML
  - Environment variable substitution is supported in both the ``great_expectations.yml`` and config variables ``config_variables.yml`` config file.

:::

If using a YAML file, save desired credentials or config values to ``great_expectations/uncommitted/config_variables.yml`` or another YAML file of your choosing:

```yaml file=../../../../tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py#L9-L15
```

:::note

  - If you wish to store values that include the dollar sign character ``$``, please escape them using a backslash ``\`` so substitution is not attempted. For example in the above example for Postgres credentials you could set ``password: pa\$sword`` if your password is ``pa$sword``. Say that 5 times fast, and also please choose a more secure password!
  - When you save values via the <TechnicalTag relative="../../../" tag="cli" text="CLI" />, they are automatically escaped if they contain the ``$`` character.
  - You can also have multiple substitutions for the same item, e.g. ``database_string: ${USER}:${PASSWORD}@${HOST}:${PORT}/${DATABASE}``

:::

If using environment variables, set values by entering ``export ENV_VAR_NAME=env_var_value`` in the terminal or adding the commands to your ``~/.bashrc`` file:

```bash file=../../../../tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py#L19-L25
```

### 2. If using a YAML file, set the ``config_variables_file_path`` key in your ``great_expectations.yml`` or leave the default.

```yaml file=../../../../tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py#L29
```

### 3. Replace credentials or other values in your ``great_expectations.yml`` with ``${}``-wrapped variable names (i.e. ``${ENVIRONMENT_VARIABLE}`` or ``${YAML_KEY}``).

```yaml file=../../../../tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py#L33-L59
```


## Additional Notes

- The default ``config_variables.yml`` file located at ``great_expectations/uncommitted/config_variables.yml`` applies to deployments created using ``great_expectations init``.
- To view the full script used in this page, see it on GitHub: [how_to_configure_credentials.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py)


</TabItem>
<TabItem value="secret-manager">

Choose which secret manager you are using:
<Tabs
  groupId="secret-manager"
  defaultValue='aws'
  values={[
  {label: 'AWS Secrets Manager', value:'aws'},
  {label: 'GCP Secret Manager', value:'gcp'},
  {label: 'Azure Key Vault', value:'azure'},
  ]}>

<TabItem value="aws">

This guide will explain how to configure your ``great_expectations.yml`` project config to substitute variables from AWS Secrets Manager.

<Prerequisites>

- Configured a secret manager and secrets in the cloud with [AWS Secrets Manager](https://docs.aws.amazon.com/secretsmanager/latest/userguide/tutorials_basic.html)

</Prerequisites>

:::warning

Secrets store substitution uses the configurations from your ``great_expectations.yml`` project config **after** all other types of substitution are applied (from environment variables or from the ``config_variables.yml`` config file)

The secrets store substitution works based on keywords. It tries to retrieve secrets from the secrets store for the following values :

- AWS: values starting with ``secret|arn:aws:secretsmanager`` if the values you provide don't match with the keywords above, the values won't be substituted.

:::

**Setup**

To use AWS Secrets Manager, you may need to install the ``great_expectations`` package with its ``aws_secrets`` extra requirement:

```bash
pip install great_expectations[aws_secrets]
```

In order to substitute your value by a secret in AWS Secrets Manager, you need to provide an arn of the secret like this one:
``secret|arn:aws:secretsmanager:123456789012:secret:my_secret-1zAyu6``

:::note

The last 7 characters of the arn are automatically generated by AWS and are not mandatory to retrieve the secret, thus ``secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my_secret`` will retrieve the same secret.

:::

You will get the latest version of the secret by default.

You can get a specific version of the secret you want to retrieve by specifying its version UUID like this: ``secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my_secret:00000000-0000-0000-0000-000000000000``

If your secret value is a JSON string, you can retrieve a specific value like this:
``secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my_secret|key``

Or like this:
``secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my_secret:00000000-0000-0000-0000-000000000000|key``

**Example great_expectations.yml:**

```yaml
datasources:
  dev_postgres_db:
    class_name: SqlAlchemyDatasource
    data_asset_type:
      class_name: SqlAlchemyDataset
      module_name: great_expectations.dataset
    module_name: great_expectations.datasource
    credentials:
      drivername: secret|arn:aws:secretsmanager:${AWS_REGION}:${ACCOUNT_ID}:secret:dev_db_credentials|drivername
      host: secret|arn:aws:secretsmanager:${AWS_REGION}:${ACCOUNT_ID}:secret:dev_db_credentials|host
      port: secret|arn:aws:secretsmanager:${AWS_REGION}:${ACCOUNT_ID}:secret:dev_db_credentials|port
      username: secret|arn:aws:secretsmanager:${AWS_REGION}:${ACCOUNT_ID}:secret:dev_db_credentials|username
      password: secret|arn:aws:secretsmanager:${AWS_REGION}:${ACCOUNT_ID}:secret:dev_db_credentials|password
      database: secret|arn:aws:secretsmanager:${AWS_REGION}:${ACCOUNT_ID}:secret:dev_db_credentials|database
  prod_postgres_db:
    class_name: SqlAlchemyDatasource
    data_asset_type:
      class_name: SqlAlchemyDataset
      module_name: great_expectations.dataset
    module_name: great_expectations.datasource
    credentials:
      drivername: secret|arn:aws:secretsmanager:${AWS_REGION}:${ACCOUNT_ID}:secret:PROD_DB_CREDENTIALS_DRIVERNAME
      host: secret|arn:aws:secretsmanager:${AWS_REGION}:${ACCOUNT_ID}:secret:PROD_DB_CREDENTIALS_HOST
      port: secret|arn:aws:secretsmanager:${AWS_REGION}:${ACCOUNT_ID}:secret:PROD_DB_CREDENTIALS_PORT
      username: secret|arn:aws:secretsmanager:${AWS_REGION}:${ACCOUNT_ID}:secret:PROD_DB_CREDENTIALS_USERNAME
      password: secret|arn:aws:secretsmanager:${AWS_REGION}:${ACCOUNT_ID}:secret:PROD_DB_CREDENTIALS_PASSWORD
      database: secret|arn:aws:secretsmanager:${AWS_REGION}:${ACCOUNT_ID}:secret:PROD_DB_CREDENTIALS_DATABASE
```

</TabItem>
<TabItem value="gcp">

This guide will explain how to configure your ``great_expectations.yml`` project config to substitute variables from GCP Secrets Manager.

<Prerequisites>

- Configured a secret manager and secrets in the cloud with [GCP Secret Manager](https://cloud.google.com/secret-manager/docs/quickstart)

</Prerequisites>

:::warning

Secrets store substitution uses the configurations from your ``great_expectations.yml`` project config **after** all other types of substitution are applied (from environment variables or from the ``config_variables.yml`` config file)

The secrets store substitution works based on keywords. It tries to retrieve secrets from the secrets store for the following values :

- GCP: values matching the following regex ``^secret\|projects\/[a-z0-9\_\-]{6,30}\/secrets`` if the values you provide don't match with the keywords above, the values won't be substituted.

:::

**Setup**

To use GCP Secret Manager, you may need to install the ``great_expectations`` package with its ``gcp`` extra requirement:

```bash
pip install great_expectations[gcp]
```

In order to substitute your value by a secret in GCP Secret Manager, you need to provide a name of the secret like this one:
``secret|projects/project_id/secrets/my_secret``

You will get the latest version of the secret by default.

You can get a specific version of the secret you want to retrieve by specifying its version id like this: ``secret|projects/project_id/secrets/my_secret/versions/1``

If your secret value is a JSON string, you can retrieve a specific value like this:
``secret|projects/project_id/secrets/my_secret|key``

Or like this:
``secret|projects/project_id/secrets/my_secret/versions/1|key``

**Example great_expectations.yml:**

```yaml
datasources:
  dev_postgres_db:
    class_name: SqlAlchemyDatasource
    data_asset_type:
      class_name: SqlAlchemyDataset
      module_name: great_expectations.dataset
    module_name: great_expectations.datasource
    credentials:
      drivername: secret|projects/${PROJECT_ID}/secrets/dev_db_credentials|drivername
      host: secret|projects/${PROJECT_ID}/secrets/dev_db_credentials|host
      port: secret|projects/${PROJECT_ID}/secrets/dev_db_credentials|port
      username: secret|projects/${PROJECT_ID}/secrets/dev_db_credentials|username
      password: secret|projects/${PROJECT_ID}/secrets/dev_db_credentials|password
      database: secret|projects/${PROJECT_ID}/secrets/dev_db_credentials|database
  prod_postgres_db:
    class_name: SqlAlchemyDatasource
    data_asset_type:
      class_name: SqlAlchemyDataset
      module_name: great_expectations.dataset
    module_name: great_expectations.datasource
    credentials:
      drivername: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_DRIVERNAME
      host: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_HOST
      port: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_PORT
      username: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_USERNAME
      password: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_PASSWORD
      database: secret|projects/${PROJECT_ID}/secrets/PROD_DB_CREDENTIALS_DATABASE
```

</TabItem>
<TabItem value="azure">

This guide will explain how to configure your ``great_expectations.yml`` project config to substitute variables from Azure Key Vault.

<Prerequisites>

- [Set up a working deployment of Great Expectations](../../../tutorials/getting_started/intro.md)
- Configured a secret manager and secrets in the cloud with [Azure Key Vault](https://docs.microsoft.com/en-us/azure/key-vault/general/overview)

</Prerequisites>

:::warning

Secrets store substitution uses the configurations from your ``great_expectations.yml`` project config **after** all other types of substitution are applied (from environment variables or from the ``config_variables.yml`` config file)

The secrets store substitution works based on keywords. It tries to retrieve secrets from the secrets store for the following values :

- Azure : values matching the following regex ``^secret\|https:\/\/[a-zA-Z0-9\-]{3,24}\.vault\.azure\.net`` if the values you provide don't match with the keywords above, the values won't be substituted.

:::

**Setup**

To use Azure Key Vault, you may need to install the ``great_expectations`` package with its ``azure_secrets`` extra requirement:

```bash
pip install great_expectations[azure_secrets]
```

In order to substitute your value by a secret in Azure Key Vault, you need to provide a name of the secret like this one:
``secret|https://my-vault-name.vault.azure.net/secrets/my-secret``

You will get the latest version of the secret by default.

You can get a specific version of the secret you want to retrieve by specifying its version id (32 lowercase alphanumeric characters) like this: ``secret|https://my-vault-name.vault.azure.net/secrets/my-secret/a0b00aba001aaab10b111001100a11ab``

If your secret value is a JSON string, you can retrieve a specific value like this:
``secret|https://my-vault-name.vault.azure.net/secrets/my-secret|key``

Or like this:
``secret|https://my-vault-name.vault.azure.net/secrets/my-secret/a0b00aba001aaab10b111001100a11ab|key``


**Example great_expectations.yml:**

```yaml
datasources:
  dev_postgres_db:
    class_name: SqlAlchemyDatasource
    data_asset_type:
      class_name: SqlAlchemyDataset
      module_name: great_expectations.dataset
    module_name: great_expectations.datasource
    credentials:
      drivername: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|drivername
      host: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|host
      port: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|port
      username: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|username
      password: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|password
      database: secret|https://${VAULT_NAME}.vault.azure.net/secrets/dev_db_credentials|database
  prod_postgres_db:
    class_name: SqlAlchemyDatasource
    data_asset_type:
      class_name: SqlAlchemyDataset
      module_name: great_expectations.dataset
    module_name: great_expectations.datasource
    credentials:
      drivername: secret|https://${VAULT_NAME}.vault.azure.net/secrets/PROD_DB_CREDENTIALS_DRIVERNAME
      host: secret|https://${VAULT_NAME}.vault.azure.net/secrets/PROD_DB_CREDENTIALS_HOST
      port: secret|https://${VAULT_NAME}.vault.azure.net/secrets/PROD_DB_CREDENTIALS_PORT
      username: secret|https://${VAULT_NAME}.vault.azure.net/secrets/PROD_DB_CREDENTIALS_USERNAME
      password: secret|https://${VAULT_NAME}.vault.azure.net/secrets/PROD_DB_CREDENTIALS_PASSWORD
      database: secret|https://${VAULT_NAME}.vault.azure.net/secrets/PROD_DB_CREDENTIALS_DATABASE
```

</TabItem>
</Tabs>

</TabItem>
</Tabs>