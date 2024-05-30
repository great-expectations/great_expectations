import GxData from '../../_core_components/_data.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import ConnectionString from './_connection_string.md';
import EnvironmentVariables from './_environment_variables.md';
import ConfigYml from './_config_yml.md';
import AwsSecretsManager from './_aws_secrets_manager.md';
import GcpSecretManager from './_gcp_secret_manager.md';
import AzureKeyVault from './_azure_key_vault.md';
import AccessCredentials from './_access_credentials.md'



To connect GX to your SQL data, you will first need your connection string and corresponding credentials. Because your connection string and credentials provide access to your data they should be stored securely outside of version control.  {GxData.product_name} allows you to securely store credentials and connection strings as environment variables or in an uncommitted config file.  These variables are then accessed through string substitution in your version controlled code.

### Prerequisites

- The ability to set environment variables or a File Data Context.

{GxData.product_name} also supports referencing credentials that have been stored in the AWS Secrets Manager, Google Cloud Secret Manager, and Azure Key Vault secrets managers.  To set up {GxData.product_name} to access one of these secrets managers you will additionally require:

- The ability to install Python modules with `pip`.

### Procedure

1. Determine your connection string format.

   <ConnectionString/>

2. Store the credentials required for your connection string.

   GX supports the following methods of securely storing credentials.  Chose one to implement for your connection string:

   <Tabs queryString="storage_type" groupId="storage_type" defaultValue='environment_variables' values={[{label: 'Environment Variables', value:'environment_variables'}, {label: 'config.yml', value:'config_yml'}, {label: 'Secret Manager', value:'secret_manager'}]}>

   <TabItem value="environment_variables">
      <EnvironmentVariables/>
   </TabItem>

   <TabItem value="config_yml">
      <ConfigYml/>
   </TabItem>

   <TabItem value="secret_manager">

      {GxData.product_name} supports the AWS Secrets Manager, Google Cloud Secret Manager, and Azure Key Vault secrets managers.

      <Tabs queryString="manager_type" groupId="manager_type" defaultValue='aws' values={[{label: 'AWS Secrets Manager', value:'aws'}, {label: 'GCP Secret Manager', value:'gcp'}, {label: 'Azure Key Vault', value:'azure'}]}>
      
         <TabItem value="aws">
            <AwsSecretsManager/>
         </TabItem>

         <TabItem value="gcp">
            <GcpSecretManager/>
         </TabItem>

         <TabItem value="azure">
            <AzureKeyVault/>
         </TabItem>

      </Tabs>

   </TabItem>

   </Tabs>

3. Access your credentials in Python strings.

   <AccessCredentials/>
