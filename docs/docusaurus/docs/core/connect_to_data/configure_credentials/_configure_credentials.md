import GxData from '../../_core_components/_data.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import ConnectionString from './_connection_string.md';
import EnvironmentVariables from './_environment_variables.md';
import ConfigYml from './_config_yml.md';
import AccessCredentials from './_access_credentials.md'



To connect GX to your SQL data, you will need your connection string and corresponding credentials. Because your connection string and credentials provide access to your data they should be stored securely outside of version control.  GX Core allows you to securely store credentials and connection strings as environment variables or in an uncommitted config file.  These variables are then accessed through string substitution in your version controlled code.

### Prerequisites

- The ability to set environment variables or a File Data Context.

GX Core also supports referencing credentials that have been stored in the AWS Secrets Manager, Google Cloud Secret Manager, and Azure Key Vault secrets managers.  To set up GX Core to access one of these secrets managers you will additionally require:

- The ability to install Python modules with `pip`.

### Procedure

1. Determine your connection string format.

   <ConnectionString/>

2. Store the credentials required for your connection string.

   GX supports the following methods of securely storing credentials.  Chose one to implement for your connection string:

   <Tabs queryString="storage_type" groupId="storage_type" defaultValue='environment_variables' values={[{label: 'Environment Variables', value:'environment_variables'}, {label: 'config.yml', value:'config_yml'}]}>

   <TabItem value="environment_variables">
      <EnvironmentVariables/>
   </TabItem>

   <TabItem value="config_yml">
      <ConfigYml/>
   </TabItem>

   </Tabs>

3. Access your credentials in Python strings.

   <AccessCredentials/>

4. Optional. Access credentials stored in a secret manager.

   GX Core supports the AWS Secrets Manager, Google Cloud Secret Manager, and Azure Key Vault secrets managers.  For more information on how to set up string substitutions that pull credentials from these sources, see [Access secrets managers](core/configure_project_settings/access_secrets_managers/access_secrets_managers.md).