---
title: Configure credentials, tokens, and connection strings
description: Securely store and retrieve credentials using string substitution and Environment Variables or an uncommitted credentials file.
hide_feedback_survey: false
hide_title: false
---

import GxData from '../../_core_components/_data.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import EnvironmentVariables from './_environment_variables.md';
import ConfigYml from './_config_yml.md';
import AccessCredentials from './_access_credentials.md'



Credentials, whether they are tokens for accessing third party apps such as Slack or connection strings for accessing your data, should be stored securely outside of version control.  GX Core allows you to securely store credentials of all types as environment variables on a local system, or as entries in an uncommitted config file.  These credentials are then referenced by variable name in your version controlled code, and implemented by GX through string substitution.

### Prerequisites

- The ability to set environment variables or a File Data Context.

GX Core also supports referencing credentials that have been stored in the AWS Secrets Manager, Google Cloud Secret Manager, and Azure Key Vault secrets managers.  To set up GX Core to access one of these secrets managers you will additionally require:

- The ability to install Python modules with `pip`.

### Procedure

1. Assign the credentials to a reference variable.

   GX supports the following methods of securely storing credentials.  Chose one to implement for your connection string:

   <Tabs queryString="storage_type" groupId="storage_type" defaultValue='environment_variables' values={[{label: 'Environment Variables', value:'environment_variables'}, {label: 'config.yml', value:'config_yml'}]}>

   <TabItem value="environment_variables">
      <EnvironmentVariables/>
   </TabItem>

   <TabItem value="config_yml">
      <ConfigYml/>
   </TabItem>

   </Tabs>

2. Access your credentials in Python strings.

   <AccessCredentials/>
