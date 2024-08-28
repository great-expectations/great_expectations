---
title: Access secrets managers
description: Access credentials that are stored in AWS Secrets Manager, GCP Secret Manager, or Azure key vault.
hide_feedback_survey: false
hide_title: false
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';
import GxData from '../../_core_components/_data.jsx'

import AwsSecretsManager from './_aws_secrets_manager.md';
import GcpSecretManager from './_gcp_secret_manager.md';
import AzureKeyVault from './_azure_key_vault.md';

GX Core supports the AWS Secrets Manager, Google Cloud Secret Manager, and Azure Key Vault secrets managers.

Use of a secrets manager is optional.  [Credentials can be securely stored as environment variables or entries in a yaml file](core/configure_project_settings/configure_credentials/configure_credentials.md) without referencing content stored in a secrets manager.

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