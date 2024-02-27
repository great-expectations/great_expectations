---
title: Manage credentials
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import InProgress from '../_core_components/_in_progress.md'

Many environments and data storage systems require the provision of credentials to access.  It is important for these credentials to be stored in a secure way, outside of version control.  Great Expectations (GX) offers two ways to store and access secrets out of the box: in a YAML file that exists outside of version control, or as environment variables.  GX also supports third party secrets managers for Amazon Web Services, Google Cloud Platform, and Microsoft Azure. 

<Tabs 
  queryString="credential-style" 
  groupId="credentials" 
  defaultValue='yaml' 
  values={
   [
    {label: 'Environment variables', value:'environment_variables'},
    {label: 'YAML file', value:'yaml'},
    {label: 'Secrets manager', value:'secrets_manager'},
   ]
  }>

  <TabItem value="yaml" label="YAML file">
  </TabItem>

  <TabItem value="environment_variables" label="Environment variables">
  </TabItem>

  <TabItem value="secrets_manager" label="Secrets manager">

Select one of the following secret manager applications:

  <Tabs queryString="manager" groupId="manager" defaultValue='aws' values={[{label: 'AWS Secrets Manager', value:'aws'}, {label: 'GCP Secret Manager', value:'gcp'}, {label: 'Azure Key Vault', value:'azure'}]}>

  <TabItem value="aws">
  </TabItem>

  <TabItem value="gcp">
  </TabItem>

  <TabItem value="azure">
  </TabItem>

  </Tabs>

  </TabItem>

</Tabs>

## Prerequisites

<Tabs 
  queryString="credential-style" 
  groupId="credentials" 
  defaultValue='yaml' 
  values={
   [
    {label: 'Environment variables', value:'environment_variables'},
    {label: 'YAML file', value:'yaml'},
    {label: 'Secrets manager', value:'secrets_manager'},
   ]
  }>

  <TabItem value="yaml" label="YAML file">

- An existing File Data Context.  To create a new File Data Context, see [Initialize a new Data Context](/core/installation_and_setup/manage_data_contexts.md?context-type=file#initialize-a-new-data-context).


  </TabItem>

  <TabItem value="environment_variables" label="Environment variables">

- The ability to set environment variables.


  </TabItem>

  <TabItem value="secrets_manager" label="Secrets manager">

  <Tabs queryString="manager" groupId="manager" defaultValue='aws' values={[{label: 'AWS Secrets Manager', value:'aws'}, {label: 'GCP Secret Manager', value:'gcp'}, {label: 'Azure Key Vault', value:'azure'}]}>

  <TabItem value="aws">
<InProgress/>
  </TabItem>

  <TabItem value="gcp">
<InProgress/>
  </TabItem>

  <TabItem value="azure">
<InProgress/>
  </TabItem>

  </Tabs>

  </TabItem>

</Tabs>

## Define credentials

<Tabs 
  queryString="credential-style" 
  groupId="credentials" 
  defaultValue='yaml' 
  values={
   [
    {label: 'Environment variables', value:'environment_variables'},
    {label: 'YAML file', value:'yaml'},
    {label: 'Secrets manager', value:'secrets_manager'},
   ]
  }>

  <TabItem value="yaml" label="YAML file">
<InProgress/>
  </TabItem>

  <TabItem value="environment_variables" label="Environment variables">
<InProgress/>
  </TabItem>

  <TabItem value="secrets_manager" label="Secrets manager">

  <Tabs queryString="manager" groupId="manager" defaultValue='aws' values={[{label: 'AWS Secrets Manager', value:'aws'}, {label: 'GCP Secret Manager', value:'gcp'}, {label: 'Azure Key Vault', value:'azure'}]}>

  <TabItem value="aws">
<InProgress/>
  </TabItem>

  <TabItem value="gcp">
<InProgress/>
  </TabItem>

  <TabItem value="azure">
<InProgress/>
  </TabItem>

  </Tabs>

  </TabItem>

</Tabs>

## Next steps

- [(Optional) Configure Stores](./manage_metadata_stores.md)
- [(Optional) Configure Data Docs](./manage_metadata_stores.md)
- [Connect to data](../connect_to_data/connect_to_data)