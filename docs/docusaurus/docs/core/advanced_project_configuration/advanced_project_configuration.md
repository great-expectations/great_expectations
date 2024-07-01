---
title: 'Condigure advanced project settings'
description: Configure Data Context settings, securely store credentials, customize Data Documentation and metadata storage locations, and toggle analytics events. 
hide_feedback_survey: true
hide_title: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
    {frontMatter.description}
</OverviewCard>

<LinkCardGrid>
  <LinkCard 
    topIcon 
    label="Configure credentials"
    description="Securely store credentials for reference in Data Source connection strings and Checkpoint Actions."
    to="/core/advanced_project_configuration/configure_credentials" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Access credentials from 3rd party secret managers"
    description="Configure credential references that pull the relevant information from Azure Key Vault, AWS Secrets Manager, or GCP Secret Manager."
    to="/core/advanced_project_configuration/access_credentials_from_secret_managers" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Configure Store locations"
    description="Set custom storage locations for configurations of credentials, Data Sources, Expectation Suites, and other GX components as well as the save location for Validation Results."
    to="/core/advanced_project_configuration/configure_store_locations" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Toggle analytics events"
    description="Toggle the collection of GX analytics on or off."
    to="/core/advanced_project_configuration/toggle_analytics_events" 
    icon="/img/expectation_icon.svg" 
  />
</LinkCardGrid>
