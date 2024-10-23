---
title: "Configure project settings"
description: Learn how to configure advanced project settings such as the locations where GX saves metadata, secure credentials, access to secrets managers, the content and location of Data Docs sites, and internal analytics event triggering.
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
      label="Metadata Stores"
      description="Specify the locations at which GX stores information such as Expectation Suite configurations, Checkpoint Configurations, and Validation Results."
      to="/core/configure_project_settings/configure_metadata_stores" 
      icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
      topIcon 
      label="Data Docs"
      description="Configure the hosting locations and contents of Data Docs sites." 
      to="/core/configure_project_settings/configure_data_docs"
      icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
      topIcon 
      label="Credentials"
      description="Securely store and access credentials for database connection strings, org IDs for connecting to a GX Cloud account, and tokens for Checkpoint Action API endpoints and webhooks."
      to="/core/configure_project_settings/configure_credentials" 
      icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
      topIcon 
      label="Secrets Managers"
      description="Securely access and retrieve credentials from the AWS Secrets Manager, Google Cloud Secret Manager, or Azure Key Vault secrets managers."
      to="/core/configure_project_settings/access_secrets_managers" 
      icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
      topIcon 
      label="Analytics Events"
      description="Toggle the GX's collection of analytics events."
      to="/core/configure_project_settings/toggle_analytics_events" 
      icon="/img/expectation_icon.svg" 
  />
</LinkCardGrid>