---
title: 'Install and manage GX Core'
hide_feedback_survey: true
hide_title: true
toc_min_heading_level: 3
toc_max_heading_level: 3
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

import GxData from '../../components/_data.jsx';


<OverviewCard title={frontMatter.title}>

  To use GX Core, you'll install Python, the GX Python library, and additional dependencies for your deployment environment or your preferred Data Source.

</OverviewCard>

## Install GX Core and dependencies

After you install GX Core, you might need to install additional Python libraries or third party utilities for your deployment environment or your preferred Data Sources.

<LinkCardGrid>
  <LinkCard 
    topIcon 
    label="Install GX Core"
    description="Install Python and the GX Core Python library."
    to="/core/installation_and_setup/install_gx" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Amazon S3"
    description="Install and set up support for Amazon S3 and GX"
    to="/core/installation_and_setup/additional_dependencies?dependencies=amazon" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="Azure Blob Storage"
    description="Install and set up support for Azure Blob Storage and GX"
    to="/core/installation_and_setup/additional_dependencies?dependencies=azure" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="Google Cloud Storage"
    description="Install and set up support for Google Cloud Storage and GX"
    to="/core/installation_and_setup/additional_dependencies?dependencies=gcs" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="SQL databases"
    description="Install and set up support for SQL Data Sources and GX"
    to="/core/installation_and_setup/additional_dependencies?dependencies=sql" 
    icon="/img/expectation_icon.svg" 
  />
</LinkCardGrid>

## Manage a GX Core project

A Data Context is your entry point to managing a Great Expectations (GX) project. It tells GX where to store metadata such as your configurations for Data Sources, Expectation Suites, Checkpoints, and Data Docs. It contains your Validation Results and the metrics associated with them. The Data Context also provides access to those objects in Python, along with other helper functions for the GX Python API.

<LinkCardGrid>

  <LinkCard 
    topIcon 
    label="Manage Data Contexts"
    description="Create, retrieve, and manage Data Contexts (your entry point to the GX API) in a Python script."
    to="/core/installation_and_setup/manage_data_contexts" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Manage Credentials"
    description="Securely store and access the credentials needed for certain environments and Data Sources."
    to="/core/installation_and_setup/manage_credentials" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Manage Stores"
    description="Manage the locations that GX stores configuration information"
    to="/core/installation_and_setup/manage_metadata_stores" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Manage Data Docs"
    description="Host and share human readable documentation about your Expectations and Validation Results."
    to="/core/installation_and_setup/manage_data_docs" 
    icon="/img/expectation_icon.svg" 
  />

</LinkCardGrid>
