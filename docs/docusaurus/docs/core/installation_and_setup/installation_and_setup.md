---
title: 'Installation and setup'
hide_feedback_survey: true
hide_title: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

import GxData from '../../components/_data.jsx';


<OverviewCard title={frontMatter.title}>

  Set up your deployment environment with Python, the Great Expectations (GX) Python library, and any additional dependencies or credentials you will need to work in a given deployment environment or with a particular data source format.

</OverviewCard>


## Basic installation

<p>At a minimum, using Great Expectations in Python scripts requires an installation of Python (version {GxData.min_python} to {GxData.max_python}) that includes the Great Expectations library.</p>

<LinkCardGrid>

  <LinkCard 
    topIcon 
    label="Set up a Python environment"
    description="Install Python and set up a virtual environment for your GX project."
    to="/core/installation_and_setup/set_up_a_python_environment" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Install Great Expectations"
    description="Install the GX Python library locally or in a hosted environment such as an EMR Spark cluster or Databricks cluster."
    to="/core/installation_and_setup/install_gx" 
    icon="/img/expectation_icon.svg" 
  />

</LinkCardGrid>

## Install additional dependencies

Some environments and Data Sources utilize additional Python libraries or third party utilities that are not included in the base installation of Great Expectations (GX).  If your use cases involve any of the following, follow the corresponding guidance to install the necessary dependencies.

<LinkCardGrid>
  <LinkCard 
    topIcon 
    label="Amazon S3"
    description="Install and set up support for Amazon S3 and GX"
    to="/docs/1.0-prerelease/core/installation_and_setup/additional_dependencies?dependencies=amazon" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="Azure Blob Storage"
    description="Install and set up support for Azure Blob Storage and GX"
    to="/docs/1.0-prerelease/core/installation_and_setup/additional_dependencies?dependencies=azure" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="Google Cloud Storage"
    description="Install and set up support for Google Cloud Storage and GX"
    to="/docs/1.0-prerelease/core/installation_and_setup/additional_dependencies?dependencies=gcs" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="SQL Data Sources"
    description="Install and set up support for SQL Data Sources and GX"
    to="/docs/1.0-prerelease/core/installation_and_setup/additional_dependencies?dependencies=sql" 
    icon="/img/expectation_icon.svg" 
  />
</LinkCardGrid>

## Manage a GX project

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
