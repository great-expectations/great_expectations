---
title: 'Installation and setup'
hide_feedback_survey: true
hide_title: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

import GxData from '../../components/_data.jsx';

import AdditionalDependencies from './additional_dependencies/additional_dependencies.md'


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
    description="Install the GX Python library"
    to="/core/installation_and_setup/install_gx" 
    icon="/img/expectation_icon.svg" 
  />

</LinkCardGrid>

## Install additional dependencies

<AdditionalDependencies/>

## Manage a GX project

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
