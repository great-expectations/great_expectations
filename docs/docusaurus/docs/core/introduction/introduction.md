---
title: Introduction to Great Expectations
description: Learn about the key features of GX, how to connect with the GX community, and try GX in Python.
hide_feedback_survey: true
hide_title: true
---
import GxData from '../_core_components/_data.jsx'

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Learn about the key features of Great Expectations (GX).  Connect with the GX community, and try GX in Python using provided sample data.
</OverviewCard>

<LinkCardGrid>

  <LinkCard 
    topIcon 
    label="About GX"
    description="Learn about the key features of GX."
    to="/core/introduction/about_gx" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
      topIcon 
      label="Community resources"
      description="Learn how to connect with the GX community, where to ask questions about GX, and how to contribute to the GX open source code and documentation."
      to="/core/introduction/community_resources" 
      icon="/img/expectation_icon.svg"
  />

  <LinkCard 
    topIcon 
    label="Try GX"
    description="Set up a local GX deployment and give it a test run using sample data."
    to="/core/introduction/try_gx" 
    icon="/img/expectation_icon.svg" 
  />

</LinkCardGrid>

## Manage data objects

Retrieve, view, and delete data objects, or use them to retrieve data for use within GX.

<LinkCardGrid>

  <LinkCard 
    topIcon 
    label="Manage Data Sources"
    description="Manage the objects that connect GX to your data."
    to="/core/manage_and_access_data/manage_data_sources" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Manage Data Assets"
    description="Manage the objects that tell GX which sets of records are relevant to your use cases."
    to="/core/manage_and_access_data/manage_data_assets" 
    icon="/img/expectation_icon.svg" 
  />

<LinkCard 
    topIcon 
    label="Manage Batch Requests"
    description="Manage the objects that retrieve data from a Data Asset."
    to="/core/manage_and_access_data/manage_batch_requests" 
    icon="/img/expectation_icon.svg" 
  />

<LinkCard 
    topIcon 
    label="Manage Batches"
    description="Manage the objects that represent your retrieved data."
    to="/core/manage_and_access_data/manage_batches" 
    icon="/img/expectation_icon.svg" 
  />

</LinkCardGrid>