---
title: Introduction to GX Core
description: Learn about the key features of GX Core, how to connect with the GX community, and try GX Core in Python.
hide_feedback_survey: true
hide_title: true
---
import GxData from '../_core_components/_data.jsx'

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Learn about the key features of Great Expectations (GX) Core.  Connect with the GX community, and try GX Core in Python using provided sample data.
</OverviewCard>

<LinkCardGrid>

  <LinkCard 
    topIcon 
    label="About GX Core"
    description="Learn about the key features of GX Core."
    to="/core/introduction/about_gx" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="GX Core overview"
    description="Learn about GX Core components and workflows."
    to="/core/introduction/gx_overview" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Try GX Core"
    description="Walk through example GX Core workflows using sample data."
    to="/core/introduction/try_gx" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
      topIcon 
      label="Community resources"
      description="Learn how to connect with the GX community, where to ask questions about GX Core, and how to contribute to the GX open source code and documentation."
      to="/core/introduction/community_resources" 
      icon="/img/expectation_icon.svg"
  />

</LinkCardGrid>