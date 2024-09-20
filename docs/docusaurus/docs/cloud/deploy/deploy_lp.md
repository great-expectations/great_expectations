---
sidebar_label: 'Deploy GX Cloud'
title: 'Deploy GX Cloud'
hide_title: true
description: Deploy GX Cloud in your environment.
hide_feedback_survey: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Deploy GX Cloud in your environment.
</OverviewCard>

<LinkCardGrid>
  <LinkCard topIcon label="GX Cloud deployment patterns" description="Explore GX Cloud deployment patterns that fit your organization's needs." to="/cloud/deploy/deployment_patterns" icon="/img/workflow_icon.svg"/>

  <LinkCard topIcon label="Deploy the GX Agent" description="Deploy and enable the GX Agent for agent-based deployments." to="/cloud/deploy/deploy_gx_agent" icon="/img/connect_icon.svg"/>
</LinkCardGrid>