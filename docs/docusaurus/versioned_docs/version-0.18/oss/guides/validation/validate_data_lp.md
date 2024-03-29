---
sidebar_label: 'Validate Data'
title: 'Validate Data'
hide_title: true
id: validate_data_lp
description: Validate Data, save Validation Results, run Actions, and create Data Docs.
hide_feedback_survey: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Validate Data, save Validation Results, run Actions, and create Data Docs.
</OverviewCard>

<LinkCardGrid>
  <LinkCard topIcon label="Data Validation workflow" description="Learn more about the GX Data Validation process" to="/oss/guides/validation/validate_data_overview" icon="/img/workflow_icon.svg" />
  <LinkCard topIcon label="Manage Checkpoints" description="Add validation data, create and configure Checkpoints, and pass in-memory DataFrames" to="/oss/guides/validation/checkpoints/checkpoint_lp" icon="/img/checkpoint_icon.svg" />
  <LinkCard topIcon label="Configure Actions" description="Configure Actions to send Validation Result notifications, update Data Docs, and store Validation Results" to="/oss/guides/validation/validation_actions/actions_lp" icon="/img/configure_icon.svg" />
  <LinkCard topIcon label="Limit validation results in Data Docs" description="Limit validation results to improve Data Doc updating and rendering performance" to="/oss/guides/validation/limit_validation_results" icon="/img/configure_icon.svg" />
</LinkCardGrid>