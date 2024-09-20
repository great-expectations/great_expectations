---
title: "GX Cloud"
hide_title: true
description: Get started with GX Cloud and learn more about GX Cloud features and functionality.
hide_table_of_contents: true
pagination_next: null
pagination_prev: null
slug: "/cloud/"
displayed_sidebar: gx_cloud
hide_feedback_survey: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Test and validate your Data Assets with our web-based UI.
</OverviewCard>

### Get started

<LinkCardGrid>
  <LinkCard topIcon label="GX Cloud overview" description="Learn more about GX Cloud features and functionality." to="/cloud/overview/gx_cloud_overview" icon="/img/small_gx_logo.png" />

  <LinkCard topIcon label="Deploy GX Cloud" description="Learn how to deploy GX Cloud in your environment." to="/cloud/deploy/deploy_lp" icon="/img/small_gx_logo.png" />

  <LinkCard topIcon label="Connect GX Cloud" description="Ready to integrate GX Cloud with your production environment? Connect GX Cloud to popular data platforms and orchestration tools." to="/cloud/connect/connect_lp" icon="/img/small_gx_logo.png" />
</LinkCardGrid>

### Manage

<LinkCardGrid>
  <LinkCard topIcon label="Manage Data Assets" description="Create, edit, or delete a Data Asset." to="/cloud/data_assets/manage_data_assets" icon="/img/small_gx_logo.png" />
  <LinkCard topIcon label="Manage Expectations" description="Create, edit, or delete an Expectation." to="/cloud/expectations/manage_expectations" icon="/img/small_gx_logo.png" />
  <LinkCard topIcon label="Manage Expectation Suites" description="Create or delete Expectation Suites." to="/cloud/expectation_suites/manage_expectation_suites" icon="/img/small_gx_logo.png" />
  <LinkCard topIcon label="Manage Validations" description="Run a Validation, or view the Validation run history." to="/cloud/validations/manage_validations" icon="/img/small_gx_logo.png" />
  <LinkCard topIcon label="Manage alerts" description="Add, edit, or delete alerts." to="/cloud/alerts/manage_alerts" icon="/img/small_gx_logo.png" />
  <LinkCard topIcon label="Manage users and access tokens" description="Manage GX Cloud users and access tokens." to="/cloud/users/manage_users" icon="/img/small_gx_logo.png" />
</LinkCardGrid>
