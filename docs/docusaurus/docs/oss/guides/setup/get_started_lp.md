---
sidebar_label: 'Get started with GX'
title: 'Get started with Great Expectations'
hide_title: true
id: get_started_lp
description: Install Great Expectations and initialize your deployment.
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Start here if you're unfamiliar with GX or want to use GX with Databricks or a SQL Data Source in a deployment environment.
</OverviewCard>

<LinkCardGrid>
  <LinkCard topIcon label="Quickstart" description="Install GX, connect to sample data, build your first Expectation, validate data, and review the validation results" to="/oss/tutorials/quickstart/" icon="/img/test_icon.svg" />
  <LinkCard topIcon label="GX overview" description="An overview of GX for new users and those looking for an understanding of its components and its primary workflows" to="/reference/learn/conceptual_guides/gx_overview" icon="/img/overview_icon.svg" />
  <LinkCard topIcon label="Get started with GX and Databricks" description="Learn how you can use GX with Databricks in a production environment" to="/oss/tutorials/getting_started/how_to_use_great_expectations_in_databricks" icon="/img/databricks_icon.svg" />
  <LinkCard topIcon label="Get Started with GX and SQL" description="Learn how you can use GX with a SQL Data Source in a production environment" to="/oss/tutorials/getting_started/how_to_use_great_expectations_with_sql" icon="/img/sql_icon.svg" />
</LinkCardGrid>