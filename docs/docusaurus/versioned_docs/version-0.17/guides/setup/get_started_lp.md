---
sidebar_label: 'Get started with GX'
title: 'Get started with Great Expectations'
id: get_started_lp
description: Install Great Expectations and initialize your deployment.
hide_feedback_survey: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import VersionedLink from '@site/src/components/VersionedLink'

<p class="DocItem__header-description">Start here if you're unfamiliar with Great Expectations (GX), or you want to use GX with Databricks or a SQL Data Source in a production environment. To install and configure GX in your specific production environment, see <VersionedLink to='/guides/setup/setup_overview_lp'>Set up your Great Expectations environment</VersionedLink>. </p>



<LinkCardGrid>
  <LinkCard topIcon label="Quickstart" description="Install GX, connect to sample data, build your first Expectation, validate data, and review the validation results" to="/tutorials/quickstart/" icon="/img/test_icon.svg" />
  <LinkCard topIcon label="GX overview" description="An overview of GX for new users and those looking for an understanding of its components and its primary workflows" to="/conceptual_guides/gx_overview" icon="/img/overview_icon.svg" />
  <LinkCard topIcon label="Get started with GX and Databricks" description="Learn how you can use GX with Databricks in a production environment" to="/tutorials/getting_started/how_to_use_great_expectations_in_databricks" icon="/img/databricks_icon.svg" />
  <LinkCard topIcon label="Get Started with GX and SQL" description="Learn how you can use GX with a SQL Data Source in a production environment" to="/tutorials/getting_started/how_to_use_great_expectations_with_sql" icon="/img/sql_icon.svg" />
</LinkCardGrid>