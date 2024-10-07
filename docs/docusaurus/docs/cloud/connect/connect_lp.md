---
sidebar_label: 'Connect GX Cloud'
title: 'Connect GX Cloud'
hide_title: true
description: Connect GX Cloud to your deployment environment.
hide_feedback_survey: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  When you've finished evaluating GX Cloud, it's time to connect GX Cloud to your deployment environment.
</OverviewCard>

<LinkCardGrid>
  <LinkCard topIcon label="Connect GX Cloud to PostgreSQL" description="Quickly start using GX Cloud with PostgreSQL." to="/cloud/connect/connect_postgresql" icon="/img/postgresql_icon.svg" />
  <LinkCard topIcon label="Connect GX Cloud to Snowflake" description="Quickly start using GX Cloud with Snowflake." to="/cloud/connect/connect_snowflake" icon="/img/snowflake_icon.png" />
  <LinkCard topIcon label="Connect GX Cloud to Databricks SQL" description="Quickly start using GX Cloud with Databricks SQL." to="/cloud/connect/connect_databrickssql" icon="/img/databricks_icon.svg" />
  <LinkCard topIcon label="Connect GX Cloud and Airflow" description="Use Airflow to run scheduled GX Cloud validations." to="/cloud/connect/connect_airflow" icon="/img/airflow_icon.png" />
  <LinkCard topIcon label="Connect to GX Cloud with Python" description="Quickly start using GX Cloud with Python." to="/cloud/connect/connect_python" icon="/img/python_icon.svg" />
</LinkCardGrid>