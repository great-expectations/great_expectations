---
sidebar_label: 'Integration tutorials'
title: 'Integration tutorials'
hide_title: true
description: Tutorials on how to integrate GX with other tools
hide_feedback_survey: true
pagination_next: null
pagination_prev: null
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Learn how to integrate GX with other tools.
</OverviewCard>

Great Expectations can be integrated with a variety of orchestrators and data pipeline tools.

<LinkCardGrid>

  <LinkCard topIcon label="Integrate GX with Airflow" description="Learn how to integrate GX with Airflow and add data validation to a pipeline." to="/reference/learn/integrations/airflow_pipeline_tutorial" icon="/img/airflow_icon.svg"/>

  <LinkCard topIcon label="Use GX with dbt" description="Learn how to use GX with dbt." to="/reference/learn/integrations/dbt_tutorial" icon="/img/dbt_icon.svg"/>

</LinkCardGrid>