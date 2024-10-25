---
sidebar_label: 'Data quality use cases'
title: 'Data quality use cases'
hide_title: true
description: Example data quality use cases and implementations with GX Cloud and GX Core.
hide_feedback_survey: true
pagination_next: null
pagination_prev: null
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Learn how to use GX to validate key data quality dimensions.
</OverviewCard>

A comprehensive data quality strategy relies on a multi-dimensional approach to achieving and maintaining high-quality data. GX enables you to define and validate data quality checks across a variety of dimensions.

<LinkCardGrid>

  <LinkCard topIcon label="Distribution" description="Validate that data values adhere to expected ranges." to="/reference/learn/data_quality_use_cases/distribution" icon="/img/actions_icon.svg"/>

  <LinkCard topIcon label="Missingness" description="Identify gaps in data to maintain data completeness." to="/reference/learn/data_quality_use_cases/missingness" icon="/img/actions_icon.svg"/>

  <LinkCard topIcon label="Schema" description="Verify that data structure conforms to established rules." to="/reference/learn/data_quality_use_cases/schema" icon="/img/actions_icon.svg"/>

  <LinkCard topIcon label="Volume" description="Validate that record quantity falls within expected bounds." to="/reference/learn/data_quality_use_cases/volume" icon="/img/actions_icon.svg"/>

</LinkCardGrid>