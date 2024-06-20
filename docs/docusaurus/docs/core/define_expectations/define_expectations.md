---
title: 'Define Expectations'
description: Create and modify Expectations, then organize them into Expectation Suites.
hide_feedback_survey: true
hide_title: true
toc_min_heading_level: 1
toc_max_heading_level: 1
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Create, edit, and implement Expectations and Expectation Suites. An Expectation is a verifiable assertion about your data, and an Expectation Suite is a collection of Expectations that describe the ideal state of a set of data.
</OverviewCard>

<LinkCardGrid>
  <LinkCard 
    topIcon 
    label="Create an Expectation"
    description="Create an instance of an Expectation"
    to="/core/define_expectations/create_an_expectation" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Test an Expectation"
    description="Test an Expectation against a Batch of data."
    to="/core/define_expectations/test_an_expectation" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Organize Expectations into Expectation Suites"
    description="Modify the parameters for an existing Expectation"
    to="/core/define_expectations/organize_expectation_suites" 
    icon="/img/expectation_icon.svg" 
  />
</LinkCardGrid>
