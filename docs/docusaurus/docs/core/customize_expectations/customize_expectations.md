---
title: 'Customize Expectations'
description: Learn how to customize Expectation classes, add notes and metadata, and use SQL queries and row conditions to add an additional filter on what data the Expectation evaluates.
hide_feedback_survey: true
hide_title: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Learn how to customize Expectation classes, add notes and metadata, and use SQL queries and row conditions to add an additional filter on what data the Expectation evaluates.
</OverviewCard>

<LinkCardGrid>
  <LinkCard 
    topIcon 
    label="Restrict an Expectation with row conditions"
    description="Use `row_conditions` to restrict the data an Expectation evaluates"
    to="/core/customize_expectations/expectation_row_conditions" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
      topIcon 
      label="Define a custom Expectation class"
      description="Define an Expectation class with additional notes and default attributes by subclassing an existing Expectation."
      to="/core/customize_expectations/define_a_custom_expectation" 
      icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
      topIcon 
      label="Use SQL to define a custom Expectation"
      description="Create an Expectation that operates by evaluating the results of a customized SQL query."
      to="/core/customize_expectations/use_sql_to_define_a_custom_expectation.md" 
      icon="/img/expectation_icon.svg" 
    /> 
</LinkCardGrid>
