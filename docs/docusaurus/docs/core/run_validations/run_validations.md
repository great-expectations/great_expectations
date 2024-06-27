---
title: "Run Validations"
description: Learn how to associate a Batch Definition with an Expectation Suite, set a format for results, and run a Validation Definition using default parameters or parameters provided at runtime.
hide_feedback_survey: true
hide_title: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Learn how to associate a Batch Definition with an Expectation Suite, set a format for results, and run a Validation Definition using default parameters or parameters provided at runtime.
</OverviewCard>


<LinkCardGrid>

  <LinkCard 
    topIcon 
    label="Create a Validation Definition"
    description="Use a Validation Definition to associate a Batch Definition with an Expectation Suite."
    to="/core/run_validations/create_a_validation_definition" 
    icon="/img/expectation_icon.svg" 
  />
  
  <LinkCard 
    topIcon 
    label="Choose result format"
    description="Set the level of detail for the results of a Validation Definition."
    to="/core/run_validations/choose_result_format" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Run a Validation Definition"
    description="Run a Validation Definition using predefined defaults or parameters defined at runtime."
    to="/core/run_validations/run_a_validation_definition" 
    icon="/img/expectation_icon.svg" 
  />

</LinkCardGrid>