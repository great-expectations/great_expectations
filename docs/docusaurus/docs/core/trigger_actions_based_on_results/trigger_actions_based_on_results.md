---
title: "Trigger actions based on results"
description: Learn how to use Checkpoints to automate responses to Validation Results.
hide_feedback_survey: true
hide_title: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Learn how to use Checkpoints to automate responses to Validation Results such as sending alerts and updating Data Docs.
</OverviewCard>


<LinkCardGrid>

  <LinkCard 
    topIcon 
    label="Create a Checkpoint with Actions"
    description="Create a Checkpoint that triggers actions based on Validation Results."
    to="/core/trigger_actions_based_on_results/create_a_checkpoint_with_actions" 
    icon="/img/expectation_icon.svg" 
  />
  
  <LinkCard 
    topIcon 
    label="Choose a Result Format"
    description="Configure the verbosity of returned Validation Results."
    to="/core/trigger_actions_based_on_results/choose_a_result_format" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Run a Checkpoint"
    description="Validate data and automate actions based on the Validation Results by running a Checkpoint"
    to="/core/trigger_actions_based_on_results/run_a_checkpoint" 
    icon="/img/expectation_icon.svg" 
  />

</LinkCardGrid>