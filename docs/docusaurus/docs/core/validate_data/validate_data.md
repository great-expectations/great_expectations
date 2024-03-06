---
title: "Validate Data"
---

import OverviewCard from '@site/src/components/OverviewCard';
import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';

import InProgress from '../_core_components/_in_progress.md';

<InProgress/>

<LinkCardGrid>
  
  <LinkCard 
      topIcon 
      label="Manage Validators"
      description="Use Validators to validate"
      to="/core/validate_data/manage_validators" 
      icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Manage Checkpoints"
    description="Use Checkpoints to validate Expectation Suites against Batches of data."
    to="/core/validate_data/manage_checkpoints.md" 
    icon="/img/expectation_icon.svg" 
  />
  
</LinkCardGrid>