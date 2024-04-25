---
title: "Validate Data"
hide_feedback_survey: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

import InProgress from '../_core_components/_in_progress.md';

<OverviewCard title={frontMatter.title}>
  Create, edit, and implement Validation Definitions and Checkpoints.
</OverviewCard>

## Manage Validation Definitions

Use Validation Definitions to associate data with Expectation Suites for Checkpoints and generate Validation Results for data testing and exploration.

<LinkCardGrid>
  
  <LinkCard 
      topIcon 
      label="Create a Validation Definition"
      description="Associate a Batch of data with an Expectation Suite."
      to="/core/validate_data/validation_definitions/manage_validation_definitions#create-a-validation-definition" 
      icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Get a Validation Definition by name"
    description="Retrieve a single, specific Validation Definition from your Data Context."
    to="/core/validate_data/validation_definitions/manage_validation_definitions#get-a-validation-definition-by-name" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Get a Validation Definition by attributes"
    description="Retrieve related Validation Definitions by referencing their shared attributes."
    to="/core/validate_data/validation_definitions/manage_validation_definitions#get-validation-definitions-by-attributes" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Delete a Validation Definition"
    description="Remove a Validation Definition from your Data Context."
    to="/core/validate_data/checkpoints/manage_checkpoints" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Rename a Validation Definition"
    description="Replace an existing Validation Definition with a renamed one."
    to="/core/validate_data/checkpoints/manage_checkpoints" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Run a Validation Definition"
    description="Validate an Expectation Suite against a Batch of data using a Validation Definition."
    to="/core/validate_data/checkpoints/manage_checkpoints" 
    icon="/img/expectation_icon.svg" 
  />
  
</LinkCardGrid>

## Manage Checkpoints

A Checkpoint executes one or more Validation Definitions and then performs a set of Actions based on the Validation Results each Validation Definition returns.

<LinkCardGrid>
  
  <LinkCard 
      topIcon 
      label="Create a Checkpoint"
      description="Create a Checkpoint."
      to="/core/validate_data/validation_definitions/manage_validation_definitions#create-a-validation-definition" 
      icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="List available Checkpoints"
    description="List available Checkpoints."
    to="/core/validate_data/checkpoints/manage_checkpoints#list-available-checkpoints" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Get a Checkpoint by name"
    description="Get a Checkpoint by name."
    to="/core/validate_data/checkpoints/manage_checkpoints#get-a-checkpoint-by-name" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Get Checkpoints by attributes"
    description="/core/validate_data/checkpoints/manage_checkpoints#get-checkpoints-by-attributes."
    to="/core/validate_data/checkpoints/manage_checkpoints#get-checkpoints-by-attributes" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Update a Checkpoint"
    description="Update a Checkpoint."
    to="/core/validate_data/checkpoints/manage_checkpoints#update-a-checkpoint" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Delete a Checkpoint"
    description="Delete a Checkpoint."
    to="/core/validate_data/checkpoints/manage_checkpoints#delete-a-checkpoint" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Run a Checkpoint"
    description="Run a Checkpoint."
    to="/core/validate_data/checkpoints/manage_checkpoints#run-a-checkpoint" 
    icon="/img/expectation_icon.svg" 
  />
  
</LinkCardGrid>