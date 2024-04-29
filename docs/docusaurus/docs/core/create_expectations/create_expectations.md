---
title: 'Create and manage Expectations and Expectation Suites'
description: Create and manage Expectations and Expectation Suites.
hide_feedback_survey: true
hide_title: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Create, edit, and implement Expectations and Expectation Suites. An Expectation is a verifiable assertion about your data, and an Expectation Suite is a collection of Expectations that describe the ideal state of a set of data.
</OverviewCard>

## Manage Expectations

<LinkCardGrid>
  <LinkCard 
    topIcon 
    label="Create an Expectation"
    description="Create an instance of an Expectation"
    to="/core/create_expectations/expectations/manage_expectations#create-an-expectation" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Test an Expectation"
    description="Test an Expectation against a Batch of data."
    to="/core/create_expectations/expectations/manage_expectations#test-an-expectation" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Modify an Expectation"
    description="Modify the parameters for an existing Expectation"
    to="/core/create_expectations/expectations/manage_expectations#modify-an-expectation" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Customize an Expectation Class"
    description="Customize the name, default parameters, and Data Docs rendering of an existing Expectation Class"
    to="/core/create_expectations/expectations/manage_expectations#customize-an-expectation-class" 
    icon="/img/expectation_icon.svg" 
  />
</LinkCardGrid>

## Manage Expectation Suites

<LinkCardGrid>
  <LinkCard 
    topIcon 
    label="Create an Expectation Suite"
    description="Create an empty Expectation Suite to populate with Expectations"
    to="/core/create_expectations/expectation_suites/manage_expectation_suites#create-an-expectation-suite" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Get an existing Expectation Suite"
    description="Retrieve a previously created Expectation Suite from a Data Context"
    to="/core/create_expectations/expectation_suites/manage_expectation_suites#get-an-existing-expectation-suite" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Rename an Expectation Suite"
    description="Modify the name of a new or existing Expectation Suite and save the new value."
    to="/core/create_expectations/expectation_suites/manage_expectation_suites#rename-an-expectation-suite" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Delete an Expectation Suite"
    description="Permanently remove an Expectation Suite from a Data Context"
    to="/core/create_expectations/expectation_suites/manage_expectation_suites#delete-an-expectation-suite" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Add Expectations"
    description="Add individual Expectations to an Expectation Suite"
    to="/core/create_expectations/expectation_suites/manage_expectation_suites#add-expectations-to-an-expectation-suite" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Get an Expectation"
    description="Get a specific Expectation from an Expectation Suite"
    to="/core/create_expectations/expectation_suites/manage_expectation_suites#get-an-expectation-from-an-expectation-suite" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Edit a single Expectation"
    description="Edit a specific Expectation in an Expectation Suite"
    to="/core/create_expectations/expectation_suites/manage_expectation_suites#edit-a-single-expectation-in-an-expectation-suite" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Edit multiple Expectations"
    description="Apply edits to multiple Expectations in an Expectation Suite"
    to="/core/create_expectations/expectation_suites/manage_expectation_suites#edit-multiple-expectations-in-an-expectation-suite" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Delete an Expectation"
    description="Delete an Expectation from an Expectation Suite"
    to="/core/create_expectations/expectation_suites/manage_expectation_suites#delete-an-expectation-from-an-expectation-suite" 
    icon="/img/expectation_icon.svg" 
  />
</LinkCardGrid>
