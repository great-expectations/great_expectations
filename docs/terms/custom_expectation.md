---
title: "Custom Expectation"
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='inactive' connect='inactive' create='active' validate='active'/> 

## Overview

### Definition

A Custom Expectation is an extension of the `Expectation` class, developed outside the Great Expectations library.

### Features and promises

Custom Expectations are intended to allow you to create <TechnicalTag relative="../" tag="expectation" text="Expectations" /> tailored to your specific data needs.

### Relationship to other objects

Other than the development of Custom Expectations, which takes place outside the usual Great Expectations workflow for <TechnicalTag relative="../" tag="validation" text="Validating" /> data, Custom Expectations should interact with Great Expectations in the same way as any other Expectation would.

## Use cases

<UniversalMap setup='inactive' connect='inactive' create='active' validate='active'/>

For details on when and how you would use a Custom Expectation to Validate Data, please see [the corresponding documentation on Expectations](./expectation.md#use-cases).

## Features

### Whatever you need

Custom Expectations are created outside Great Expectations.  When you create a Custom Expectation, you can tailor it to whatever needs you and your data have.

## API basics

### How to access

If you are using a Custom Expectation to validate data, you will typically access it exactly as you would any other Expectation.  However, if your Custom Expectation has not yet been contributed or merged into the Great Expectations codebase, you may want to set your Custom Expectation up to be accessed as a Plugin.  This will allow you to continue using your Custom Expectation while you wait for it to be accepted and merged.

If you are still working on developing your Custom Expectation, you will access it by opening the python file that contains it in your preferred editing environment.

### How to create

We provide extensive documentation on how to create Custom Expectations.  If you are interested in doing so, we advise you reference [our guides on how to create Custom Expectations](../guides/expectations/index.md#creating-custom-expectations).

### How to contribute

Community contributions are a great way to help Great Expectations grow!  If you've created a Custom Expectation that you would like to share with others, we have a [guide on how to contribute a new Expectation to Great Expectations](../guides/expectations/contributing/how_to_contribute_a_custom_expectation_to_great_expectations.md), just waiting for you!

