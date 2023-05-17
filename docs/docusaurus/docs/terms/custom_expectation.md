---
title: "Custom Expectation"
---

import TechnicalTag from '../term_tags/_tag.mdx';

A Custom Expectation is an extension of the `Expectation` class, developed outside the Great Expectations library.  When you create a Custom Expectation, you can tailor it your specific needs.

Custom Expectations are intended to allow you to create <TechnicalTag relative="../" tag="expectation" text="Expectations" /> tailored to your specific data needs.

## Relationship to other objects

Other than the development of Custom Expectations, which takes place outside the usual Great Expectations workflow for <TechnicalTag relative="../" tag="validation" text="Validating" /> data, Custom Expectations should interact with Great Expectations in the same way as any other Expectation would.

## Use cases

For details on when and how you would use a Custom Expectation to Validate Data, see [the corresponding documentation on Expectations](./expectation.md#use-cases).

## Access

If you are using a Custom Expectation to validate data, you will typically access it exactly as you would any other Expectation.  However, if your Custom Expectation has not yet been contributed or merged into the Great Expectations codebase, you may want to set your Custom Expectation up to be accessed as a Plugin.  This will allow you to continue using your Custom Expectation while you wait for it to be accepted and merged.

If you are still working on developing your Custom Expectation, you will access it by opening the python file that contains it in your preferred editing environment.

## Create

We provide extensive documentation on how to create Custom Expectations.  If you are interested in doing so, we advise you reference [our guides on how to create Custom Expectations](../guides/expectations/index.md#creating-custom-expectations).

## Contribute

Community contributions are a great way to help Great Expectations grow!  If you've created a Custom Expectation that you would like to share with others, we have a [guide on how to contribute a new Expectation to Great Expectations](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_EXPECTATIONS.md), just waiting for you!

