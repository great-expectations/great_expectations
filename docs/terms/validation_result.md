---
id: validation_result
title: Validation Result
hoverText: An object generated when data is Validated against an Expectation or Expectation Suite.
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='inactive' connect='inactive' create='active' validate='active'/> 

## Overview

### Definition

A Validation Result is an object generated when data is validated against an Expectation or Expectation Suite.

### Features and promises

Validation Results can be saved for every time that a Checkpoint Validates data, permitting you to maintain records of your data's adherence to Expectations and track trends in the quality of the validated data.  Validation Results are also among the information passed to Actions when a Checkpoint is run.  This allows them to be used for any purpose that an Action can be created to handle. 

### Relationship to other objects

Validation Results are generated inside a Checkpoint when a Validator runs an Expectation Suite against its paired Batch Request.  The Validation Result will be passed to the Checkpoint's `action_list` where it will be available to any Actions that may need it.  The `StoreValidationResultAction` subclass of `ValidationAction` will save Validation Results to the Validation Result Store if it is present in a Checkpoint's `action_list`.

## Use cases

<CreateHeader/>

A Validation Result is generated when you use the interactive workflow to create Expectations and the newly created Expectation is run against the sample Batch of data you are working with.

<ValidateHeader/>

Validation Results are generated when you run Checkpoints to Validate data.  These Validation Results can be saved in JSON format in a Validation Result Store, and rendered as Data Docs.  Both of these activities are handled by Actions in the Checkpoint's `action_list`.

## Features

### Trends in data quality

Saved Validation Results allow you to track trends in how well your data adheres to Expectations.  This can help you determine if failed Expectations are due to outliers in new data, or a more systemic problem.

### Actions and documentation

Because Validation Results are passed to Actions in a Checkpoint's `action_list` you can create Actions that have behaviour conditional to the Validation Result.  An example of this might be sending a Slack or email notification if the Validation fails, or only launching a secondary process if the Validation Results report that Validation passed.

## API basics

### How to access

Inside of Actions, Validation Results are a parameter that is passed to the Action when it is run.  The other place where you can access your Validation Results is in your Validation Result Store.

In the Validation Result Store you will find your Validation Results stored as a list of dictionaries under the `results` key of the JSON file that corresponds to the Checkpoint run you are investigating.

### How to create

Validation Results are created automatically when data is Validated in Great Expectations.  You will not need to manually create them; they are a product of the Validation process.

A single Validation Result is created when an Expectation is created with the interactive workflow and the newly created Expectation validates itself against the sample data that is used in that process.  This is generated as an `ExpectationValidationResult` object.

A Validation Result is also created for each Expectation in an Expectation Suite when a Checkpoint is run.  These are generated as values in the `results` list of an `ExpectationSuiteValidationREsult` object.

### Configuration

A Validation Result provides information as attributes that are accessible through dot notation if you are accessing an `ExpectationValidationResult` instance.  These attributes are:

- **success:** A `true` or `false` indicator of whether the Expectation passed.
- **expectation_config:** The config used by the Expectation, including such things as type of Expectation and key word arguments that were passed to the Expectation.
- **result:** The observed value generated when the Expectation was run.
- **meta:** Provides additional information about the Validation Result of some Expectations.
- **exception_info:** This is a dictionary with three keys. 
  - `raised_exception` indicates if an exception was raised during the Validation. 
  - `exception_traceback` contains the traceback of the raised exception, if an exception was raised.
  - `exception_message` contains the message associated with the raised exception, if an exception was raised.

You may also interact with an `ExpectationSuiteValidationResult` object, which is configured to match the values that are saved to the Validation Result Store.  These are:

- **success:** A `true` or `false` indicator of whether all the Expectations in the Expectation Suite passed.
- **evaluation_parameters:** The Evaluation Parameters and their values at the time when the Expectation Suite was Validated.
- **results:** A list of `ExpectationValidationResult` results for each Expectation in the Expectation Suite.
- **meta:** Additional information about the Validation Results, such as the name of the Expectation Suite that was run, information about the Batch that was Validated, when the Validation took place, and what version of Great Expectations was used to run the Validation.
- **statistics:** Some statistics to summarize the `results` list, including things like the number of evaluated Expectations and the percentage of those Expectations that passed successfully.

The attributes described above for `ExpectationValidationResult` and `ExpectationSuiteValidationResult` also correspond to the keys that you will find in the serialized JSON that is created when Validation Results are saved to the Validation Results Store.