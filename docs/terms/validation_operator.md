---
title: Validation Operator
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='inactive' connect='inactive' create='inactive' validate='active'/> 

## Overview

### Definition

A Validation Operator triggers <TechnicalTag relative="../" tag="action" text="Actions" /> after <TechnicalTag relative="../" tag="validation" text="Validation" /> completes.

### Features and promises

As of the V3 (Batch Request) API release, you will not need to directly interact with a Validation Operator.  The Validation Operator runs the Actions in a <TechnicalTag relative="../" tag="checkpoint" text="Checkpoint's" /> `action_list`.  Although Validation Operators are a vital part of the process of running Actions, they conveniently work behind the scenes.  All you need to do is have your Checkpoint's Actions properly configured in the `action_list`.   

### Relationship to other objects

A Validation Operator is an internal part of a Checkpoint.  It runs the Actions that are configured in the Checkpoint's `action_list` after Validation completes.

## Use cases

<ValidateHeader/>

Validation Operators only come into play when a Checkpoint is run. After Validation completes, the Validation Operator will run any Actions that are configured in the Checkpoint's `action_list`

## Features

### Convenience

As of the V3 (Batch Request) API release, Validation Operators moved to a behind-the-scenes role.  You no longer need to do anything with a Validation Operator in order to get one to work: they are automatically included as part of any Checkpoint you run.

## API basics and configuration

You will not need to access a Validation Operator if you are using the V3 (Batch Request) API.  

:::note
If you are still using the V2 API and need to access, create, or configure a Validation Operator, please see [our V2 documentation.](https://legacy.docs.greatexpectations.io/en/latest/)
:::