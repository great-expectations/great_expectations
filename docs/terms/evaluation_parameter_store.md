---
title: "Evaluation Parameter Store (Metric Store)"
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import SetupHeader from '/docs/images/universal_map/_um_setup_header.mdx'
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='active' connect='inactive' create='active' validate='active'/> 

## Overview

### Definition

An Evaluation Parameter Store (also known as a Metric Store) is a connector to store and retrieve information about parameters used during Validation of an Expectation which reference simple expressions or previously generated metrics.

### Features and promises



### Relationship to other objects

Evaluation Parameter Stores are an alternative way to store Metrics without the accompanying Validation Results.  The process of storing information in an Evaluation Parameter Store is also typically executed bby an Action in a Checkpoint's `action_list`.  Metrics stored in Evaluation Parameter Stores are also available as Evaluation Parameters when defining Expectations.

## Use cases

<SetupHeader/>



<CreateHeader/>



<ValidateHeader/>



## Features

## API basics

### How to access

### How to create

### Configuration

## More details

### Design motivation
