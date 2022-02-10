---
title: "Data Docs Store"
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import SetupHeader from '/docs/images/universal_map/_um_setup_header.mdx'
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='active' connect='active' create='active' validate='active'/> 

## Overview

### Definition

A Data Docs Store is a connector to store and retrieve information pertaining to Human readable documentation generated from Great Expectations metadata detailing Expectations, Validation Results, etc.

### Features and promises

### Relationship to other objects

## Use cases

<SetupHeader/>



<CreateHeader/>

When Profilers are run to create Expectations their results are made available through Data Docs.  This process uses a Data Docs Store behind the scenes.

<ValidateHeader/>



## Features

## API basics

### How to access

### How to create

### Configuration

Unlike other Stores, Data Docs stores are configured in the `store_backend` section under `data_doc_sites` in the `great_expectations.yml` config file.

## More details

### Design motivation
