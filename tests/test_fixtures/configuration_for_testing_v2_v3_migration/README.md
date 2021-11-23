---
title: Configurations for Testing V2 to V3 API Migration
author: @Shinnnyshinshin
date: 20211022
---
## Overview

- This folder contains configurations that were used to test the V2 to V3 migration guide found here : https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide
- It contains a complete-and-working V2 Configuration and a complete-and-working V3 Configuration that can be used to

## So what's in the folder?

- Test File : `Titanic.csv` that is found in the base directory

**V2 configuration in `v2/great_expectations/` folder**
  - Checkpoint named `test_v2_checkpoint`
  - uses LegacyCheckpoint class
  - uses batch_kwargs
  - uses Validation Operator action_list_operator
  - references Titanic.csv testfile
  - `great_expectations.yml`
    - uses config_version: `2.0`
    - uses v2-datasource
    - uses `validation_operators`
    - no `CheckpointStore`

**V3 configuration in `v2/great_expectations/` folder**
  - Checkpoint named `test_v3_checkpoint`
  - uses Checkpoint class
  - uses batch_request
  - references Titanic.csv testfile
  - `great_expectations.yml`
    - uses config_version: 3.0
    - uses v3-datasource
    - uses `CheckpointStore`
