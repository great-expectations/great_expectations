---
title: Configurations for Testing V2 to V3 API Migration
author: @Shinnnyshinshin
date: 20211022
---
## Overview

- This folder contains configurations that were used to test the V2 to V3 migration guide found here : https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide
- It contains a complete-and-working V2 Configuration and a complete-and-working V3 Configuration that can be used to help with the migration process.

## So what's in the folder?

- `data/`: This folder contains a test file, `Titanic.csv` that is used by the configurations in this directory.  

- The other folders `pandas/`, `spark/`, `postgresql/` each contain the following:
  - **V2 configuration in `v2/great_expectations/` folder**
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

  - **V3 configuration in `v2/great_expectations/` folder**
    - Checkpoint named `test_v3_checkpoint`
    - uses Checkpoint class
    - uses batch_request
    - references Titanic.csv testfile
    - `great_expectations.yml`
      - uses config_version: 3.0
      - uses v3-datasource
      - uses `CheckpointStore`

- In the `postgresql/` folder, there is an additional Jupyter Notebook that can be used to load the `Titanic.csv` into a `postgresql` database running in a local Docker container. In developing these example configurations, we used the `docker-compose.yml` file that is in the [`great_expectations` repository](https://github.com/great-expectations/great_expectations/tree/develop/assets/docker/postgresql)
