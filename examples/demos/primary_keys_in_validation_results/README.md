---
title: Examples for configuring primary key (PK) values in Validation results.
author: Shinnnyshinshin
date: 20220118
---
## Overview

- This folder contains example files and data that were shown at the [January 2022 Great Expectations Community Meetup
](https://www.youtube.com/watch?v=mrE-5dOdEmg&ab_channel=GreatExpectations).

## What's in the folder?

- `great_expectation.yml` file containing a `Data Source` with a `InferredAssetDataConnector` connecting to example data. For Pandas and Spark it is the `.csv` and for sqlite it is the `.db` file.
- `ExpectationSuite` named `visitors_exp` that contains a single Expectation. It is configured to detect non-pageview related entries in the `event_type` column of our data.
- `Checkpoint` configuration named `my_checkpoint` will run the `visitor_exp` Expectation Suite against the data configured in the `Data Source`.
- Jupyter Notebook containing `Checkpoint` configuration with `result_format` configured in the following ways:
    1. Checkpoint run without primary key configuration.
    2. Checkpoint run with a **single** column defined in `unexpected_index_column_names`.
    3. Checkpoint run with **two** columns defined in `unexpcted_index_column_names`.
- For each of the runs, DataDocs are created and opened.
