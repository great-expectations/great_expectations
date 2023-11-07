---
sidebar_label: 'Troubleshooting'
title: 'Troubleshooting'
description: Resolve common issues with GX OSS.
---

Use the information provided here to help you resolve issues with GX OSS.

### My Expectation Suite is not generating Expectations

This issue is typically caused when an Expectation fails and `discard_failed_expectations` is set to `True`. To resolve this issue, set `discard_failed_expectations` to `False` when using `validator.save_expectation_suite`. For example, `validator.save_expectation_suite(discard_failed_expectations= False)`.

### Why did a specific Expectation fail?

See [Identify failed rows in an Expectation](./guides/expectations/advanced/identify_failed_rows_expectations.md). 

### How can I improve Validation performance with a large Data Asset?

Use Batches and Spark. See [Request data from a Data Asset](./guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset.md) and [Connect to in-memory Data Assets - Spark](./guides/connecting_to_your_data/fluent/in_memory/connect_in_memory_data.md). 

### I'm experiencing issues with my Expectations and Data Sources after upgrading GX OSS

Confirm you're using Fluent Data Sources and you have installed the latest version of GX OSS. If you're using data connectors, or importing `RuntimeBatchRequest` or `BatchRequest` methods, it's likely you're using an outdated version of GX OSS.

### How do I adjust the timezone and regional settings in my Data Docs?

Unfortunately, these settings can't be adjusted. GX uses your computer settings to determine the time value.