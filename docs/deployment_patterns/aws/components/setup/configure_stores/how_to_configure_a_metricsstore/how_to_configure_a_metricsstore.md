---
title: How to configure and use a MetricStore
---
import AddingAMetricstore from './_adding_a_s3_metricstore.mdx'
import ConfiguringAValidationAction from './_configuring_a_validation_action.mdx'
import TestYourMetricstoreAndStoremetricsaction from './_test_your_metricstore_and_storemetricsaction.mdx'
import Summary from './_summary.mdx'

import TechnicalTag from '/docs/term_tags/_tag.mdx';

Saving <TechnicalTag tag="metric" text="Metrics" /> during <TechnicalTag tag="validation" text="Validation" /> makes it easy to construct a new data series based on observed dataset characteristics computed by Great Expectations. That data series can serve as the source for a dashboard or overall data quality metrics, for example.

Storing metrics is still a **beta** feature of Great Expectations, and we expect configuration and capability to evolve rapidly.

## Steps

### 1. Add a S3 MetricStore
<AddingAMetricstore />

### 2. Configure a Validation Action
<ConfiguringAValidationAction />

### 3. Test your MetricStore and StoreMetricsAction
<TestYourMetricstoreAndStoremetricsaction />

## Summary
<Summary />
