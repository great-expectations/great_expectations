---
sidebar_label: 'Limit validation results in Data Docs'
title: 'Limit validation results in Data Docs'
id: limit_validation_results
description: Limit validation results to improve Data Doc updating and rendering performance.
---

import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

As you use Great Expectations (GX), the number of <TechnicalTag tag="validation_result" text="Validation Results"/> that are generated, stored, and rendered for your <TechnicalTag tag="data_docs" text="Data Docs"/> also grows. This increasing Data Doc accumulation can result in a degradation of performance and an increased use of computational resources when you update or render them. 

Running numerous validations and the size of your data volume both contribute to Data Doc accumulation and degraded Checkpoint performance. Whenever you run a Checkpoint, the default GX behavior is to re-render all Data Docs within a deployment, regardless of the Data Docs’ association with the run in progress.

Use one of the following options to improve performance and reduce the likelihood that Data Doc accumulation will affect GX performance.

## The `UpdateDataDocs` Checkpoint Action

The `UpdateDataDocs` Checkpoint Action renders new Validations only for the Checkpoints containing the Action. GX recommends using the `UpdateDataDocs` Checkpoint Action in the following circumstances:

- You're experiencing a performance degradation that is caused by too many Validations.

- The Validations running in your Checkpoints are smaller than the other Validations in your environment in terms of the Expectation or data volumes.

An important consideration is that the `UpdateDataDocs` Checkpoint Action is limited to the active Checkpoint and changes made to GX or to your local environment might not be captured in other Data Docs. Also, the `UpdateDataDocs` Checkpoint Action might be unsuitable if you're displaying Data Docs live.

## The `validation_results_limit` option

When the `UpdateDataDocs` Checkpoint Action is not suitable, you can use the `validation_results_limit` option to specify the number of historical Data Docs that GX retains. 

The `validation_results_limit` is an option for the `site_index_builder` parameter that is part of the larger `data_docs_sites` settings in your `great_expectations.yml` file. 

The following example limits the Validation Results on a local Data Docs site to the five most recent:

```yaml
data_docs_sites:
 local_site:
   class_name: SiteBuilder
   show_how_to_buttons: true
   store_backend:
     class_name: TupleFilesystemStoreBackend
     base_directory: uncommitted/data_docs/local_site/
   site_index_builder:
     class_name: DefaultSiteIndexBuilder
     validation_results_limit: 5
```

When you use the `validation_results_limit` option, Validation Results from previous Checkpoints are only rendered and indexed to the defined limit. If your GX performance issue is due to a historical accumulation of Data Docs, using `validation_results_limit` can help improve performance without sacrificing Data Doc creation in your environment.

The `validation_results_limit` option doesn’t limit the number of HTML documents contained in your Data Docs site. If HTML documents other than Validation Results are contributing to performance degradation, the `validation_results_limit` option won't help.
