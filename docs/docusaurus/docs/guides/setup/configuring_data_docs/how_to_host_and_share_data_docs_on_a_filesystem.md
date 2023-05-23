---
title: How to host and share Data Docs on a filesystem
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will explain how to host and share <TechnicalTag relative="../../../" tag="data_docs" text="Data Docs" /> on a filesystem.

## Prerequisites

<Prerequisites>

- [A Great Expectations instance](/docs/guides/setup/setup_overview)

</Prerequisites>

## Steps

### 1. Review defaults and change if desired.

Filesystem-hosted Data Docs are configured by default for Great Expectations deployments created using great_expectations init.  To create additional Data Docs sites, you may re-use the default Data Docs configuration below. You may replace ``local_site`` with your own site name, or leave the default.

```yaml
data_docs_sites:
  local_site:  # this is a user-selected name - you may select your own
    class_name: SiteBuilder
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/data_docs/local_site/ # this is the default path but can be changed as required
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
```

### 2. Test that your configuration is correct by building the site

Run the following Python code to build and open your Data Docs:

``` python name="tests/integration/docusaurus/reference/glossary/data_docs.py data_docs"
```

## Additional notes

- To share the site, you can zip the directory specified under the ``base_directory`` key in your site configuration and distribute as desired.

## Additional resources

- <TechnicalTag tag="data_docs" text="Data Docs"/>
