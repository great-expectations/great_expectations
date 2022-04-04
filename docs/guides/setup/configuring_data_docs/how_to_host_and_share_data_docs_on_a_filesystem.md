---
title: How to host and share Data Docs on a filesystem
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '/docs/term_tags/_tag.mdx';

This guide will explain how to host and share <TechnicalTag relative="../../../" tag="data_docs" text="Data Docs" /> on a filesystem.

<Prerequisites>

- [Set up a working deployment of Great Expectations.](../../../tutorials/getting_started/intro.md)

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

Use the following <TechnicalTag relative="../../../" tag="cli" text="CLI" /> command: ``great_expectations docs build --site-name local_site``. If successful, the CLI will open your newly built Data Docs site and provide the path to the index page.

```bash
> great_expectations docs build --site-name local_site

The following Data Docs sites will be built:

 - local_site: file:///great_expectations/uncommitted/data_docs/local_site/index.html

Would you like to proceed? [Y/n]: Y

Building Data Docs...

Done building Data Docs
```

## Additional notes

- To share the site, you can zip the directory specified under the ``base_directory`` key in your site configuration and distribute as desired.

## Additional resources

- [Core concepts: Data Docs](../../../reference/data_docs.md)
