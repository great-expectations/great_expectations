---
title: How to write a how-to-guide
---



Steps
First, do something bashy.

something --foo bar
Next, do something with yml.

site_section_builders:
  expectations: # Look, comments work, too!
    class_name: DefaultSiteSectionBuilder
    source_store_name: expectations_store
    renderer:
      module_name: great_expectations.render.renderer
      class_name: ExpectationSuitePageRenderer
Next, try a python snippet or two.

batch.expect_table_row_count_to_be_between(min_value=1000, max_value=4000)
Here’s a pinch of connecting text.

batch.expect_table_row_count_to_be_between(min_value=2000, max_value=5000)
Additional notes
How-to guides are not about teaching or explanation. They are about providing clear, bite-sized replication steps. If you must include a longer explanation, it should go in this section.

Additional resources
Links in RST are a pain.

