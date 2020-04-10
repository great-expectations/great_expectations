.. _contributing_style_guide:


Style Guide
==============

* Ensure any new features or behavioral differences introduced by your changes are documented in the docs, and ensure you have docstrings on your contributions. We use the Sphinx's `Napoleon extension <http://www.sphinx-doc.org/en/master/ext/napoleon.html>`__ to build documentation from Google-style docstrings.
* Avoid abbreviations, e.g. use `column_index` instead of `column_idx`.
* Use unambiguous expectation names, even if they're a bit longer, e.g. use `expect_columns_to_match_ordered_list` instead of `expect_columns_to_be`.
* Expectation names should reflect their decorators:

    * `expect_table_...` for methods decorated directly with `@expectation`
    * `expect_column_values_...` for `@column_map_expectation`
    * `expect_column_...` for `@column_aggregate_expectation`
    * `expect_column_pair_values...` for `@column_pair_map_expectation`

These guidelines should be followed consistently for methods and variables exposed in the API. They aren't intended to be strict rules for every internal line of code in every function.

