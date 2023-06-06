---
title: How to configure an Expectation Store to use a filesystem
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

By default, new <TechnicalTag tag="profiling" text="Profiled" /> <TechnicalTag tag="expectation" text="Expectations" /> are stored as <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> in JSON format in the ``expectations/`` subdirectory of your ``great_expectations`` folder. Use the information provided here to configure a new storage location for Expectations on your filesystem.

## Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectation Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- A storage location for Expectations. This can be a local path, or a path to a network filesystem.
    
</Prerequisites>

## 1. Create a new folder for Expectations

Run the following command to create a new folder for your Expectations and move your existing Expectations to the new folder:

```bash
# in the great_expectations/ folder
mkdir shared_expectations
mv expectations/npi_expectations.json shared_expectations/
```
In this example, the name of the Expectation is ``npi_expectations`` and the path to the new storage location is ``/shared_expectations``.

## 2. Identify your Data Context Expectations Store

The configuration for your Expectations <TechnicalTag tag="store" text="Store" /> is available in your <TechnicalTag tag="data_context" text="Data Context" />.  Open ``great_expectations.yml``and find the following entry:

```yaml
expectations_store_name: expectations_store

stores:
  expectations_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: expectations/
```
This configuration tells Great Expectations to look for Expectations in the ``expectations_store`` Store. The default ``base_directory`` for ``expectations_store`` is ``expectations/``.

## 3. Update your configuration file to include a new Store for Expectations results

In the following example, `expectations_store_name` is set to ``shared_expectations_filesystem_store``, but it can be personalized.  Also, ``base_directory`` is set to ``shared_expectations/``, but you can set it to another path that is accessible by Great Expectations.

```yaml
expectations_store_name: shared_expectations_filesystem_store

stores:
  shared_expectations_filesystem_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: shared_expectations/
```

## 4. Confirm that the new Expectation Suites have been added

If you copied your existing Expectation Suites to your filesystem, run the following Python command to confirm that Great Expectations can find them:

<!--A snippet is required for this code block.-->

```python
import great_expectations as gx

context = gx.get_context()
context.list_expectation_suite_names()
```

A list of Expectation Suites you copied your filesystem is returned. Expectation Suites that weren't copied to the new Store aren't listed.

## Version control systems

GX recommends that you store Expectations in a version control system such as Git. The JSON format of Expectations allows for informative diff-statements and modification tracking. In the following example, the ```expect_table_column_count_to_equal`` value changes from ``333`` to ``331``, and then to ``330``:

```bash
git log -p npi_expectations.json

commit cbc127fb27095364c3c1fcbf6e7f078369b07455
  changed expect_table_column_count_to_equal to 331

diff --git a/great_expectations/expectations/npi_expectations.json b/great_expectations/expectations/npi_expectations.json

--- a/great_expectations/expectations/npi_expectations.json
+++ b/great_expectations/expectations/npi_expectations.json
@@ -17,7 +17,7 @@
   {
     "expectation_type": "expect_table_column_count_to_equal",
     "kwargs": {
-        "value": 333
+        "value": 331
     }
commit 05b3c8c1ed35d183bac1717d4877fe13bc574963
changed expect_table_column_count_to_equal to 333

diff --git a/great_expectations/expectations/npi_expectations.json b/great_expectations/expectations/npi_expectations.json
--- a/great_expectations/expectations/npi_expectations.json
+++ b/great_expectations/expectations/npi_expectations.json
   {
     "expectation_type": "expect_table_column_count_to_equal",
     "kwargs": {
-        "value": 330
+        "value": 333
     }
```
