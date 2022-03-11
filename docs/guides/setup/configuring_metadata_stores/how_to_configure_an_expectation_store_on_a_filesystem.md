---
title: How to configure an Expectation Store to use a filesystem
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

By default, newly <TechnicalTag tag="profiling" text="Profiled" /> <TechnicalTag tag="expectation" text="Expectations" /> are stored as <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> in JSON format in the ``expectations/`` subdirectory of your ``great_expectations`` folder.  This guide will help you configure a new storage location for Expectations on your filesystem.

<Prerequisites>

- [Configured a Data Context](../../../tutorials/getting_started/initialize_a_data_context.md).
- [Configured an Expectation Suite](../../../tutorials/getting_started/create_your_first_expectations.md).
- Determined a new storage location where you would like to store Expectations. This can either be a local path, or a path to a network filesystem.
    
</Prerequisites>

## Steps

### 1. Configure a new folder on your filesystem where Expectations will be stored

Create a new folder where you would like to store your Expectations, and move your existing Expectation files over to the new location. In our case, the name of the Expectations file is ``npi_expectations`` and the path to our new storage location is ``/shared_expectations``.

```bash
# in the great_expectations/ folder
mkdir shared_expectations
mv expectations/npi_expectations.json shared_expectations/
```


### 2. Identify your Data Context Expectations Store

In your ``great_expectations.yml`` , look for the following lines.  The configuration tells Great Expectations to look for Expectations in a <TechnicalTag tag="store" text="Store" /> called ``expectations_store``. The ``base_directory`` for ``expectations_store`` is set to ``expectations/`` by default.

```yaml
expectations_store_name: expectations_store

stores:
  expectations_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: expectations/
```


### 3. Update your configuration file to include a new store for Expectations results on your filesystem

In the example below, <TechnicalTag tag="expectation_store" text="Expectations Store" /> is being set to ``shared_expectations_filesystem_store`` with the ``base_directory`` set to ``shared_expectations/``.

```yaml
expectations_store_name: shared_expectations_filesystem_store

stores:
  shared_expectations_filesystem_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: shared_expectations/
```


### 4. Confirm that the location has been updated by running ``great_expectations store list``

Notice the output contains two Expectation stores: the original ``expectations_store`` on the local filesystem and the ``shared_expectations_filesystem_store`` we just configured.  This is ok, since Great Expectations will look for Expectations in the ``shared_expectations/`` folder as long as we set the ``expectations_store_name`` variable to ``shared_expectations_filesystem_store``.  The config for ``expectations_store`` can be removed if you would like.

```bash
great_expectations store list

2 Stores found:

- name: expectations_store
class_name: ExpectationsStore
store_backend:
  class_name: TupleFilesystemStoreBackend
  base_directory: expectations/

- name: shared_expectations_filesystem_store
class_name: ExpectationsStore
store_backend:
  class_name: TupleFilesystemStoreBackend
  base_directory: shared_expectations/
```


### 5. Confirm that Expectations can be read from the new storage location by running ``great_expectations suite list``

```bash
great_expectations suite list

1 Expectation Suite found:
  - npi_expectations
```

## Additional Notes

- For best practices, we highly recommend that you store Expectations in a version-control system like Git. The JSON format of Expectations will allow for informative diff-statements and effective tracking of modifications. In the example below, 2 changes have been made to ``npi_expectations``.  The Expectation ```expect_table_column_count_to_equal`` was changed from ``330`` to ``333`` to ``331``.

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
