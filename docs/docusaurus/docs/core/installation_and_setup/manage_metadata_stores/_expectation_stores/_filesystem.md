Use the information provided here to configure a new storage location for Expectations on your Filesystem.

### Prerequisites

- [A Data Context](/core/installation_and_setup/manage_data_contexts.md).
- [An Expectations Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md).
- A storage location for Expectations. This can be a local path, or a path to a network filesystem.

### Create a new folder for Expectations

Run the following command to create a new folder for your Expectations and move your existing Expectations to the new folder:

```bash
# in the gx/ folder
mkdir shared_expectations
mv expectations/npi_expectations.json shared_expectations/
```
In this example, the name of the Expectation is ``npi_expectations`` and the path to the new storage location is ``/shared_expectations``.

### Identify your Data Context Expectations Store

The configuration for your Expectations Store is available in your Data Context.  Open ``great_expectations.yml``and find the following entry:

```yaml
expectations_store_name: expectations_store

stores:
  expectations_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: expectations/
```
This configuration tells GX Core to look for Expectations in the ``expectations_store`` Store. The default ``base_directory`` for ``expectations_store`` is ``expectations/``.

### Update your configuration file to include a new Store for Expectations results

In the following example, `expectations_store_name` is set to ``shared_expectations_filesystem_store``, but it can be personalized.  Also, ``base_directory`` is set to ``shared_expectations/``, but you can set it to another path that is accessible by GX Core.

```yaml
expectations_store_name: shared_expectations_filesystem_store

stores:
  shared_expectations_filesystem_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: shared_expectations/
```

### Confirm that the new Expectation Suites have been added

If you copied your existing Expectation Suites to your filesystem, run the following Python command to confirm that GX Core can find them:

<!--A snippet is required for this code block.-->

```python
import great_expectations as gx

context = gx.get_context()
context.list_expectation_suite_names()
```

A list of Expectation Suites you copied your filesystem is returned. Expectation Suites that weren't copied to the new Store aren't listed.

### Version control systems

GX recommends that you store Expectations in a version control system such as Git. The JSON format of Expectations allows for informative diff-statements and modification tracking. In the following example, the ```expect_table_column_count_to_equal`` value changes from ``333`` to ``331``, and then to ``330``:

```bash
git log -p npi_expectations.json

commit cbc127fb27095364c3c1fcbf6e7f078369b07455
  changed expect_table_column_count_to_equal to 331

diff --git a/gx/expectations/npi_expectations.json b/great_expectations/expectations/npi_expectations.json

--- a/gx/expectations/npi_expectations.json
+++ b/gx/expectations/npi_expectations.json
@@ -17,7 +17,7 @@
   {
     "expectation_type": "expect_table_column_count_to_equal",
     "kwargs": {
-        "value": 333
+        "value": 331
     }
commit 05b3c8c1ed35d183bac1717d4877fe13bc574963
changed expect_table_column_count_to_equal to 333

diff --git a/gx/expectations/npi_expectations.json b/great_expectations/expectations/npi_expectations.json
--- a/gx/expectations/npi_expectations.json
+++ b/gx/expectations/npi_expectations.json
   {
     "expectation_type": "expect_table_column_count_to_equal",
     "kwargs": {
-        "value": 330
+        "value": 333
     }
```