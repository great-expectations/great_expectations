---
title: Use SQL to define a custom Expectation
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../_core_components/prerequisites/_gx_installation.md';
import PrereqPreconfiguredDataContext from '../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqPreconfiguredDataSourceAndAsset from '../_core_components/prerequisites/_data_source_and_asset_connected_to_data.md';

Among the available Expectations, the `UnexpectedRowsExpectation` is designed to facilitate the execution of SQL or Spark-SQL queries as the core logic for an Expectation.  By default, `UnexpectedRowsExpectation` considers validation successful when no rows are returned by the provided SQL query.

You customize an `UnexpectedRowsExpectation` in essentially the same manner as you would [define a custom Expectation](/core/customize_expectations/define_a_custom_expectation_class.md), by subclassing `UnexpectedRowsExpectation` and providing customized default attributes and text for Data Docs. However, there are some caveats around the `UnexpectedRowsExpectation`'s `unexpected_rows_query` attribute that deserve further detail.

<!-- TODO: Do we want to discuss custom `_validate(...)` logic here, or should that be held for a future topic on building custom Expectation classes from scratch? -->

<!-- Additionally, the `UnexpectedRowsExpectation`'s use of SQL or Spark-SQL queries makes it uniquely suitable for customized validation logic.  Although the default behavior of an `UnexpectedRowsExpectation` is to treat returned rows as having failed validation, you can override this default by providing a custom `_validate(...)` method for your customized subclass of `UnexpectedRowsExpectation`. -->

<h2>Prerequisites</h2>

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- <PrereqPreconfiguredDataContext/>.
- Recommended. <PrereqPreconfiguredDataSourceAndAsset/> for [testing your customized Expectation](/core/define_expectations/test_an_expectation.md).

### Procedure

<Tabs 
   queryString="procedure"
   defaultValue="instructions"
   values={[
      {value: 'instructions', label: 'Instructions'},
      {value: 'sample_code', label: 'Sample code'}
   ]}
>

<TabItem value="instructions" label="Instructions">

1. Create a new Expectation class that inherits the `UnexpectedRowsExpectation` class.
  
   The class name `UnexpectedRowsExpectation` describes the functionality of the Expectation: it finds rows with unexpected values.  When you create a customized Expectation class you can provide a class name that is more indicative of your specific use case.  In this example, the customized subclass of `UnexpectedRowsExpectation` will be used to find invalid passenger counts in taxi trip data:

   ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/use_sql_to_define_a_custom_expectation.py - define a more descriptive name for an UnexpectedRowsExpectation"
   ```

2. Override the Expectation's `unexpected_rows_query` attribute.

   The `unexpected_rows_query` attribute is a SQL or Spark-SQL query that returns a selection of rows from the Batch of data being validated.  By default, rows that are returned have failed the validation check.

   Although the `unexpected_rows_query` should be written in standard SQL or Spark-SQL syntax, it must also contain the special `{batch}` placeholder.  When the Expectation is evaluated, the `{batch}` placeholder will be replaced with the Batch of data that is validated.

   In this example, `unexpected_rows_query` will select any rows where the passenger count is greater than `6` or less than `0`.  These rows will fail validation for this Expectation:

   ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/use_sql_to_define_a_custom_expectation.py - define the query for an UnexpectedRowsExpectation"
   ```

3. Customize the rendering of the new Expectation when displayed in Data Docs.

   As with other Expectations, the `description` attribute contains the text describing the customized Expectation when your results are rendered into Data Docs.  It can be set when an Expectation class is defined or edited as an attribute of an Expectation instance.  You can format the `description` string with Markdown syntax:

   ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/use_sql_to_define_a_custom_expectation.py - define a custom UnexpectedRowsExpectation"
   ```

4. Use the customized subclass as an Expectation.

   Once the customized Expectation subclass has been defined, instances of it can be created, added to Expectation Suites, and validated just like any other Expectation class:

   ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/use_sql_to_define_a_custom_expectation.py - instantiate the custom SQL Expectation"
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/use_sql_to_define_a_custom_expectation.py - full code example"
```

</TabItem>

</Tabs>