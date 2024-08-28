---
title: Customize an Expectation Class
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../_core_components/prerequisites/_gx_installation.md';
import PrereqPreconfiguredDataContext from '../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqPreconfiguredDataSourceAndAsset from '../_core_components/prerequisites/_data_source_and_asset_connected_to_data.md';

Existing Expectation Classes can be customized to include additional information such as buisness logic, more descriptive naming conventions, and specialized rendering for Data Docs.  This is done by subclassing an existing Expectation class and populating the subclass with default values and customized attributes.

Advantages of subclassing an Expectation and providing customized attributes rather than creating an instance of the parent Expectation and passing in parameters include:

   - All instances of the Expectation that use the default values will be updated if changes are made to the class definition.
   - More descriptive Expectation names can be provided that indicate the buisness logic behind the Expectation.
   - Customized text can be provided to describe the Expectation when Data Docs are generated from Validation Results.

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

1. Choose a base Expectation class.

   You can customize any of the core Expectation classes in GX. You can view the available Expectations and their functionality in the [Expectation Gallery](https://greatexpectations.io/expectations).

   In this example, `ExpectColumnValuesToBeBetween` will be customized.

2. Create a new Expectation class that inherits the base Expectation class.
  
   The core Expectations in GX have names describing their functionality.  When you create a customized Expectation class you can provide a class name that is more indicative of your specific use case:

   ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/define_a_custom_expectation_class.py - define a custom Expectation subclass"
   ```

3. Override the Expectation's attributes with new default values.

   The attributes that can be overriden correspond to the parameters required by the base Expectation.  These can be referenced from the [Expectation Gallery](https://greatexpectations.io/expectations).

   In this example, the default column for `ExpectValidPassengerCount` is set to `passenger_count` and the default value range for the column is defined as between `1` and `6`:

   ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/define_a_custom_expectation_class.py - define default attributes for a custom Expectation class"
   ```

4. Customize the rendering of the new Expectation when displayed in Data Docs.

   The `description` attribute of a customized Expectation class contains the text describing the customized Expectation when its results are rendered into Data Docs.  You can format the `description` string with Markdown syntax:

   ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/define_a_custom_expectation_class.py - define description attribute for a cusom Expectation"
   ```

5. Use the customized subclass as an Expectation.

   It is best not to overwrite the predefined default values by passing in parameters when a customized Expectation is created.  This ensures that the `description` remains accurate to the values that the customized Expectation uses.  It also allows you to update all instances of the customized Expectation by editing the default values in the customized Expectation's class definition rather than having to update each instance individually in their Expectation Suites:

   ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/define_a_custom_expectation_class.py - instantiate a Custom Expectation"
   ```

   A customized Expectation instance can be added to Expectation Suites and validated just like any other Expectation.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/define_a_custom_expectation_class.py - full code example"
```

</TabItem>

</Tabs>