---
title: Customize an Expectation Class
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

Existing Expectation Classes can be customized to include additional information such as buisness logic, more descriptive naming conventions, and specialized rendering for Data Docs.  This is done by subclassing an existing Expectation class and populating the subclass with default values and customized attributes.

Advantages of subclassing an Expectation and providing customized attributes rather than creating an instance of the parent Expectation and passing in parameters include:

   - All instances of the Expectation that use the default values will be updated if changes are made to the class definition.
   - More descriptive Expectation names can be provided that indicate the buisness logic behind the Expectation rather than the functionality of the Expectation.
   - Customized text can be provided to describe the Expectation when Data Docs are generated from Validation Results.

<h2>Prerequisites</h2>

- Python
- GX installation
- Data Context

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Choose and import a base Expectation class.

   You can customize any of the core Expectation classes in GX. You can view the available Expectations and their functionality in the [Expectation Gallery](https://greatexpectations.io/expectations).

   In this example, `ExpectColumnValueToBeBetween` will be customized:
 
   ```python title="Python"
   from great_expectations.expectations import ExpectColumnValueToBeBetween
   ```


2. Create a new Expectation class that inherits the base Expectation class.
  
   The core Expectations in GX have names descriptive of their functionality.  When you create a customized Expectation class you can provide a class name that is more indicative of your specific use case:

   ```python title="Python"
   class ExpectValidPassengerCount(ExpectColumnValueToBeBetween):
   ```

3. Override the Expectation's attributes with new default values.

   The attributes that can be overriden correspond to the parameters required by the base Expectation.  These can be referenced from the [Expectation Gallery](https://greatexpectations.io/expectations).

   In this example, we specify that the default column for `ExpectValidPassengerCount` is `passenger_count` and the default range is between `1` and `6`:

   ```python title="Python"
   class ExpectValidPassengerCount(ExpectColumnValueToBeBetween):
       # highlight-start
       column: str = "passenger_count"
       min_value: int = 1
       max_value: int = 6
       # highlight-end
   ```

5. Customize the rendering of the new Expectation when displayed in Data Docs.

   The `render_text` attribute contains the text describing the customized Expectation when your results are rendered into Data Docs.  It can be set when an Expectation class is defined or edited as an attribute of an Expectation instance.  You can format the `render_text` string with Markdown syntax:

   ```python title="Python"
   class ExpectValidPassengerCount(ExpectColumnValueToBeBetween):
       column: str = "passenger_count"
       min_value: int = 1
       max_value: int = 6
       # highlight-start
       render_text: str = "There should be between **1** and **6** passengers."
       # highlight-end
   ```

6. Use the customized subclass as an Expectation.

   Once a customized Expectation subclass has been defined, instances of it can be created, added to Expectation Suites, and validated just like any other Expectation class:

   ```python title="Python"
   expectation1 = ExpectValidPassengerCount()  # Uses the predefined default values
   expectation2 = ExpectValidPassengerCount(column="occupied_seats")  # Uses a different column than the default, but keeps the default min_value, max_value, and render_text.
   ```
   
   It is best to use the predefined default values when a customized Expectation is created.  This ensures that the `render_text` remains accurate to the values that the Expectation uses.  It also allows you to update all instances of the customized Expectation by editing the default values in the customized Expectation's class definition rather than having to update each instance individually in their Expectation Suites.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python"
import great_expectations as gx
from great_expectations.expectations import ExpectColumnValueToBeBetween

class ExpectValidPassengerCount(ExpectColumnValueToBeBetween):
  column: str = "passenger_count"
  min_value: int = 1
  max_value: int = 6
  render_text: str = "There should be between **1** and **6** passengers."

context = gx.get_context()

expectation1 = ExpectValidPassengerCount()  # Uses the predefined default values
expectation2 = ExpectValidPassengerCount(column="occupied_seats")  # Uses a different column than the default, but keeps the default min_value, max_value, and render_text.

data_source_name = "my_taxi_data"
asset_name = "2018_taxi_data"
batch_definition_name = "all_records_in_asset"
batch = context.get_datasource(datasource_name).get_asset(asset_name).get_batch_definition(batch_definition_name=batch_definition_name).get_batch()

batch.validate(expectation1)
batch.validate(expectation2)

```

</TabItem>

</Tabs>